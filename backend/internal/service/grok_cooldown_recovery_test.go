//go:build unit

package service

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGrokRobustForbiddenRequiresAccountEvidence(t *testing.T) {
	for _, body := range []string{
		`{"error":{"message":"Forbidden"}}`,
		`{"error":{"code":"permission_denied","message":"Access denied"}}`,
		`<html><title>403 Forbidden</title>Request blocked by edge gateway</html>`,
		`{"error":{"code":"unknown_edge_rejection","message":"Request rejected"}}`,
	} {
		t.Run(body, func(t *testing.T) {
			repo := &grokQuotaAccountRepo{}
			svc := &OpenAIGatewayService{accountRepo: repo}
			account := &Account{ID: 24401, Platform: PlatformGrok, Type: AccountTypeOAuth}
			svc.handleGrokAccountUpstreamError(context.Background(), account, http.StatusForbidden, http.Header{"Retry-After": {"120"}}, []byte(body))
			require.Zero(t, repo.tempUnschedCalls)
			require.Zero(t, repo.rateLimitedCalls)
			require.False(t, svc.shouldFailoverGrokUpstreamError(http.StatusForbidden, []byte(body)))
			require.False(t, svc.shouldFailoverOpenAIUpstreamResponse(account, http.StatusForbidden, "", []byte(body)))
			require.False(t, svc.handleOpenAIAccountUpstreamError(context.Background(), account, http.StatusForbidden, nil, []byte(body), "grok-4.7"))
		})
	}
}

func TestGrokRobustTransientRecoveryIsModelScoped(t *testing.T) {
	for _, status := range []int{500, 502, 503, 504, 524} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			repo := &grokQuotaAccountRepo{}
			svc := &OpenAIGatewayService{accountRepo: repo}
			account := &Account{ID: int64(24400 + status), Platform: PlatformGrok, Type: AccountTypeOAuth,
				Credentials: map[string]any{"model_mapping": map[string]any{"public-alias": "grok-4.7", "grok-4.7": "grok-4.6"}}}
			ctx := withGrokTeamRateLimitModel(context.Background(), "grok-4.7")
			state := svc.getOpenAIAccountModelTransientState()
			for attempt := 1; attempt <= 3; attempt++ {
				before := time.Now()
				svc.handleGrokAccountUpstreamError(ctx, account, status, http.Header{"Retry-After": {"120"}}, nil)
				require.Zero(t, repo.tempUnschedCalls)
				require.Zero(t, repo.rateLimitedCalls)
				require.False(t, svc.isOpenAIAccountRuntimeBlocked(account))
				require.False(t, svc.isOpenAIAccountRequestRuntimeBlocked(account, "grok-4.5"))
				require.Equal(t, attempt > 1, svc.isOpenAIAccountRequestRuntimeBlocked(account, "public-alias"))
				if attempt > 1 {
					wait := 10 * time.Second
					if attempt == 3 {
						wait = 45 * time.Second
					}
					require.True(t, state.isBlocked(account.ID, "grok-4.7", before.Add(wait-time.Second)))
					require.False(t, state.isBlocked(account.ID, "grok-4.7", time.Now().Add(wait+time.Second)))
				}
			}
			svc.ReportOpenAIAccountScheduleResult(account, "grok-4.7", true, nil)
			require.False(t, svc.isOpenAIAccountRequestRuntimeBlocked(account, "public-alias"))
			svc.handleGrokAccountUpstreamError(ctx, account, status, nil, nil)
			require.False(t, svc.isOpenAIAccountRequestRuntimeBlocked(account, "public-alias"), "success resets the failure streak")

			// A model success must not clear a subsequently observed account quota.
			until := time.Now().Add(time.Hour)
			account.RateLimitResetAt = &until
			svc.BlockAccountScheduling(account, until, "429")
			svc.ReportOpenAIAccountScheduleResult(account, "grok-4.7", true, nil)
			require.True(t, svc.isOpenAIAccountRequestRuntimeBlocked(account, "public-alias"))
			require.Equal(t, until, *account.RateLimitResetAt)
			require.Zero(t, repo.recoveryClearCalls)
		})
	}
}

func TestGrokRobustModelQuotaPreservesShortReset(t *testing.T) {
	for i, duration := range []time.Duration{time.Second, 5 * time.Minute, 10 * time.Minute} {
		id := int64(24900 + i)
		until := time.Now().Add(duration)
		markGrokModelQuotaBlock(id, "grok-4.7", until)
		require.True(t, isGrokModelQuotaBlocked(id, "grok-4.7", until.Add(-time.Millisecond)))
		require.False(t, isGrokModelQuotaBlocked(id, "grok-4.6", time.Now()))
		require.False(t, isGrokModelQuotaBlocked(id, "grok-4.7", until.Add(time.Millisecond)), "must not turn a short reset into two hours")
	}
	markGrokModelQuotaBlock(24910, "grok-4.7", time.Now().Add(-time.Second))
	require.False(t, isGrokModelQuotaBlocked(24910, "grok-4.7", time.Now()))
	until := time.Now().Add(time.Hour)
	markGrokModelQuotaBlock(24911, "grok-4.7", until)
	markGrokModelQuotaBlock(24911, "grok-4.7", time.Now().Add(time.Minute))
	require.True(t, isGrokModelQuotaBlocked(24911, "grok-4.7", until.Add(-time.Second)), "a shorter stale observation must not clear an existing quota")
}

func TestGrokRobustModelQuotaDoesNotProjectAccountCooldown(t *testing.T) {
	for i, body := range []string{
		`{"error":{"message":"You've used all the included free usage for model grok-4.7"}}`,
		`{"error":{"message":"The model is currently at capacity due to high demand"}}`,
	} {
		repo := &grokQuotaAccountRepo{}
		svc := &OpenAIGatewayService{accountRepo: repo}
		account := &Account{ID: int64(24920 + i), Platform: PlatformGrok, Type: AccountTypeOAuth}
		ctx := withGrokTeamRateLimitModel(context.Background(), "grok-4.7")
		headers := http.Header{"Retry-After": {"60"}, "X-Ratelimit-Limit-Requests": {"100"}, "X-Ratelimit-Remaining-Requests": {"0"}}
		svc.handleGrokAccountUpstreamError(ctx, account, http.StatusTooManyRequests, headers, []byte(body))
		require.Zero(t, repo.tempUnschedCalls)
		require.Zero(t, repo.rateLimitedCalls)
		require.NotContains(t, repo.updates[account.ID], "grok_sched_utilization")
		require.True(t, isGrokModelQuotaBlocked(account.ID, "grok-4.7", time.Now()))
		require.False(t, isGrokModelQuotaBlocked(account.ID, "grok-4.6", time.Now()))
	}
}
