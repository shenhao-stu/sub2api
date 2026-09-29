//go:build unit

package service

import (
	"bytes"
	"context"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/xai"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestGrokTeamCooldownPreservesObservedBoundary(t *testing.T) {
	now := time.Now()
	for _, seconds := range []int{1, 10, 29, 30, 31, 45, 120} {
		t.Run(strconv.Itoa(seconds), func(t *testing.T) {
			until := now.Add(time.Duration(seconds) * time.Second)
			require.Equal(t, until, resolveGrokTeamRateLimitUntil(until, now))
		})
	}
	require.True(t, resolveGrokTeamRateLimitUntil(now.Add(-time.Second), now).IsZero())
	require.Equal(t, now.Add(grokTeamRateLimitMaxTTL), resolveGrokTeamRateLimitUntil(now.Add(24*time.Hour), now))
}

func TestGrokModelQuotaCooldownIgnoresMissingOrExpiredObservation(t *testing.T) {
	account := healthyGrokOAuthGatewayTestAccount(930300, "test-access")
	account.Credentials["team_id"] = t.Name()
	for _, until := range []time.Time{{}, time.Now().Add(-time.Second)} {
		markGrokModelQuotaCooldown(account, "grok-4.5", until)
		require.False(t, isGrokModelQuotaBlocked(account.ID, "grok-4.5", time.Now()))
		require.False(t, isGrokTeamModelRateLimited(account, "grok-4.5", time.Now()))
	}
}

func TestGrokQuotaCooldownMatchesAcrossHTTPPaths(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for index, path := range []string{"responses", "chat", "metered"} {
		for _, modelQuota := range []bool{false, true} {
			t.Run(path+"/model="+strconv.FormatBool(modelQuota), func(t *testing.T) {
				id := int64(930001 + index*10)
				if modelQuota {
					id++
				}
				account := healthyGrokOAuthGatewayTestAccount(id, "test-access")
				account.Credentials["team_id"] = t.Name()
				repo := &grokQuotaAccountRepo{mockAccountRepoForPlatform: &mockAccountRepoForPlatform{accountsByID: map[int64]*Account{id: account}}}
				response := `{"error":{"message":"Too many requests"}}`
				if modelQuota {
					response = `{"error":{"message":"You have used all the included free usage for model grok-4.5"}}`
				}
				if path == "metered" {
					response = strings.TrimSuffix(response, "}") + `,"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`
				}
				upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 429,
					Header: http.Header{"Content-Type": {"application/json"}, "Retry-After": {"45"}}, Body: io.NopCloser(strings.NewReader(response))}}
				svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig(), billingService: newLocalFixtureBilling(),
					accountRepo: repo, httpUpstream: upstream, grokTokenProvider: NewGrokTokenProvider(repo, nil)}
				body := []byte(`{"model":"grok-4.5","input":"hi","stream":false}`)
				if path == "chat" {
					body = []byte(`{"model":"grok-4.5","messages":[{"role":"user","content":"hi"}],"stream":false}`)
				}
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
				start := time.Now()
				var err error
				if path == "chat" {
					_, err = svc.ForwardAsChatCompletions(context.Background(), c, account, body, "", "")
				} else {
					_, err = svc.forwardGrokResponses(context.Background(), c, account, body, "grok-4.5", false, start)
				}
				require.Error(t, err)
				require.NotNil(t, upstream.lastReq)
				require.True(t, isGrokTeamModelRateLimited(account, "grok-4.5", start.Add(30*time.Second)))
				require.False(t, isGrokTeamModelRateLimited(account, "grok-4.5", time.Now().Add(47*time.Second)), "a second writer must not replace Retry-After with ten minutes")
				require.False(t, isGrokTeamModelRateLimited(account, "grok-4.6", time.Now()))
				if modelQuota {
					require.Zero(t, repo.rateLimitedCalls)
				} else {
					require.Equal(t, 1, repo.rateLimitedCalls)
				}
			})
		}
	}
}

func TestGrokSpendingCooldownRequiresExhaustedWindow(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	start, end := now.Add(-time.Hour).Format(time.RFC3339), now.Add(48*time.Hour).Format(time.RFC3339)
	for _, percent := range []*float64{nil, floatPtr(0), floatPtr(90), floatPtr(100)} {
		billing := &xai.BillingSummary{PeriodType: "weekly", PeriodStart: start, PeriodEnd: end,
			UsagePercent: percent, WeeklyStatusCode: 200, WeeklyUpdatedAt: now.Format(time.RFC3339),
			BillingPeriodEnd: now.Add(10 * 24 * time.Hour).Format(time.RFC3339), MonthlyLimitCents: floatPtr(0)}
		account := &Account{Extra: map[string]any{grokBillingExtraKey: billing}}
		want := now.Add(grokSpendingLimitProbeCooldown)
		if percent != nil && *percent == 100 {
			want = now.Add(48 * time.Hour)
		}
		require.Equal(t, want, grokSpendingLimitResetAt(account, now))
	}
}

func TestGrokQuotaCooldownCapacityKeepsSiblingAvailable(t *testing.T) {
	account := healthyGrokOAuthGatewayTestAccount(930100, "test-access")
	account.Credentials["team_id"] = t.Name()
	sibling := *account
	sibling.ID++
	repo := &grokQuotaAccountRepo{}
	svc := &OpenAIGatewayService{accountRepo: repo}
	svc.handleGrokAccountUpstreamError(withGrokTeamRateLimitModel(context.Background(), "grok-4.5"), account,
		http.StatusTooManyRequests, http.Header{"Retry-After": {"60"}},
		[]byte(`{"error":{"message":"The model is currently at capacity due to high demand"}}`))
	require.True(t, isGrokModelQuotaBlocked(account.ID, "grok-4.5", time.Now()))
	require.False(t, isGrokModelQuotaBlocked(account.ID, "grok-4.6", time.Now()))
	require.False(t, isGrokTeamModelRateLimited(&sibling, "grok-4.5", time.Now()))
	require.Zero(t, repo.rateLimitedCalls)
	require.Zero(t, repo.tempUnschedCalls)
}

func TestGrokBillingWindowEvidence(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	start, end := now.Add(-time.Hour), now.Add(time.Hour)
	for _, tc := range []struct {
		name                 string
		percent              *float64
		start, end, observed time.Time
		status               int
		valid                bool
	}{
		{"exhausted", floatPtr(100), start, end, now, 200, true},
		{"not exhausted", floatPtr(99), start, end, now, 200, false},
		{"unknown", nil, start, end, now, 200, false},
		{"nonfinite", floatPtr(math.Inf(1)), start, end, now, 200, false},
		{"not a number", floatPtr(math.NaN()), start, end, now, 200, false},
		{"failed probe", floatPtr(100), start, end, now, 503, false},
		{"previous period", floatPtr(100), start, end, start.Add(-time.Second), 200, false},
		{"future observation", floatPtr(100), start, end, now.Add(time.Minute), 200, false},
		{"expired period", floatPtr(100), start, now, start, 200, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reset := grokExhaustedBillingWindowReset(tc.percent, tc.start.Format(time.RFC3339),
				tc.end.Format(time.RFC3339), tc.observed.Format(time.RFC3339), tc.status, now)
			require.Equal(t, tc.valid, !reset.IsZero())
			if tc.valid {
				require.Equal(t, tc.end, reset)
			}
		})
	}
}

func TestGrokSpendingCooldownKeepsOnlyExhaustedBillingWindows(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	for _, tc := range []struct {
		name            string
		weekly, monthly float64
		want            time.Duration
	}{
		{"weekly only", 100, 20, time.Hour},
		{"monthly only", 20, 100, 24 * time.Hour},
		{"both", 100, 100, 24 * time.Hour},
		{"neither", 20, 20, grokSpendingLimitProbeCooldown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			billing := &xai.BillingSummary{
				PeriodType: "weekly", PeriodStart: now.Add(-time.Hour).Format(time.RFC3339),
				PeriodEnd: now.Add(time.Hour).Format(time.RFC3339), UsagePercent: &tc.weekly,
				WeeklyStatusCode: 200, WeeklyUpdatedAt: now.Format(time.RFC3339),
				BillingPeriodStart: now.Add(-24 * time.Hour).Format(time.RFC3339),
				BillingPeriodEnd:   now.Add(24 * time.Hour).Format(time.RFC3339), UsedPercent: &tc.monthly,
				MonthlyStatusCode: 200, MonthlyUpdatedAt: now.Format(time.RFC3339),
			}
			account := &Account{Extra: map[string]any{grokBillingExtraKey: billing}}
			require.Equal(t, now.Add(tc.want), grokSpendingLimitResetAt(account, now))
		})
	}
}

func TestGrokBodyQuotaCooldownMatchesTerminalAndHTTPFailures(t *testing.T) {
	for index, path := range []string{"http400", "http429", "stream_error", "stream_response_failed"} {
		t.Run(path, func(t *testing.T) {
			account := healthyGrokOAuthGatewayTestAccount(int64(930200+index), "test-access")
			account.Credentials["team_id"] = t.Name()
			repo := &grokQuotaAccountRepo{}
			svc := &OpenAIGatewayService{accountRepo: repo}
			body := []byte(`{"error":{"code":"subscription:free-usage-exhausted","message":"You have used all the included free usage for model grok-4.5"}}`)
			start := time.Now()
			cooldown := grokRateLimitFallbackCooldown
			if strings.HasPrefix(path, "http") {
				status := 429
				if path == "http400" {
					status, cooldown = 400, grokFreeUsageProbeCooldown
				}
				svc.handleGrokAccountUpstreamError(withGrokTeamRateLimitModel(context.Background(), "grok-4.5"), account, status, nil, body)
			} else {
				if path == "stream_response_failed" {
					body = append(append([]byte(`{"response":`), body...), '}')
				}
				svc.handleGrokStreamTerminalError(nil, account, 429, body, "grok-4.5")
			}
			require.True(t, isGrokTeamModelRateLimited(account, "grok-4.5", start.Add(cooldown-time.Second)))
			require.False(t, isGrokTeamModelRateLimited(account, "grok-4.5", time.Now().Add(cooldown+time.Second)))
			require.True(t, isGrokModelQuotaBlocked(account.ID, "grok-4.5", start.Add(cooldown-time.Second)))
			require.False(t, isGrokModelQuotaBlocked(account.ID, "grok-4.5", time.Now().Add(cooldown+time.Second)))
			require.False(t, isGrokTeamModelRateLimited(account, "grok-4.6", time.Now()))
			require.Zero(t, repo.rateLimitedCalls)
			require.Zero(t, repo.tempUnschedCalls)
		})
	}
}
