//go:build unit

package service

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/xai"
	"github.com/stretchr/testify/require"
)

func TestGrokFreeQuotaWithoutResetUsesQuotaProbeInterval(t *testing.T) {
	for i, headers := range []http.Header{nil, {"X-Ratelimit-Remaining-Tokens": {"0"}}} {
		account := &Account{ID: int64(932101 + i), Platform: PlatformGrok, Type: AccountTypeOAuth}
		svc := &OpenAIGatewayService{accountRepo: &grokQuotaAccountRepo{}}
		body := []byte(`{"error":{"message":"You've used all the included free usage for model grok-4.6 for now. Usage resets over a rolling 24-hour window — tokens (actual/limit): 637350/500000."}}`)
		now := time.Now()
		svc.handleGrokAccountUpstreamError(withGrokTeamRateLimitModel(context.Background(), "grok-4.6"), account, http.StatusTooManyRequests, headers, body)
		require.True(t, isGrokModelQuotaBlocked(account.ID, "grok-4.6", now.Add(9*time.Minute)))
		require.False(t, isGrokModelQuotaBlocked(account.ID, "grok-4.7", now))
		require.False(t, isGrokModelQuotaBlocked(account.ID, "grok-4.6", time.Now().Add(11*time.Minute)))
	}
}

func TestGrokObservedRetryBoundaryWinsOverAdaptiveBackoff(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	limited, reset := now.Add(-11*time.Minute), now.Add(-time.Minute)
	account := &Account{ID: 932102, Platform: PlatformGrok, Type: AccountTypeOAuth, RateLimitedAt: &limited, RateLimitResetAt: &reset}
	seconds := 45
	snapshot := &xai.QuotaSnapshot{StatusCode: 429, UpdatedAt: now.Format(time.RFC3339), RetryAfterSeconds: &seconds}
	until, active := grokRateLimitResetAtForAccount(account, snapshot, now)
	require.True(t, active)
	require.Equal(t, now.Add(45*time.Second), until)
}

func TestGrokExpiredObservedQuotaWindowDoesNotRestartFallback(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	remaining, expired := int64(0), now.Add(-time.Minute).Unix()
	snapshot := &xai.QuotaSnapshot{StatusCode: 429, Tokens: &xai.QuotaWindow{Remaining: &remaining, ResetUnix: &expired}}
	until, active := grokRateLimitResetAt(snapshot, now)
	require.False(t, active)
	require.True(t, until.IsZero())
}
