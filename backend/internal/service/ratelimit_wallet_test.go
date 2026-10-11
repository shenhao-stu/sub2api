//go:build unit

package service

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

const walletForbiddenBody = `{"code":"INSUFFICIENT_BALANCE","message":"Insufficient account balance"}`

func TestAPIKeyWalletExhaustionRequiresStructuredBillingRejection(t *testing.T) {
	account := &Account{Type: AccountTypeAPIKey}
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{"root forbidden balance", 403, walletForbiddenBody, true},
		{"nested billing error", 403, `{"error":{"type":"billing_error","message":"insufficient balance"}}`, true},
		{"wrapped forbidden balance", 403, `{"error":{"code":"bad_response_status_code","message":"Insufficient account balance"}}`, true},
		{"forbidden quota", 403, upstreamQuotaErrorBody, true},
		{"payment quota", 402, upstreamQuotaErrorBody, true},
		{"rate limited quota", 429, upstreamQuotaErrorBody, true},
		{"short quota message", 429, `{"error":{"type":"insufficient_quota","message":"insufficient balance"}}`, true},
		{"permission denied", 403, `{"error":{"code":"permission_denied","message":"Insufficient account balance"}}`, false},
		{"content policy", 403, `{"error":{"code":"content_policy_violation","message":"Insufficient account balance"}}`, false},
		{"ordinary rate limit", 429, `{"error":{"code":"rate_limit_exceeded","message":"Insufficient account balance"}}`, false},
		{"billing without wallet message", 403, `{"error":{"type":"billing_error","message":"Billing verification required"}}`, false},
		{"phrase in longer message", 403, `{"error":{"type":"billing_error","message":"Prompt contained insufficient balance"}}`, false},
		{"quoted error", 403, `{"error":{"message":"{\"code\":\"INSUFFICIENT_BALANCE\",\"message\":\"Insufficient account balance\"}"}}`, false},
		{"request echo", 403, `{"request":{"code":"INSUFFICIENT_BALANCE","message":"Insufficient account balance"}}`, false},
		{"truncated", 403, walletForbiddenBody[:len(walletForbiddenBody)-1], false},
		{"trailing data", 403, walletForbiddenBody + " garbage", false},
		{"request error", 400, walletForbiddenBody, false},
		{"authentication", 401, walletForbiddenBody, false},
		{"success", 200, walletForbiddenBody, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isAPIKeyWalletExhausted(account, tc.status, []byte(tc.body)))
		})
	}
	require.False(t, isAPIKeyWalletExhausted(nil, 403, []byte(walletForbiddenBody)))
	require.False(t, isAPIKeyWalletExhausted(&Account{Type: AccountTypeOAuth}, 403, []byte(walletForbiddenBody)))
}

func TestWalletForbiddenUsesRecoverableCooldownAcrossPoolAndOrdinaryAccounts(t *testing.T) {
	for _, platform := range []string{PlatformAnthropic, PlatformOpenAI} {
		for _, pool := range []bool{false, true} {
			account := &Account{ID: 335, Platform: platform, Type: AccountTypeAPIKey, Status: StatusActive,
				Schedulable: true, Credentials: map[string]any{"pool_mode": pool}}
			repo := &upstreamQuotaRepo{account: account}
			blocker := &runtimeBlockRecorder{}
			svc := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
			svc.SetAccountRuntimeBlocker(blocker)
			before := time.Now()
			require.True(t, svc.HandleUpstreamError(context.Background(), account, 403, nil, []byte(walletForbiddenBody)))
			require.Equal(t, 1, repo.atomicCalls)
			require.Zero(t, repo.setErrorCalls)
			require.Zero(t, repo.tempCalls)
			require.Zero(t, repo.rateLimitedCalls)
			require.Equal(t, StatusActive, account.Status)
			require.True(t, account.Schedulable)
			require.NotNil(t, account.RateLimitResetAt)
			require.False(t, account.RateLimitResetAt.Before(before.Add(10*time.Minute)))
			require.Equal(t, []string{"upstream_insufficient_quota"}, blocker.reasons)
		}
	}
}

func TestWalletForbiddenPreservesLongerAuthoritativeResetAfterCancellation(t *testing.T) {
	reset := time.Now().Add(6 * time.Hour)
	stale := &Account{ID: 335, Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Credentials: map[string]any{"pool_mode": true}}
	current := *stale
	current.RateLimitResetAt = &reset
	repo := &upstreamQuotaRepo{account: &current}
	blocker := &runtimeBlockRecorder{}
	svc := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
	svc.SetAccountRuntimeBlocker(blocker)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.True(t, svc.HandleUpstreamError(ctx, stale, 403, nil, []byte(walletForbiddenBody)))
	require.Equal(t, 1, repo.atomicCalls)
	require.Equal(t, reset, *current.RateLimitResetAt)
	require.Equal(t, []time.Time{reset}, blocker.until)
}

func TestWalletForbiddenHonorsExplicitErrorPolicy(t *testing.T) {
	for _, tc := range []struct {
		name        string
		credentials map[string]any
		tempCalls   int
	}{
		{"custom excluded", map[string]any{"pool_mode": true, "custom_error_codes_enabled": true, "custom_error_codes": []any{float64(401)}}, 0},
		{"explicit temporary", map[string]any{"pool_mode": true, "temp_unschedulable_enabled": true, "temp_unschedulable_rules": []any{map[string]any{
			"error_code": float64(403), "keywords": []any{"INSUFFICIENT_BALANCE"}, "duration_minutes": float64(30),
		}}}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			account := &Account{ID: 335, Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Credentials: tc.credentials}
			repo := &upstreamQuotaRepo{account: account}
			svc := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
			svc.HandleUpstreamError(context.Background(), account, 403, nil, []byte(walletForbiddenBody))
			require.Zero(t, repo.atomicCalls)
			require.Equal(t, tc.tempCalls, repo.tempCalls)
			require.Zero(t, repo.setErrorCalls)
		})
	}
}

func TestAnthropicWalletForbiddenFailsOverBeforeCommittingResponse(t *testing.T) {
	account := &Account{ID: 335, Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Credentials: map[string]any{"pool_mode": true}}
	repo := &upstreamQuotaRepo{account: account}
	cfg := &config.Config{}
	svc := &GatewayService{cfg: cfg, rateLimitService: NewRateLimitService(repo, nil, cfg, nil, nil)}
	c, rec := newOpenAIUpstreamErrorTestContext(t)
	resp := newOpenAIUpstreamErrorResponse(http.StatusForbidden, walletForbiddenBody)
	result, err := svc.handleErrorResponse(context.Background(), resp, c, account)
	var failover *UpstreamFailoverError
	require.ErrorAs(t, err, &failover)
	require.Nil(t, result)
	require.Equal(t, http.StatusForbidden, failover.StatusCode)
	require.False(t, failover.RetryableOnSameAccount)
	require.Empty(t, rec.Body.String())
	require.False(t, c.Writer.Written())
	require.Equal(t, 1, repo.atomicCalls)
}

func TestWalletForbiddenHonorsDisabledQuotaCooldown(t *testing.T) {
	reset := time.Now().Add(time.Hour)
	account := &Account{ID: 335, Platform: PlatformAnthropic, Type: AccountTypeAPIKey,
		Credentials: map[string]any{"pool_mode": true}, RateLimitResetAt: &reset}
	repo := &upstreamQuotaRepo{account: account}
	settings := newMockSettingRepo()
	settings.data[SettingKeyRateLimit429CooldownSettings] = `{"enabled":false,"cooldown_seconds":5}`
	svc := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
	svc.SetSettingService(NewSettingService(settings, &config.Config{}))
	require.True(t, svc.HandleUpstreamError(context.Background(), account, 403, nil, []byte(walletForbiddenBody)))
	require.Zero(t, repo.atomicCalls)
	require.Zero(t, repo.setErrorCalls)
	require.Equal(t, reset, *account.RateLimitResetAt)
}
