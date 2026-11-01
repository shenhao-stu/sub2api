//go:build unit

package service

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type modelCooldownRepo struct {
	anthropicWindowLimitRepo
	ctxErr error
}

func (r *modelCooldownRepo) SetModelRateLimitIfLater(ctx context.Context, _ int64, model string, until time.Time, _ ...string) error {
	r.ctxErr = ctx.Err()
	r.modelRateLimitCalls++
	r.lastModelRateLimitScope = model
	r.lastModelRateLimitReset = until
	return nil
}

func TestStructuredModelCooldownPreservesModelScope(t *testing.T) {
	for _, pool := range []bool{false, true} {
		repo := &modelCooldownRepo{}
		account := &Account{ID: 11, Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
			Credentials: map[string]any{"pool_mode": pool, "model_mapping": map[string]any{"gemini-argon": "ultra/gemini-argon"}}}
		svc := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		now := time.Now()
		require.True(t, svc.HandleUpstreamError(ctx, account, http.StatusTooManyRequests,
			http.Header{"Retry-After": []string{"3600"}},
			[]byte(`{"error":{"code":"model_cooldown","model":"ultra/gemini-argon","reset_seconds":3600}}`), "gemini-argon"))
		require.Equal(t, 1, repo.modelRateLimitCalls)
		require.Equal(t, "ultra/gemini-argon", repo.lastModelRateLimitScope)
		require.WithinDuration(t, now.Add(time.Hour), repo.lastModelRateLimitReset, 2*time.Second)
		require.NoError(t, repo.ctxErr)
		require.Zero(t, repo.rateLimitCalls)
		require.Zero(t, repo.tempUnschedCalls)
	}
}

func TestUpstreamModelCooldownRequiresExactStructuredReset(t *testing.T) {
	valid := `{"error":{"code":"model_cooldown","model":"ultra/argon","reset_seconds":3600}}`
	for _, tc := range []struct {
		body, model string
		want        time.Duration
	}{
		{valid, "ultra/argon", time.Hour},
		{valid, "argon", 0},
		{valid, "ultra/flash", 0},
		{valid, "", 0},
		{valid + " trailing", "ultra/argon", 0},
		{`{"error":{"message":"model_cooldown","model":"ultra/argon","reset_seconds":3600}}`, "ultra/argon", 0},
		{`{"error":{"code":"model_cooldown","reset_seconds":3600}}`, "ultra/argon", 0},
		{`{"error":{"code":"rate_limit_exceeded","model":"ultra/argon","reset_seconds":3600}}`, "ultra/argon", 0},
	} {
		require.Equal(t, tc.want, upstreamModelCooldown([]byte(tc.body), tc.model))
	}
	for _, value := range []string{"0", "-1", "604801", "3600.5", `"3600"`, "9223372036854775808", "null"} {
		body := fmt.Sprintf(`{"error":{"code":"model_cooldown","model":"ultra/argon","reset_seconds":%s}}`, value)
		require.Zero(t, upstreamModelCooldown([]byte(body), "ultra/argon"), value)
	}
}

func TestStructuredModelCooldownDoesNotChangeOtherStatusesOrOAuth(t *testing.T) {
	body := []byte(`{"error":{"code":"model_cooldown","model":"ultra/argon","reset_seconds":3600}}`)
	repo := &modelCooldownRepo{}
	svc := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
	account := &Account{ID: 11, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	for _, status := range []int{200, 400, 401, 403, 500, 503} {
		require.False(t, svc.persistUpstreamModelCooldown(context.Background(), account, status, body, "ultra/argon"))
	}
	account.Type = AccountTypeOAuth
	require.False(t, svc.persistUpstreamModelCooldown(context.Background(), account, 429, body, "ultra/argon"))
	require.Zero(t, repo.modelRateLimitCalls)
}

func TestStructuredModelCooldownFailoverUsesCanonicalModelOnce(t *testing.T) {
	repo := &modelCooldownRepo{}
	rateLimits := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
	gateway := &OpenAIGatewayService{rateLimitService: rateLimits}
	rateLimits.SetAccountRuntimeBlocker(gateway)
	account := &Account{ID: 11, Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
		Credentials: map[string]any{"model_mapping": map[string]any{"argon": "ultra/argon", "ultra/argon": "must-not-remap"},
			"temp_unschedulable_enabled": true, "temp_unschedulable_rules": []any{map[string]any{"error_code": float64(429), "keywords": []any{"model_cooldown"}, "duration_minutes": float64(1)}}}}
	response := &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": []string{"3600"}}}
	body := []byte(`{"error":{"code":"model_cooldown","model":"ultra/argon","reset_seconds":3600}}`)
	require.True(t, gateway.handleFailoverSideEffects(context.Background(), response, account, body, "ultra/argon"))
	require.Equal(t, "ultra/argon", repo.lastModelRateLimitScope)
	require.WithinDuration(t, time.Now().Add(time.Hour), repo.lastModelRateLimitReset, 2*time.Second)
	require.False(t, gateway.isOpenAIAccountRuntimeBlocked(account))
	require.Zero(t, repo.rateLimitCalls)
	require.Zero(t, repo.tempUnschedCalls)
}
