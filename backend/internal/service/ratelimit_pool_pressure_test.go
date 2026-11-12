//go:build unit

package service

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type poolPressureRepo struct {
	mockAccountRepoForGemini
	overloads int
	errors    int
}

func (r *poolPressureRepo) SetOverloaded(context.Context, int64, time.Time) error {
	r.overloads++
	return nil
}

func (r *poolPressureRepo) SetError(context.Context, int64, string) error {
	r.errors++
	return nil
}

func TestAnthropicPoolPressureDoesNotCoolDownHealthySlots(t *testing.T) {
	const queue = `{"type":"error","error":{"type":"overloaded_error","code":"pool_overloaded","details":{"reason":"pool_queue_timeout","attempt_count":0}}}`
	for _, tc := range []struct {
		name, platform, kind, body string
		overloads                  int
	}{
		{"local queue", PlatformAnthropic, AccountTypeAPIKey, queue, 0},
		// vm2api's public error mapper removes internal selection details.
		{"public pool capacity", PlatformAnthropic, AccountTypeAPIKey, `{"error":{"code":"pool_overloaded","message":"号池负载过高，稍后再试","type":"overloaded_error"}}`, 0},
		{"public pool capacity with envelope", PlatformAnthropic, AccountTypeAPIKey, `{"type":"error","error":{"code":"pool_overloaded","type":"overloaded_error"}}`, 0},
		{"explicit unknown details", PlatformAnthropic, AccountTypeAPIKey, `{"error":{"code":"pool_overloaded","type":"overloaded_error","details":{}}}`, 1},
		{"explicit null details", PlatformAnthropic, AccountTypeAPIKey, `{"error":{"code":"pool_overloaded","type":"overloaded_error","details":null}}`, 1},
		{"non-error envelope", PlatformAnthropic, AccountTypeAPIKey, `{"type":"message","error":{"code":"pool_overloaded","type":"overloaded_error"}}`, 1},
		{"public provider overload", PlatformAnthropic, AccountTypeAPIKey, `{"error":{"message":"Overloaded","type":"overloaded_error"}}`, 1},
		{"session queue", PlatformAnthropic, AccountTypeAPIKey, `{"type":"error","error":{"type":"overloaded_error","code":"pool_overloaded","details":{"reason":"session_queue_timeout"}}}`, 0},
		{"real provider overload", PlatformAnthropic, AccountTypeAPIKey, `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`, 1},
		{"pool upstream cooldown", PlatformAnthropic, AccountTypeAPIKey, `{"type":"error","error":{"type":"overloaded_error","code":"pool_overloaded","details":{"reason":"pool_overload_cooldown","attempt_count":0}}}`, 1},
		{"after dispatch", PlatformAnthropic, AccountTypeAPIKey, `{"type":"error","error":{"type":"overloaded_error","code":"pool_overloaded","details":{"reason":"pool_queue_timeout","attempt_count":1}}}`, 1},
		{"missing attempt count", PlatformAnthropic, AccountTypeAPIKey, `{"type":"error","error":{"type":"overloaded_error","code":"pool_overloaded","details":{"reason":"pool_queue_timeout"}}}`, 1},
		{"invalid payload", PlatformAnthropic, AccountTypeAPIKey, queue + " trailing", 1},
		{"other platform", PlatformOpenAI, AccountTypeAPIKey, queue, 1},
		{"OAuth account", PlatformAnthropic, AccountTypeOAuth, queue, 1},
		{"public capacity other platform", PlatformOpenAI, AccountTypeAPIKey, `{"error":{"code":"pool_overloaded","type":"overloaded_error"}}`, 1},
		{"public capacity OAuth", PlatformAnthropic, AccountTypeOAuth, `{"error":{"code":"pool_overloaded","type":"overloaded_error"}}`, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &poolPressureRepo{}
			svc := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
			account := &Account{ID: 21, Platform: tc.platform, Type: tc.kind}
			require.False(t, svc.HandleUpstreamError(context.Background(), account, 529, nil, []byte(tc.body)))
			require.Equal(t, tc.overloads, repo.overloads)
		})
	}
}

func TestAnthropicPoolPressurePreservesExplicitErrorPolicy(t *testing.T) {
	repo := &poolPressureRepo{}
	svc := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
	account := &Account{ID: 21, Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Credentials: map[string]any{
		"custom_error_codes_enabled": true,
		"custom_error_codes":         []any{float64(529)},
	}}
	body := []byte(`{"error":{"code":"pool_overloaded","type":"overloaded_error"}}`)
	require.True(t, svc.HandleUpstreamError(context.Background(), account, 529, nil, body))
	require.Equal(t, 1, repo.errors)
	require.Zero(t, repo.overloads)
}
