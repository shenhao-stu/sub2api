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
}

func (r *poolPressureRepo) SetOverloaded(context.Context, int64, time.Time) error {
	r.overloads++
	return nil
}

func TestAnthropicPoolPressureDoesNotCoolDownHealthySlots(t *testing.T) {
	const queue = `{"type":"error","error":{"type":"overloaded_error","code":"pool_overloaded","details":{"reason":"pool_queue_timeout","attempt_count":0}}}`
	for _, tc := range []struct {
		name, platform, kind, body string
		overloads                  int
	}{
		{"local queue", PlatformAnthropic, AccountTypeAPIKey, queue, 0},
		{"session queue", PlatformAnthropic, AccountTypeAPIKey, `{"type":"error","error":{"type":"overloaded_error","code":"pool_overloaded","details":{"reason":"session_queue_timeout"}}}`, 0},
		{"real provider overload", PlatformAnthropic, AccountTypeAPIKey, `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`, 1},
		{"pool upstream cooldown", PlatformAnthropic, AccountTypeAPIKey, `{"type":"error","error":{"type":"overloaded_error","code":"pool_overloaded","details":{"reason":"pool_overload_cooldown","attempt_count":0}}}`, 1},
		{"after dispatch", PlatformAnthropic, AccountTypeAPIKey, `{"type":"error","error":{"type":"overloaded_error","code":"pool_overloaded","details":{"reason":"pool_queue_timeout","attempt_count":1}}}`, 1},
		{"missing attempt count", PlatformAnthropic, AccountTypeAPIKey, `{"type":"error","error":{"type":"overloaded_error","code":"pool_overloaded","details":{"reason":"pool_queue_timeout"}}}`, 1},
		{"invalid payload", PlatformAnthropic, AccountTypeAPIKey, queue + " trailing", 1},
		{"other platform", PlatformOpenAI, AccountTypeAPIKey, queue, 1},
		{"OAuth account", PlatformAnthropic, AccountTypeOAuth, queue, 1},
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
