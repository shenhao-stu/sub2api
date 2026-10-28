//go:build unit

package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestTokenRefreshPersistentTransportStopsSameCycleRetries(t *testing.T) {
	for _, platform := range []string{PlatformGrok, PlatformGemini} {
		for _, tc := range []struct {
			name     string
			message  string
			attempts int
		}{
			{"bad proxy credentials", "socks connect tcp proxy:443->auth.example:443: username/password authentication failed", 1},
			{"unreachable host", "socks connect tcp proxy:1080->auth.example:443: unknown error host unreachable", 1},
			{"transient eof", "EOF", 3},
		} {
			t.Run(platform+"/"+tc.name, func(t *testing.T) {
				account := &Account{ID: 433, Platform: platform, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: false,
					Credentials: map[string]any{"access_token": "test-access", "refresh_token": "test-refresh"}}
				repo := &tokenRefreshAccountRepo{}
				repo.accountsByID = map[int64]*Account{account.ID: account}
				cfg := &config.Config{TokenRefresh: config.TokenRefreshConfig{MaxRetries: 3}}
				svc := NewTokenRefreshService(repo, nil, nil, nil, nil, nil, nil, cfg, nil)
				refresher := &tokenRefresherStub{err: errors.New(tc.message)}
				err := svc.refreshWithRetry(context.Background(), account, refresher, refresher, time.Hour)
				require.ErrorIs(t, err, refresher.err)
				require.Equal(t, tc.attempts, refresher.calls)
				require.Equal(t, 1, repo.setTempUnschedCalls)
				require.Contains(t, repo.lastTempUnschedReason, "token refresh retry exhausted:")
				require.Zero(t, repo.setErrorCalls, "proxy rejection is not account credential invalidation")
				require.Zero(t, repo.updateCredentialsCalls)
				require.Equal(t, StatusActive, account.Status)
				require.False(t, account.Schedulable)
				if platform == PlatformGrok {
					require.Equal(t, 1, repo.conditionalTempCalls, "keep credential/proxy compare-and-swap")
				}
			})
		}
	}
}
