//go:build unit

package service

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/xai"
	"github.com/stretchr/testify/require"
)

type grokVersionRepo struct {
	SettingRepository
	mu                  sync.Mutex
	values              map[string]string
	readErr, errorWrite error
}

func (r *grokVersionRepo) GetMultiple(_ context.Context, keys []string) (map[string]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[string]string{}
	for _, k := range keys {
		if v, ok := r.values[k]; ok {
			out[k] = v
		}
	}
	return out, r.readErr
}
func (r *grokVersionRepo) Set(ctx context.Context, key, value string) error {
	return r.SetMultiple(ctx, map[string]string{key: value})
}
func (r *grokVersionRepo) SetMultiple(_ context.Context, values map[string]string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.errorWrite != nil {
		return r.errorWrite
	}
	for k, v := range values {
		r.values[k] = v
	}
	return nil
}
func (r *grokVersionRepo) CompareAndSet(_ context.Context, key, expected, value string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.errorWrite != nil {
		return false, r.errorWrite
	}
	if r.values[key] != expected {
		return false, nil
	}
	r.values[key] = value
	return true, nil
}

func TestGrokCLISyncCoalescesAndPersists(t *testing.T) {
	t.Setenv(xai.CLIVersionEnv, "")
	repo := &grokVersionRepo{values: map[string]string{}}
	identity := &xai.CLIIdentity{}
	var fetches atomic.Int32
	s := NewGrokCLIVersionSyncService(repo, identity, func(context.Context) (string, error) { fetches.Add(1); return "1.0.50", nil })
	defer s.Stop()
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); s.Refresh(context.Background(), xai.CLIClientVersion) }()
	}
	wg.Wait()
	require.EqualValues(t, 1, fetches.Load())
	require.Equal(t, "1.0.50", s.version())
	values, _ := repo.GetMultiple(context.Background(), grokCLIVersionKeys)
	require.Equal(t, "1.0.50", values[SettingKeyGrokCLIClientVersionSynced])
	require.NotEmpty(t, values[SettingKeyGrokCLIVersionLastCheckedAt])
	restarted := NewGrokCLIVersionSyncService(repo, &xai.CLIIdentity{}, func(context.Context) (string, error) { t.Error("must reuse recent verified metadata"); return "", nil })
	defer restarted.Stop()
	require.Equal(t, "1.0.50", restarted.Refresh(context.Background(), ""))
}

func TestGrokCLISyncPreservesPolicyAndLastGood(t *testing.T) {
	for _, tt := range []struct {
		name, manual, env, automatic, latest, want string
		fail                                       bool
	}{
		{name: "disabled", automatic: "false", latest: "1.0.51", want: "1.0.50"},
		{name: "manual", manual: "1.0.13", latest: "1.0.51", want: "1.0.13"},
		{name: "environment", env: "1.0.52", latest: "1.0.51", want: "1.0.52"},
		{name: "failed", latest: "", want: "1.0.50", fail: true},
		{name: "no downgrade", latest: "1.0.49", want: "1.0.50"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(xai.CLIVersionEnv, tt.env)
			repo := &grokVersionRepo{values: map[string]string{SettingKeyGrokCLIClientVersion: tt.manual, SettingKeyGrokCLIVersionAutoSyncEnabled: tt.automatic, SettingKeyGrokCLIClientVersionSynced: "1.0.50"}}
			fetches := 0
			svc := NewGrokCLIVersionSyncService(repo, &xai.CLIIdentity{}, func(context.Context) (string, error) {
				fetches++
				if tt.fail {
					return "", errors.New("offline")
				}
				return tt.latest, nil
			})
			defer svc.Stop()
			require.Equal(t, tt.want, svc.Refresh(context.Background(), ""))
			if tt.automatic == "false" || tt.manual != "" || tt.env != "" {
				require.Zero(t, fetches)
			} else {
				require.Equal(t, 1, fetches)
			}
			svc.Refresh(context.Background(), tt.want)
			require.LessOrEqual(t, fetches, 1)
			values, _ := repo.GetMultiple(context.Background(), grokCLIVersionKeys)
			require.Equal(t, "1.0.50", values[SettingKeyGrokCLIClientVersionSynced])
		})
	}
}

func TestGrokCLISyncRechecksPolicyAfterFetch(t *testing.T) {
	t.Setenv(xai.CLIVersionEnv, "")
	for _, disable := range []bool{true, false} {
		repo := &grokVersionRepo{values: map[string]string{}}
		svc := NewGrokCLIVersionSyncService(repo, &xai.CLIIdentity{}, func(context.Context) (string, error) {
			if disable {
				require.NoError(t, repo.Set(context.Background(), SettingKeyGrokCLIVersionAutoSyncEnabled, "false"))
			} else {
				require.NoError(t, repo.Set(context.Background(), SettingKeyGrokCLIClientVersionSynced, "1.0.60"))
			}
			return "1.0.50", nil
		})
		got := svc.Refresh(context.Background(), "")
		svc.Stop()
		if disable {
			require.Equal(t, xai.CLIClientVersion, got)
		} else {
			require.Equal(t, "1.0.60", got)
		}
	}
}

func TestGrokCLISyncFailuresDoNotChangeLiveIdentity(t *testing.T) {
	t.Setenv(xai.CLIVersionEnv, "")
	for _, readFailure := range []bool{true, false} {
		repo := &grokVersionRepo{values: map[string]string{SettingKeyGrokCLIClientVersionSynced: "1.0.50"}}
		identity := &xai.CLIIdentity{}
		identity.Update(xai.CLIVersionPolicy{Synced: "1.0.50"})
		if readFailure {
			repo.readErr = errors.New("database down")
		} else {
			repo.errorWrite = errors.New("database read only")
		}
		svc := NewGrokCLIVersionSyncService(repo, identity, func(context.Context) (string, error) { return "1.0.51", nil })
		require.Equal(t, "1.0.50", svc.Refresh(context.Background(), ""))
		svc.Stop()
	}
}

func TestGrokCLISyncStopCancelsMetadataFetch(t *testing.T) {
	t.Setenv(xai.CLIVersionEnv, "")
	repo := &grokVersionRepo{values: map[string]string{}}
	started, canceled := make(chan struct{}), make(chan struct{})
	svc := NewGrokCLIVersionSyncService(repo, &xai.CLIIdentity{}, func(ctx context.Context) (string, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return "", ctx.Err()
	})
	svc.Start()
	defer svc.Stop()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("not started")
	}
	svc.Stop()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("fetch still running")
	}
}

func TestGrokCLISettingsDefaultsAndValidation(t *testing.T) {
	originalMapping := xai.RuntimeModelMappingOptions()
	t.Cleanup(func() { xai.SetRuntimeModelMappingOptions(originalMapping) })
	t.Setenv(xai.CLIVersionEnv, "")
	settings := (&SettingService{cfg: &config.Config{}}).parseSettings(map[string]string{})
	require.True(t, settings.GrokCLIVersionAutoSyncEnabled)
	require.Equal(t, xai.CLIClientVersion, settings.GrokCLIClientVersionEffective)
	for _, version := range []string{"0.2.120", "1.0.50-beta.1", "1.0.50\r\nBad: yes"} {
		repo := &settingUpdateRepoStub{}
		svc := NewSettingService(repo, &config.Config{})
		settings.GrokCLIClientVersion = version
		require.Error(t, svc.UpdateSettings(context.Background(), settings))
		require.Nil(t, repo.updates)
	}
}

func TestGrokCLIUpgradeRejectionCannotCoolOrFailover(t *testing.T) {
	body := []byte(`{"error":"Your Grok CLI version (1.0.13) is outdated."}`)
	decision := classifyGrokUpstreamFailure(426, body, "grok-4.7-build-fast")
	require.False(t, decision.ShouldCooldown)
	require.False(t, decision.ShouldFailover)
	account := &Account{ID: 357, Platform: "grok", Type: "oauth"}
	svc := &OpenAIGatewayService{}
	svc.handleGrokAccountUpstreamError(context.Background(), account, 426, http.Header{"X-RateLimit-Remaining": {"0"}}, body)
	require.Nil(t, account.TempUnschedulableUntil)
	require.False(t, svc.shouldFailoverGrokUpstreamError(426, body))
}
