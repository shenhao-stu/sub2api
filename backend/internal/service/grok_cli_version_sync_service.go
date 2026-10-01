package service

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/xai"
	"golang.org/x/mod/semver"
	"golang.org/x/sync/singleflight"
)

const (
	SettingKeyGrokCLIClientVersion          = "grok_cli_client_version"
	SettingKeyGrokCLIVersionAutoSyncEnabled = "grok_cli_version_auto_sync_enabled"
	SettingKeyGrokCLIClientVersionSynced    = "grok_cli_client_version_synced"
	SettingKeyGrokCLIVersionLastCheckedAt   = "grok_cli_version_last_checked_at"
	SettingKeyGrokCLIVersionLastError       = "grok_cli_version_last_error"
	grokCLISyncInterval                     = time.Hour
	grokCLISyncRetryInterval                = 5 * time.Minute
)

var grokCLIVersionKeys = []string{SettingKeyGrokCLIClientVersion, SettingKeyGrokCLIVersionAutoSyncEnabled,
	SettingKeyGrokCLIClientVersionSynced, SettingKeyGrokCLIVersionLastCheckedAt, SettingKeyGrokCLIVersionLastError}

func grokCLIPolicy(values map[string]string) xai.CLIVersionPolicy {
	return xai.CLIVersionPolicy{Manual: strings.TrimSpace(values[SettingKeyGrokCLIClientVersion]), Synced: values[SettingKeyGrokCLIClientVersionSynced]}
}

// GrokCLIVersionSyncService owns metadata I/O and publishes immutable identity
// snapshots. Request paths never read the DB, except after a confirmed 426.
type GrokCLIVersionSyncService struct {
	repo        SettingRepository
	identity    *xai.CLIIdentity
	fetch       func(context.Context) (string, error)
	ctx         context.Context
	cancel      context.CancelFunc
	group       singleflight.Group
	policyMu    sync.Mutex
	startOnce   sync.Once
	wg          sync.WaitGroup
	lastAttempt time.Time // accessed only inside group.DoChan("sync")
}

func NewGrokCLIVersionSyncService(repo SettingRepository, identity *xai.CLIIdentity, fetch func(context.Context) (string, error)) *GrokCLIVersionSyncService {
	ctx, cancel := context.WithCancel(context.Background())
	return &GrokCLIVersionSyncService{repo: repo, identity: identity, fetch: fetch, ctx: ctx, cancel: cancel}
}

func (s *GrokCLIVersionSyncService) Start() {
	s.startOnce.Do(func() {
		s.Reload()
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			ticker := time.NewTicker(time.Minute)
			defer ticker.Stop()
			for {
				s.Refresh(s.ctx, "")
				select {
				case <-s.ctx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	})
}

func (s *GrokCLIVersionSyncService) Stop() { s.cancel(); s.wg.Wait() }

func (s *GrokCLIVersionSyncService) Reload() {
	ctx, cancel := context.WithTimeout(s.ctx, 3*time.Second)
	defer cancel()
	if _, err := s.load(ctx); err != nil {
		slog.Warn("grok_cli_identity_load_failed")
	}
}

func (s *GrokCLIVersionSyncService) load(ctx context.Context) (map[string]string, error) {
	s.policyMu.Lock()
	defer s.policyMu.Unlock()
	values, err := s.repo.GetMultiple(ctx, grokCLIVersionKeys)
	if err == nil {
		s.identity.Update(grokCLIPolicy(values))
	}
	return values, err
}

func (s *GrokCLIVersionSyncService) version() string {
	v, _ := s.identity.Policy().Resolve(os.Getenv(xai.CLIVersionEnv))
	return v
}

// Refresh coalesces outage-triggered refreshes, bounded to one metadata attempt
// per five minutes. Normal operation checks hourly. Manual/env pins are honored.
func (s *GrokCLIVersionSyncService) Refresh(ctx context.Context, rejected string) string {
	if ctx.Err() != nil || s.ctx.Err() != nil {
		return s.version()
	}
	if rejected != "" && semver.Compare("v"+s.version(), "v"+rejected) > 0 {
		return s.version()
	}

	result := s.group.DoChan("sync", func() (any, error) {
		syncCtx, cancel := context.WithTimeout(s.ctx, 15*time.Second)
		defer cancel()
		s.sync(syncCtx, rejected)
		return s.version(), nil
	})
	select {
	case <-ctx.Done():
		return s.version()
	case <-s.ctx.Done():
		return s.version()
	case r := <-result:
		return r.Val.(string)
	}
}

func (s *GrokCLIVersionSyncService) sync(ctx context.Context, rejected string) {
	values, err := s.load(ctx)
	if err != nil {
		slog.Warn("grok_cli_identity_load_failed")
		return
	}
	if !grokCLIAutoSyncAllowed(values) {
		return
	}
	if rejected != "" && semver.Compare("v"+s.version(), "v"+rejected) > 0 {
		return
	}
	now := time.Now()
	checked, _ := time.Parse(time.RFC3339, values[SettingKeyGrokCLIVersionLastCheckedAt])
	if now.Sub(s.lastAttempt) < grokCLISyncRetryInterval || (rejected == "" && now.Sub(checked) < grokCLISyncInterval) {
		return
	}
	s.lastAttempt = now
	latest, err := s.fetch(ctx)
	if err != nil || !xai.IsSupportedCLIVersion(latest) {
		slog.Warn("grok_cli_version_sync_failed", "reason", "official_metadata_unavailable")
		if ctx.Err() == nil {
			_ = s.repo.Set(ctx, SettingKeyGrokCLIVersionLastError, "official_metadata_unavailable")
		}
		return
	}
	// Re-read policy after network I/O: never write operator-owned keys or replace
	// a newer version saved by another worker while metadata was in flight.
	values, err = s.load(ctx)
	if err != nil || !grokCLIAutoSyncAllowed(values) {
		return
	}
	current := values[SettingKeyGrokCLIClientVersionSynced]
	updates := map[string]string{SettingKeyGrokCLIVersionLastCheckedAt: now.UTC().Format(time.RFC3339), SettingKeyGrokCLIVersionLastError: ""}
	if (!xai.IsSupportedCLIVersion(current) || semver.Compare("v"+latest, "v"+current) > 0) && semver.Compare("v"+latest, "v"+xai.CLIClientVersion) >= 0 {
		cas, ok := s.repo.(interface {
			CompareAndSet(context.Context, string, string, string) (bool, error)
		})
		if !ok {
			slog.Error("grok_cli_version_sync_atomic_store_missing")
			return
		}
		if _, err = cas.CompareAndSet(ctx, SettingKeyGrokCLIClientVersionSynced, current, latest); err != nil {
			slog.Warn("grok_cli_version_sync_persist_failed")
			return
		}
	}
	if err = s.repo.SetMultiple(ctx, updates); err != nil {
		slog.Warn("grok_cli_version_sync_persist_failed")
		return
	}
	if _, err = s.load(ctx); err != nil {
		slog.Warn("grok_cli_identity_load_failed")
		return
	}
	slog.Info("grok_cli_version_checked", "version", s.version())
}

func grokCLIAutoSyncAllowed(values map[string]string) bool {
	return values[SettingKeyGrokCLIVersionAutoSyncEnabled] != "false" &&
		strings.TrimSpace(values[SettingKeyGrokCLIClientVersion]) == "" &&
		!xai.IsSupportedCLIVersion(strings.TrimSpace(os.Getenv(xai.CLIVersionEnv)))
}
