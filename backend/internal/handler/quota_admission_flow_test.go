//go:build unit

package handler

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	middleware "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type admissionAccounts struct {
	service.AccountRepository
	candidates     []service.Account
	currentReads   int
	disabledAtWake bool
}

func (r *admissionAccounts) ListModelAvailabilityCandidates(context.Context, *int64, []string, bool) ([]service.Account, error) {
	return r.candidates, nil
}

func (r *admissionAccounts) ListSchedulableByGroupIDAndPlatforms(_ context.Context, group int64, platforms []string) ([]service.Account, error) {
	r.currentReads++
	if r.disabledAtWake {
		return nil, nil
	}
	var result []service.Account
	for _, account := range r.candidates {
		if account.IsSchedulable() && slices.Contains(account.GroupIDs, group) && slices.Contains(platforms, account.Platform) {
			result = append(result, account)
		}
	}
	return result, nil
}

type admissionKey struct {
	service.APIKeyRepository
	key *service.APIKey
}

func (r *admissionKey) GetByID(context.Context, int64) (*service.APIKey, error) { return r.key, nil }

type admissionSettings struct{ service.SettingRepository }

func (admissionSettings) GetValue(context.Context, string) (string, error) {
	return `{"enabled":true,"trusted_api_key_ids":[1],"tag_prefix":"customer:"}`, nil
}

type admissionUsage struct {
	service.UsageLogRepository
	rows []*service.UsageLog
}

func (r *admissionUsage) CreateBestEffort(_ context.Context, row *service.UsageLog) error {
	r.rows = append(r.rows, row)
	return nil
}

type admissionUpstream struct{ grokMediaSlotUpstream }

func (u *admissionUpstream) DoWithTLS(r *http.Request, p string, id int64, n int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(r, p, id, n)
}

func TestQuotaAdmissionWaitMessages(t *testing.T) {
	for _, mode := range []string{"same_guard_reset", "reset", "general_direct", "general_tagged", "stale_membership", "disabled_at_wake", "guard_then_reset", "revoked", "cancel", "no_opt_in"} {
		t.Run(mode, func(t *testing.T) {
			gid := int64(1)
			group := &service.Group{ID: gid, Hydrated: true, Platform: service.PlatformAnthropic, Status: service.StatusActive, RateMultiplier: 1}
			key := &service.APIKey{ID: 1, UserID: 1, User: &service.User{ID: 1, Status: service.StatusActive, Concurrency: 2, Balance: 99}, GroupID: &gid, Group: group, Status: service.StatusActive}
			reset := time.Now().Add(450 * time.Millisecond)
			account := service.Account{ID: 2, Name: "fixture", Platform: service.PlatformAnthropic, Type: service.AccountTypeAPIKey,
				Extra: map[string]any{"anthropic_passthrough": true}, Credentials: map[string]any{"api_key": "fixture", "base_url": "https://api.anthropic.com"},
				Status: service.StatusActive, Schedulable: true, Priority: 1, Concurrency: 1, RateLimitResetAt: &reset, GroupIDs: []int64{gid}, AccountGroups: []service.AccountGroup{{AccountID: 2, GroupID: gid}}}
			if mode == "same_guard_reset" {
				account.RateLimitResetAt = nil
			}
			repo := &admissionAccounts{candidates: []service.Account{account}}
			slots := []*service.Account{&account}
			if mode == "guard_then_reset" {
				guard := account
				guard.ID, guard.Priority, guard.RateLimitResetAt = 1, 0, nil
				guard.AccountGroups = []service.AccountGroup{{AccountID: 1, GroupID: gid}}
				slots = append(slots, &guard)
				repo.candidates = append(repo.candidates, guard)
			}
			if mode == "stale_membership" || mode == "disabled_at_wake" {
				repo.disabledAtWake = mode == "disabled_at_wake"
				// Production builds the group snapshot while this account is cooling.
				// Its ID can still be missing from Redis when the database reset expires.
				slots = nil
			}
			snapshot := service.NewSchedulerSnapshotService(&fakeSchedulerCache{accounts: slots}, nil, nil, nil, nil)
			up := &admissionUpstream{}
			up.call = func(req *http.Request, id int64) (*http.Response, error) {
				if mode == "same_guard_reset" && up.calls == 1 {
					return &http.Response{StatusCode: 429, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(strings.Replace(admissionGuardFixture, `"attempt_count":0`, `"retry_after_ms":500,"attempt_count":0`, 1)))}, nil
				}
				if mode == "guard_then_reset" && id == 1 {
					require.Equal(t, 1, up.calls, "request rejected by the guard must not return to this account")
					return &http.Response{StatusCode: 429, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(admissionGuardFixture))}, nil
				}
				require.False(t, time.Now().Before(reset))
				require.EqualValues(t, 2, id)
				raw, err := io.ReadAll(req.Body)
				require.NoError(t, err)
				require.Contains(t, string(raw), "original-fixture-history")
				payload := "event: message_start\ndata: " + `{"type":"message_start","message":{"id":"msg_fixture","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[],"usage":{"input_tokens":9,"output_tokens":0}}}` + "\n\nevent: message_delta\ndata: " + `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":3}}` + "\n\nevent: message_stop\ndata: " + `{"type":"message_stop"}` + "\n\n"
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}, "Request-Id": {"fixture-1"}}, Body: io.NopCloser(strings.NewReader(payload))}, nil
			}
			cfg := &config.Config{RunMode: config.RunModeSimple}
			cost := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
			defer cost.Stop()
			keyRepo := &admissionKey{key: key}
			if mode == "revoked" {
				copy := *key
				copy.Status = service.StatusAPIKeyDisabled
				keyRepo.key = &copy
			}
			usage := &admissionUsage{}
			gw := service.NewGatewayService(repo, &fakeGroupRepo{group: group}, usage, nil, nil, nil, nil, nil, cfg, snapshot, nil, service.NewBillingService(cfg, nil), nil, nil, nil, up, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
			h := &GatewayHandler{gatewayService: gw, concurrencyHelper: NewConcurrencyHelper(service.NewConcurrencyService(&fakeConcurrencyCache{}), SSEPingFormatClaude, 0), billingCacheService: cost,
				settingService: service.NewSettingService(admissionSettings{}, cfg), apiKeyService: service.NewAPIKeyService(keyRepo, nil, nil, nil, nil, nil, cfg), maxAccountSwitches: 2, cfg: cfg, quotaWait: newQuotaAdmissionWait("customer:1")}
			if strings.HasPrefix(mode, "general_") {
				h.quotaWait = newQuotaAdmissionWait("*")
			}
			h.quotaWait.heartbeat = 10 * time.Millisecond
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{"model":"claude-sonnet-4-5","max_tokens":16,"stream":true,"messages":[{"role":"user","content":"original-fixture-history"}]}`))
			ctx, cancel := context.WithCancel(req.Context())
			defer cancel()
			if mode != "no_opt_in" && mode != "general_direct" {
				ctx = service.WithAccountAffinityTags(ctx, []string{"customer:1"})
			}
			if mode == "cancel" {
				timer := time.AfterFunc(150*time.Millisecond, cancel)
				defer timer.Stop()
			}
			c.Request = req.WithContext(ctx)
			if mode != "general_direct" {
				c.Request.Header.Set(service.AccountAffinityHeader, "1")
			}
			c.Set(string(middleware.ContextKeyAPIKey), key)
			c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: key.UserID, Concurrency: 2})
			h.Messages(c)
			if mode == "stale_membership" || mode == "disabled_at_wake" {
				require.Equal(t, 1, repo.currentReads, "one fresh selection after the window")
			}
			if mode == "stale_membership" || mode == "same_guard_reset" || mode == "reset" || mode == "guard_then_reset" || strings.HasPrefix(mode, "general_") {
				expectedForwards := 1
				if mode == "guard_then_reset" || mode == "same_guard_reset" {
					expectedForwards++
				}
				require.Equal(t, expectedForwards, up.calls, rec.Body.String())
				require.Contains(t, rec.Body.String(), "event: ping")
				require.Contains(t, rec.Body.String(), "event: message_stop")
				require.Len(t, usage.rows, 1)
				require.Equal(t, 9, usage.rows[0].InputTokens)
				require.Equal(t, 3, usage.rows[0].OutputTokens)
			} else {
				require.Zero(t, up.calls, rec.Body.String())
				require.Empty(t, usage.rows)
			}
			if mode == "revoked" {
				require.Contains(t, rec.Body.String(), "permission_error")
			}
			if mode == "no_opt_in" {
				require.Equal(t, 429, rec.Code)
			}
		})
	}
}
