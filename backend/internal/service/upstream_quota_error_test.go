//go:build unit

package service

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/model"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const upstreamQuotaErrorBody = `{"error":{"code":"insufficient_quota","type":"insufficient_quota","message":"Insufficient account balance"}}`

func TestIsUpstreamQuotaExhausted(t *testing.T) {
	for _, tt := range []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{"provider balance", 429, upstreamQuotaErrorBody, true},
		{"code only", 429, `{"error":{"code":"insufficient_quota"}}`, true},
		{"type only", 429, `{"error":{"type":"insufficient_quota"}}`, true},
		{"real usage preserved", 429, `{"error":{"code":"insufficient_quota"},"usage":{"input_tokens":10}}`, true},
		{"normal rate limit", 429, `{"error":{"code":"rate_limit_exceeded","type":"tokens"}}`, false},
		{"message mention", 429, `{"error":{"message":"insufficient_quota"}}`, false},
		{"nested message", 429, `{"error":{"message":"{\"error\":{\"code\":\"insufficient_quota\"}}"}}`, false},
		{"customer balance", 429, `{"error":{"code":"insufficient_balance"}}`, false},
		{"customer quota", 429, `{"error":{"code":"quota_exceeded"}}`, false},
		{"success", 200, upstreamQuotaErrorBody, false},
		{"authentication", 401, upstreamQuotaErrorBody, false},
		{"payment", 402, upstreamQuotaErrorBody, false},
		{"content refusal", 403, `{"error":{"code":"content_policy_violation"}}`, false},
		{"request echo", 429, `{"request":{"error":{"code":"insufficient_quota"}}}`, false},
		{"top level code", 429, `{"code":"insufficient_quota"}`, false},
		{"code object", 429, `{"error":{"code":{"value":"insufficient_quota"}}}`, false},
		{"truncated", 429, `{"error":{"code":"insufficient_quota"}`, false},
		{"trailing data", 429, upstreamQuotaErrorBody + `extra`, false},
		{"null error", 429, `{"error":null}`, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsUpstreamQuotaExhausted(tt.status, []byte(tt.body)))
		})
	}
}

func TestUpstreamQuotaResetAt(t *testing.T) {
	now := time.Date(2026, 9, 28, 8, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		name       string
		retryAfter string
		fallback   time.Duration
		enabled    bool
		want       time.Duration
	}{
		{"quota default", "", 5 * time.Second, true, 10 * time.Minute},
		{"longer global cooldown", "", 30 * time.Minute, true, 30 * time.Minute},
		{"retry floor", "1", 5 * time.Second, true, 10 * time.Minute},
		{"retry seconds", "1800", 5 * time.Second, true, 30 * time.Minute},
		{"retry date", now.Add(time.Hour).Format(http.TimeFormat), 5 * time.Second, true, time.Hour},
		{"three hour retry", "10800", 5 * time.Second, true, 3 * time.Hour},
		{"one day retry", "86400", 5 * time.Second, true, 24 * time.Hour},
		{"seven day limit", "604800", 5 * time.Second, true, 7 * 24 * time.Hour},
		{"seven day date", now.Add(7 * 24 * time.Hour).Format(http.TimeFormat), 5 * time.Second, true, 7 * 24 * time.Hour},
		{"over seven days", "604801", 5 * time.Second, true, 10 * time.Minute},
		{"over seven day date", now.Add(7*24*time.Hour + time.Second).Format(http.TimeFormat), 5 * time.Second, true, 10 * time.Minute},
		{"huge date invalid", now.AddDate(100, 0, 0).Format(http.TimeFormat), 5 * time.Second, true, 10 * time.Minute},
		{"huge seconds invalid", "31536000000", 5 * time.Second, true, 10 * time.Minute},
		{"uint64 overflow", "18446744073709551616", 5 * time.Second, true, 10 * time.Minute},
		{"invalid", "invalid", 5 * time.Second, true, 10 * time.Minute},
		{"past date", now.Add(-time.Hour).Format(http.TimeFormat), 5 * time.Second, true, 10 * time.Minute},
		{"negative", "-1", 5 * time.Second, true, 10 * time.Minute},
		{"infinity", "Inf", 5 * time.Second, true, 10 * time.Minute},
		{"disabled", "", 5 * time.Second, false, 0},
		{"disabled with upstream reset", "60", 5 * time.Second, false, time.Minute},
		{"disabled with invalid reset", "604801", 5 * time.Second, false, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			headers := http.Header{}
			headers.Set("Retry-After", tt.retryAfter)
			got := upstreamQuotaResetAt(now, headers, tt.fallback, tt.enabled)
			if tt.want == 0 {
				require.True(t, got.IsZero())
			} else {
				require.Equal(t, now.Add(tt.want), got)
			}
		})
	}
}

type upstreamQuotaRepo struct {
	rateLimitAccountRepoStub
	account     *Account
	atomicCalls int
	writeErr    error
	loadErr     error
}

func (r *upstreamQuotaRepo) SetRateLimitedIfLater(ctx context.Context, _ int64, resetAt time.Time) error {
	r.atomicCalls++
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.writeErr != nil {
		return r.writeErr
	}
	if r.account.RateLimitResetAt == nil || resetAt.After(*r.account.RateLimitResetAt) {
		r.account.RateLimitResetAt = &resetAt
	}
	return nil
}

func (r *upstreamQuotaRepo) GetByID(context.Context, int64) (*Account, error) {
	return r.account, r.loadErr
}

func TestHandleUpstreamError_QuotaCooldownIsRecoverable(t *testing.T) {
	for _, platform := range []string{PlatformAnthropic, PlatformOpenAI, PlatformGemini, PlatformGrok} {
		t.Run(platform, func(t *testing.T) {
			account := &Account{ID: 123, Type: AccountTypeAPIKey, Platform: platform, Status: StatusActive, Schedulable: true}
			repo := &upstreamQuotaRepo{account: account}
			blocker := &runtimeBlockRecorder{}
			svc := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
			svc.SetAccountRuntimeBlocker(blocker)
			before := time.Now()
			svc.HandleUpstreamError(context.Background(), account, 429, nil, []byte(upstreamQuotaErrorBody))
			after := time.Now()
			require.Equal(t, 1, repo.atomicCalls)
			require.Zero(t, repo.rateLimitedCalls, "never use an unconditional rate-limit write")
			require.Zero(t, repo.setErrorCalls)
			require.Zero(t, repo.tempCalls)
			require.Zero(t, repo.updateExtraCalls)
			require.Equal(t, StatusActive, account.Status)
			require.True(t, account.Schedulable)
			require.NotNil(t, account.RateLimitResetAt)
			require.False(t, account.RateLimitResetAt.Before(before.Add(10*time.Minute)))
			require.False(t, account.RateLimitResetAt.After(after.Add(10*time.Minute)))
			require.Equal(t, []string{"upstream_insufficient_quota"}, blocker.reasons)
			require.Equal(t, *account.RateLimitResetAt, blocker.until[0])
		})
	}
}

func TestHandleUpstreamError_QuotaPreservesNewerResetWithStaleAccount(t *testing.T) {
	reset := time.Now().Add(6 * time.Hour)
	stale := &Account{ID: 123, Type: AccountTypeAPIKey, Platform: PlatformOpenAI}
	current := *stale
	current.RateLimitResetAt = &reset
	repo := &upstreamQuotaRepo{account: &current}
	blocker := &runtimeBlockRecorder{}
	svc := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
	svc.SetAccountRuntimeBlocker(blocker)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	svc.HandleUpstreamError(ctx, stale, 429, nil, []byte(upstreamQuotaErrorBody))
	require.Equal(t, 1, repo.atomicCalls)
	require.Equal(t, reset, *current.RateLimitResetAt)
	require.Equal(t, []time.Time{reset}, blocker.until)
}

func TestHandleUpstreamError_QuotaHonorsExistingPolicies(t *testing.T) {
	// Generic quota errors still honor pool mode; only an explicit credential
	// wallet rejection overrides it (covered by TestWalletExhaustionOverridesPool).
	body := []byte(`{"error":{"code":"insufficient_quota","message":"Monthly quota exhausted"}}`)
	for _, tt := range []struct {
		name        string
		credentials map[string]any
		tempCalls   int
	}{
		{"pool mode", map[string]any{"pool_mode": true}, 0},
		{"custom error codes", map[string]any{"custom_error_codes_enabled": true, "custom_error_codes": []any{float64(401)}}, 0},
		{"explicit temporary rule", map[string]any{
			"temp_unschedulable_enabled": true,
			"temp_unschedulable_rules": []any{map[string]any{
				"error_code": float64(429), "keywords": []any{"insufficient_quota"}, "duration_minutes": float64(1),
			}},
		}, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			account := &Account{ID: 123, Type: AccountTypeAPIKey, Platform: PlatformAnthropic, Credentials: tt.credentials}
			repo := &upstreamQuotaRepo{account: account}
			svc := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
			svc.HandleUpstreamError(context.Background(), account, 429, nil, body)
			require.Zero(t, repo.atomicCalls)
			require.Equal(t, tt.tempCalls, repo.tempCalls)
			require.Zero(t, repo.setErrorCalls)
		})
	}
}

func TestHandleUpstreamError_QuotaLeavesOfficialWindowHandling(t *testing.T) {
	reset := time.Now().Add(3 * time.Hour).Truncate(time.Second)
	headers := http.Header{}
	headers.Set("anthropic-ratelimit-unified-5h-utilization", "1.02")
	headers.Set("anthropic-ratelimit-unified-5h-reset", strconv.FormatInt(reset.Unix(), 10))
	repo := &anthropicWindowLimitRepo{}
	svc := NewRateLimitService(repo, nil, nil, nil, nil)
	account := &Account{ID: 123, Type: AccountTypeOAuth, Platform: PlatformAnthropic}
	svc.HandleUpstreamError(context.Background(), account, 429, headers, []byte(upstreamQuotaErrorBody))
	require.Equal(t, 1, repo.rateLimitCalls)
	require.Equal(t, reset, repo.lastRateLimitReset)
}

func TestHandleUpstreamError_QuotaCooldownDisabledDoesNotClearReset(t *testing.T) {
	reset := time.Now().Add(time.Hour)
	account := &Account{ID: 123, Type: AccountTypeAPIKey, Platform: PlatformOpenAI, RateLimitResetAt: &reset}
	repo := &upstreamQuotaRepo{account: account}
	settings := newMockSettingRepo()
	settings.data[SettingKeyRateLimit429CooldownSettings] = `{"enabled":false,"cooldown_seconds":5}`
	svc := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
	svc.SetSettingService(NewSettingService(settings, &config.Config{}))
	svc.HandleUpstreamError(context.Background(), account, 429, nil, []byte(upstreamQuotaErrorBody))
	require.Zero(t, repo.atomicCalls)
	require.Zero(t, repo.rateLimitedCalls)
	require.Equal(t, reset, *account.RateLimitResetAt)
}

type quotaContextSettingRepo struct {
	*mockSettingRepo
	readContextErr error
}

func (r *quotaContextSettingRepo) GetValue(ctx context.Context, key string) (string, error) {
	r.readContextErr = ctx.Err()
	if r.readContextErr != nil {
		return "", r.readContextErr
	}
	return r.mockSettingRepo.GetValue(ctx, key)
}

func TestHandleUpstreamError_QuotaCancelledRequestStillHonorsDisabledSetting(t *testing.T) {
	account := &Account{ID: 123, Type: AccountTypeAPIKey, Platform: PlatformOpenAI}
	repo := &upstreamQuotaRepo{account: account}
	settings := &quotaContextSettingRepo{mockSettingRepo: newMockSettingRepo()}
	settings.data[SettingKeyRateLimit429CooldownSettings] = `{"enabled":false,"cooldown_seconds":5}`
	svc := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
	svc.SetSettingService(NewSettingService(settings, &config.Config{}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	svc.HandleUpstreamError(ctx, account, 429, nil, []byte(upstreamQuotaErrorBody))
	require.NoError(t, settings.readContextErr, "cancelled requests must not make settings reads fall back to enabled")
	require.Zero(t, repo.atomicCalls)
	require.Zero(t, repo.rateLimitedCalls)
	require.Nil(t, account.RateLimitResetAt)
}

func TestHandleUpstreamError_QuotaFailuresDoNotFallBackToUnconditionalWrite(t *testing.T) {
	for _, tt := range []struct {
		name     string
		writeErr error
		loadErr  error
	}{
		{"write failure", errors.New("write failed"), nil},
		{"load failure", nil, errors.New("load failed")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			account := &Account{ID: 123, Type: AccountTypeAPIKey, Platform: PlatformOpenAI}
			repo := &upstreamQuotaRepo{account: account, writeErr: tt.writeErr, loadErr: tt.loadErr}
			blocker := &runtimeBlockRecorder{}
			svc := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
			svc.SetAccountRuntimeBlocker(blocker)
			svc.HandleUpstreamError(context.Background(), account, 429, nil, []byte(upstreamQuotaErrorBody))
			require.Equal(t, 1, repo.atomicCalls)
			require.Zero(t, repo.rateLimitedCalls)
			require.Empty(t, blocker.until)
		})
	}
}

func TestUpstreamQuotaDirectErrorWriters(t *testing.T) {
	for _, protocol := range []string{"anthropic", "responses", "compat_chat", "compat_anthropic"} {
		for _, quota := range []bool{false, true} {
			t.Run(protocol+"/quota="+strconv.FormatBool(quota), func(t *testing.T) {
				c, rec := newOpenAIUpstreamErrorTestContext(t)
				body := `{"error":{"type":"rate_limit_error","message":"insufficient_quota mentioned in a message"}}`
				if quota {
					body = upstreamQuotaErrorBody
				}
				resp := newOpenAIUpstreamErrorResponse(http.StatusTooManyRequests, body)
				account := &Account{ID: 123, Type: AccountTypeAPIKey, Platform: PlatformOpenAI}
				svc := &OpenAIGatewayService{cfg: &config.Config{}}
				var err error
				switch protocol {
				case "anthropic":
					account.Platform = PlatformAnthropic
					_, err = (&GatewayService{cfg: &config.Config{}}).handleErrorResponse(context.Background(), resp, c, account)
				case "responses":
					_, err = svc.handleErrorResponse(context.Background(), resp, c, account, nil)
				case "compat_chat":
					_, err = svc.handleCompatErrorResponse(resp, c, account, writeChatCompletionsError)
				case "compat_anthropic":
					_, err = svc.handleCompatErrorResponse(resp, c, account, writeAnthropicError)
				}
				require.Error(t, err)
				require.Equal(t, http.StatusTooManyRequests, rec.Code)
				require.Equal(t, "rate_limit_error", gjson.Get(rec.Body.String(), "error.type").String())
				if quota {
					require.Equal(t, UpstreamQuotaExhaustedMessage, gjson.Get(rec.Body.String(), "error.message").String())
					if protocol == "anthropic" || protocol == "responses" {
						require.Equal(t, "insufficient_quota", gjson.Get(rec.Body.String(), "error.code").String())
					}
				} else {
					require.NotEqual(t, UpstreamQuotaExhaustedMessage, gjson.Get(rec.Body.String(), "error.message").String())
					require.False(t, gjson.Get(rec.Body.String(), "error.code").Exists())
				}
			})
		}
	}
}

func TestUpstreamQuotaDirectWritersKeepPassthroughPriority(t *testing.T) {
	for _, anthropic := range []bool{false, true} {
		t.Run(strconv.FormatBool(anthropic), func(t *testing.T) {
			c, rec := newOpenAIUpstreamErrorTestContext(t)
			rules := &ErrorPassthroughService{}
			rules.setLocalCache([]*model.ErrorPassthroughRule{
				newNonFailoverPassthroughRule(429, "insufficient_quota", 409, "Configured quota response"),
			})
			BindErrorPassthroughService(c, rules)
			resp := newOpenAIUpstreamErrorResponse(429, upstreamQuotaErrorBody)
			account := &Account{ID: 123, Type: AccountTypeAPIKey, Platform: PlatformOpenAI}
			if anthropic {
				account.Platform = PlatformAnthropic
				_, _ = (&GatewayService{cfg: &config.Config{}}).handleErrorResponse(context.Background(), resp, c, account)
			} else {
				_, _ = (&OpenAIGatewayService{cfg: &config.Config{}}).handleErrorResponse(context.Background(), resp, c, account, nil)
			}
			require.Equal(t, 409, rec.Code)
			require.Equal(t, "Configured quota response", gjson.Get(rec.Body.String(), "error.message").String())
			require.False(t, gjson.Get(rec.Body.String(), "error.code").Exists())
		})
	}
}
