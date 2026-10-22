//go:build unit

package service

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

// retryAfterAccountRepoStub 同时实现 SetRateLimited 与 SetRateLimitedIfLater，
// 用于验证 Retry-After 路径优先走“只延长不缩短”的写入。
type retryAfterAccountRepoStub struct {
	rateLimit429AccountRepoStub
	ifLaterCalls int
	ifLaterReset time.Time
}

func (r *retryAfterAccountRepoStub) SetRateLimitedIfLater(_ context.Context, id int64, resetAt time.Time) error {
	r.ifLaterCalls++
	r.lastRateLimitID = id
	r.ifLaterReset = resetAt
	return nil
}

// vm2api 本地额度预检的 429：没有 anthropic-ratelimit-* 头，只有 Retry-After 与说明性 body。
const vm2apiQuota429Body = `{"type":"error","error":{"type":"rate_limit_error","code":"five_hour_seat_budget","message":"5h 新会话额度余量不足","details":{"source":"local_quota_policy","window":"5h"}}}`

func newRetryAfterRateLimitService(t *testing.T, repo AccountRepository, fallbackEnabled bool, fallbackSeconds int) *RateLimitService {
	t.Helper()
	settingRepo := newMockSettingRepo()
	data, _ := json.Marshal(RateLimit429CooldownSettings{Enabled: fallbackEnabled, CooldownSeconds: fallbackSeconds})
	settingRepo.data[SettingKeyRateLimit429CooldownSettings] = string(data)
	svc := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
	svc.SetSettingService(NewSettingService(settingRepo, &config.Config{}))
	return svc
}

func TestParseRetryAfterDelay(t *testing.T) {
	now := time.Date(2026, 10, 22, 8, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		name  string
		value string
		want  time.Duration
		ok    bool
	}{
		{"missing", "", 0, false},
		{"seconds", "1800", 30 * time.Minute, true},
		{"zero", "0", 0, true},
		{"seven days", "604800", 7 * 24 * time.Hour, true},
		{"over seven days", "604801", 0, false},
		{"http date", now.Add(2 * time.Hour).Format(http.TimeFormat), 2 * time.Hour, true},
		{"past date", now.Add(-time.Minute).Format(http.TimeFormat), -time.Minute, true},
		{"garbage", "soon", 0, false},
		{"negative", "-5", 0, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := http.Header{}
			if tt.value != "" {
				h.Set("Retry-After", tt.value)
			}
			got, ok := parseRetryAfterDelay(now, h)
			require.Equal(t, tt.ok, ok)
			if ok {
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestHandle429_AnthropicRetryAfterExtendsCooldown(t *testing.T) {
	repo := &retryAfterAccountRepoStub{}
	svc := newRetryAfterRateLimitService(t, repo, true, 5)

	account := &Account{ID: 33850, Platform: PlatformAnthropic, Type: AccountTypeAPIKey}
	headers := http.Header{}
	headers.Set("Retry-After", "1800")
	before := time.Now()
	svc.handle429(context.Background(), account, headers, []byte(vm2apiQuota429Body))
	after := time.Now()

	require.Equal(t, 1, repo.ifLaterCalls, "必须走 SetRateLimitedIfLater")
	require.Zero(t, repo.rateLimitCalls, "不应再走无条件 SetRateLimited")
	require.Equal(t, int64(33850), repo.lastRateLimitID)
	require.False(t, repo.ifLaterReset.Before(before.Add(30*time.Minute)))
	require.False(t, repo.ifLaterReset.After(after.Add(30*time.Minute)))
}

func TestHandle429_AnthropicRetryAfterHonoursFallbackFloor(t *testing.T) {
	repo := &retryAfterAccountRepoStub{}
	svc := newRetryAfterRateLimitService(t, repo, true, 12)

	account := &Account{ID: 1, Platform: PlatformAnthropic, Type: AccountTypeAPIKey}
	headers := http.Header{}
	headers.Set("Retry-After", "1")
	before := time.Now()
	svc.handle429(context.Background(), account, headers, []byte(vm2apiQuota429Body))

	require.Equal(t, 1, repo.ifLaterCalls)
	require.False(t, repo.ifLaterReset.Before(before.Add(12*time.Second)), "Retry-After 短于兜底秒数时取兜底秒数")
}

func TestHandle429_AnthropicRetryAfterCapsAtSevenDays(t *testing.T) {
	repo := &retryAfterAccountRepoStub{}
	svc := newRetryAfterRateLimitService(t, repo, true, 5)

	account := &Account{ID: 2, Platform: PlatformAnthropic, Type: AccountTypeAPIKey}
	headers := http.Header{}
	headers.Set("Retry-After", "9999999") // > 7d：不可信，退回兜底
	svc.handle429(context.Background(), account, headers, []byte(vm2apiQuota429Body))

	require.Zero(t, repo.ifLaterCalls)
	require.Equal(t, 1, repo.rateLimitCalls, "超上限的 Retry-After 视为无效，走兜底秒数")
}

func TestHandle429_AnthropicRetryAfterAppliesEvenWhenFallbackDisabled(t *testing.T) {
	// 兜底开关只管“猜”，上游明确给出的 Retry-After 仍应被尊重
	repo := &retryAfterAccountRepoStub{}
	svc := newRetryAfterRateLimitService(t, repo, false, 5)

	account := &Account{ID: 3, Platform: PlatformAnthropic, Type: AccountTypeAPIKey}
	headers := http.Header{}
	headers.Set("Retry-After", "60")
	before := time.Now()
	svc.handle429(context.Background(), account, headers, []byte(vm2apiQuota429Body))

	require.Equal(t, 1, repo.ifLaterCalls)
	require.False(t, repo.ifLaterReset.Before(before.Add(60*time.Second)))
}

func TestHandle429_AnthropicRetryAfterFallsBackToSetRateLimitedWithoutExtender(t *testing.T) {
	repo := &rateLimit429AccountRepoStub{} // 不实现 SetRateLimitedIfLater
	svc := newRetryAfterRateLimitService(t, repo, true, 5)

	account := &Account{ID: 4, Platform: PlatformAnthropic, Type: AccountTypeAPIKey}
	headers := http.Header{}
	headers.Set("Retry-After", "120")
	before := time.Now()
	svc.handle429(context.Background(), account, headers, []byte(vm2apiQuota429Body))

	require.Equal(t, 1, repo.rateLimitCalls)
	require.False(t, repo.lastRateLimitReset.Before(before.Add(120*time.Second)))
}

func TestHandle429_AnthropicNoRetryAfterStillUsesFallback(t *testing.T) {
	repo := &retryAfterAccountRepoStub{}
	svc := newRetryAfterRateLimitService(t, repo, true, 7)

	account := &Account{ID: 5, Platform: PlatformAnthropic, Type: AccountTypeAPIKey}
	before := time.Now()
	svc.handle429(context.Background(), account, http.Header{}, []byte(vm2apiQuota429Body))

	require.Zero(t, repo.ifLaterCalls)
	require.Equal(t, 1, repo.rateLimitCalls)
	require.False(t, repo.lastRateLimitReset.Before(before.Add(7*time.Second)))
}

func TestHandle429_AnthropicUnifiedHeaderStillWinsOverRetryAfter(t *testing.T) {
	// 真正的 Anthropic 5h 窗口头优先级高于 Retry-After（既有行为不变）
	repo := &retryAfterAccountRepoStub{}
	svc := newRetryAfterRateLimitService(t, repo, true, 5)

	account := &Account{ID: 6, Platform: PlatformAnthropic, Type: AccountTypeOAuth}
	resetAt := time.Now().Add(3 * time.Hour).Unix()
	headers := http.Header{}
	headers.Set("anthropic-ratelimit-unified-reset", strconv.FormatInt(resetAt, 10))
	headers.Set("Retry-After", "60")
	svc.handle429(context.Background(), account, headers, []byte(`{"error":{"type":"rate_limit_error"}}`))

	require.Zero(t, repo.ifLaterCalls)
	require.Equal(t, 1, repo.rateLimitCalls)
	require.Equal(t, resetAt, repo.lastRateLimitReset.Unix())
}

func TestUpstreamQuotaResetAt_StillUsesRetryAfterAfterRefactor(t *testing.T) {
	now := time.Date(2026, 10, 22, 8, 0, 0, 0, time.UTC)
	h := http.Header{}
	h.Set("Retry-After", "1800")
	require.Equal(t, now.Add(30*time.Minute), upstreamQuotaResetAt(now, h, 5*time.Second, true))
	h.Set("Retry-After", "1")
	require.Equal(t, now.Add(10*time.Minute), upstreamQuotaResetAt(now, h, 5*time.Second, true))
	require.True(t, upstreamQuotaResetAt(now, http.Header{}, 5*time.Second, false).IsZero())
}
