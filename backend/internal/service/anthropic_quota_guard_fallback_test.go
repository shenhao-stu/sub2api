//go:build unit

package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

const predispatchQuotaFixture = `{"error":{"type":"rate_limit_error","code":"quota_guard_request_too_large","message":"synthetic quota budget","details":{"reason":"quota_guard_request_too_large","attempt_count":0}}}`

func TestPredispatchQuotaGuardSelectsAnotherAccountWithoutCooldown(t *testing.T) {
	for _, pool := range []bool{false, true} {
		t.Run(map[bool]string{false: "single_slot", true: "pool"}[pool], func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
			upstream := &anthropicHTTPUpstreamRecorder{resp: &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"5"}}, Body: io.NopCloser(strings.NewReader(predispatchQuotaFixture))}}
			repo := &rateLimitAccountRepoStub{}
			svc := &GatewayService{cfg: &config.Config{}, httpUpstream: upstream, rateLimitService: NewRateLimitService(repo, nil, &config.Config{}, nil, nil)}
			account := newAnthropicAPIKeyAccountForTest()
			account.Credentials["pool_mode"] = pool
			body := []byte(`{"model":"claude-opus-5-5","max_tokens":64,"messages":[{"role":"user","content":"synthetic"}]}`)
			result, err := svc.forwardAnthropicAPIKeyPassthrough(t.Context(), c, account, body, "claude-opus-5-5", "claude-opus-5-5", false, time.Now())
			var failure *UpstreamFailoverError
			require.ErrorAs(t, err, &failure)
			require.Nil(t, result)
			require.Zero(t, repo.rateLimitedCalls, "one rejected request must not cool the account")
			require.True(t, failure.ExcludeFromRetry, "same request must not revisit its rejected account")
			require.False(t, failure.RetryableOnSameAccount)
			require.True(t, failure.ShouldRetryNextAccount())
			require.False(t, c.Writer.Written())
			require.Zero(t, repo.tempCalls)
		})
	}
}

func TestAvailabilityCooldownRespectsDedicatedVisibility(t *testing.T) {
	until := time.Now().Add(20 * time.Minute)
	accounts := []Account{
		{ID: 1, Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Extra: map[string]any{"affinity_tags": []string{"getoken:1"}}, RateLimitResetAt: &until},
		{ID: 2, Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true},
		{ID: 3, Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Extra: map[string]any{"affinity_tags": []string{"getoken:2"}}},
	}
	setAccountModelRateLimitSnapshot(&accounts[1], "claude-opus-5-5", until, "native_consumed_model_rate_limit", time.Now())
	repo := &mockAccountRepoForPlatform{accounts: accounts}
	svc := &GatewayService{accountRepo: repo, cfg: testConfig()}
	ctx := WithAccountAffinityTags(context.Background(), []string{"getoken:1"})
	got := svc.DiagnoseModelAvailabilityForPlatform(ctx, nil, "claude-opus-5-5", PlatformAnthropic)
	require.True(t, got.HasModelSupport)
	require.InDelta(t, 1200, got.RetryAfterSeconds, 1, "another tenant's healthy dedicated account cannot hide this caller's cooldown")
	other := WithAccountAffinityTags(context.Background(), []string{"getoken:2"})
	require.Zero(t, svc.DiagnoseModelAvailabilityForPlatform(other, nil, "claude-opus-5-5", PlatformAnthropic).RetryAfterSeconds)
}

func TestQuotaAdmissionWaitExcludedGuardAndTenantScope(t *testing.T) {
	reset := time.Now().Add(20 * time.Minute)
	accounts := []Account{
		{ID: 1, Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true},
		{ID: 2, Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, RateLimitResetAt: &reset},
		{ID: 3, Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Extra: map[string]any{"affinity_tags": []string{"customer:2"}}},
	}
	svc := &GatewayService{accountRepo: &mockAccountRepoForPlatform{accounts: accounts}, cfg: testConfig()}
	ctx := WithAccountAffinityTags(t.Context(), []string{"customer:1"})
	diagnose := func(excluded map[int64]struct{}) int {
		return svc.DiagnoseModelAvailabilityExcluding(ctx, nil, "claude-opus-5-5", PlatformAnthropic, excluded).RetryAfterSeconds
	}
	require.Zero(t, diagnose(nil))
	require.InDelta(t, 1200, diagnose(map[int64]struct{}{1: {}}), 1)
	require.Zero(t, diagnose(map[int64]struct{}{1: {}, 2: {}}))
}

func TestQuotaAdmissionWaitTransportAttemptEvidence(t *testing.T) {
	for _, attempt := range []int{1, 2} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
		resp := &http.Response{StatusCode: 429, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(predispatchQuotaFixture))}
		failure := (&GatewayService{}).anthropicPoolWindowRejection(c, newAnthropicAPIKeyAccountForTest(), resp, attempt)
		require.NotNil(t, failure)
		require.Equal(t, attempt == 1, failure.FirstAttemptQuotaGuard)
	}
}
