//go:build unit

package handler

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

const admissionGuardFixture = `{"error":{"type":"rate_limit_error","code":"quota_guard_request_too_large","details":{"reason":"quota_guard_request_too_large","attempt_count":0}}}`

func TestQuotaAdmissionWaitProof(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		status                 int
		body, reason, platform string
		want                   bool
	}{
		{"before dispatch", 429, admissionGuardFixture, service.AnthropicQuotaGuardCode, service.PlatformAnthropic, true},
		{"upstream already attempted", 429, strings.Replace(admissionGuardFixture, `"attempt_count":0`, `"attempt_count":1`, 1), service.AnthropicQuotaGuardCode, service.PlatformAnthropic, false},
		{"unclassified 429", 429, admissionGuardFixture, "", service.PlatformAnthropic, false},
		{"wrong platform", 429, admissionGuardFixture, service.AnthropicQuotaGuardCode, service.PlatformOpenAI, false},
		{"consumed", 429, `{"error":{"code":"native_request_consumed_no_replay"}}`, service.AnthropicQuotaGuardCode, service.PlatformAnthropic, false},
		{"server fault", 500, admissionGuardFixture, service.AnthropicQuotaGuardCode, service.PlatformAnthropic, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &service.UpstreamFailoverError{FirstAttemptQuotaGuard: true, StatusCode: tc.status, ResponseBody: []byte(tc.body), Reason: service.GatewayFailureReason(tc.reason)}
			require.Equal(t, tc.want, isUndispatchedQuotaGuard(tc.platform, f))
			f.FirstAttemptQuotaGuard = false
			require.False(t, isUndispatchedQuotaGuard(tc.platform, f))
		})
	}
}

func TestQuotaAdmissionWaitScope(t *testing.T) {
	q := newQuotaAdmissionWait("customer:1")
	ctx := service.WithAccountAffinityTags(t.Context(), []string{"customer:1"})
	require.True(t, q.matches(ctx))
	require.False(t, q.matches(t.Context()))
	require.False(t, q.matches(service.WithAccountAffinityTags(t.Context(), []string{"customer:2"})))
	require.False(t, newQuotaAdmissionWait("").matches(ctx))
	require.False(t, q.canWait(0))
	require.False(t, q.canWait(2701))
	require.True(t, q.canWait(2700))
}

func TestQuotaAdmissionWaitTransport(t *testing.T) {
	for _, tc := range []string{"reset", "cancel", "full"} {
		t.Run(tc, func(t *testing.T) {
			q := newQuotaAdmissionWait("customer:1")
			q.heartbeat = time.Millisecond
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			c.Request = httptest.NewRequest("POST", "/v1/messages", nil).WithContext(ctx)
			wait := 25 * time.Millisecond
			if tc == "cancel" {
				wait = time.Hour
				timer := time.AfterFunc(10*time.Millisecond, cancel)
				defer timer.Stop()
			}
			if tc == "full" {
				for range 2 {
					_, ok := q.reserve("fixture", 1)
					require.True(t, ok)
				}
			}
			started := false
			waited, err := q.wait(c, wait, &started, "fixture", 1)
			require.Equal(t, tc == "reset", waited)
			if tc == "cancel" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}
			if tc == "full" {
				require.False(t, started)
				require.False(t, c.Writer.Written())
				return
			}
			if tc == "reset" {
				require.True(t, started)
			}
			if started {
				require.True(t, gatewayStreamHasOnlyHeartbeats(c))
				require.Equal(t, "text/event-stream", rec.Header().Get("Content-Type"))
				require.Empty(t, strings.ReplaceAll(rec.Body.String(), string(SSEPingFormatClaude), ""))
			}
			require.Empty(t, q.tenants)
			require.Zero(t, q.count)
			require.Zero(t, q.bytes)
		})
	}
}

func TestQuotaAdmissionWaitGeneralTenantLimits(t *testing.T) {
	q := newQuotaAdmissionWait("*")
	direct := t.Context()
	require.True(t, q.matches(direct))
	tagged := service.WithAccountAffinityTags(direct, []string{"customer:1"})
	require.NotEqual(t, waitTenant(direct, 1), waitTenant(tagged, 1))
	require.NotEqual(t, waitTenant(direct, 1), waitTenant(direct, 2))
	require.Equal(t, waitTenant(tagged, 1), waitTenant(tagged, 2))
	a, ok := q.reserve("a", 16<<20)
	require.True(t, ok)
	b, ok := q.reserve("a", 1)
	require.True(t, ok)
	_, ok = q.reserve("a", 1)
	require.False(t, ok)
	_, ok = q.reserve("b", 16<<20)
	require.False(t, ok)
	c, ok := q.reserve("b", 1)
	require.True(t, ok)
	d, ok := q.reserve("c", 1)
	require.True(t, ok)
	_, ok = q.reserve("d", 1)
	require.False(t, ok)
	a()
	b()
	c()
	d()
	require.Zero(t, q.bytes)
	require.Empty(t, q.tenants)
}
