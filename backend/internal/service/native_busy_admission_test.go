package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type nativeBusyRateLimitRepo struct {
	gatewayForwardErrorPolicyRepoStub
	cooldowns int
}

func (r *nativeBusyRateLimitRepo) SetRateLimited(context.Context, int64, time.Time) error {
	r.cooldowns++
	return nil
}

func TestNativeBusyAdmissionDoesNotRetryOrCoolAccount(t *testing.T) {
	const session = `{"error":{"code":"upstream_rate_limit","details":{"retry_after":null},"message":"session_busy","type":"rate_limit_error"}}`
	const capacity = `{"error":{"code":"upstream_rate_limit","details":{"retry_after":null,"upstream_status":429,"upstream_type":"api_error"},"message":"native_session_capacity","type":"rate_limit_error"}}`
	const ingress = `{"type":"error","error":{"type":"overloaded_error","code":"native_ingress_busy","message":"Relay capacity is busy; retry later."}}`
	for _, tc := range []struct {
		name, body, code string
		passthrough      bool
	}{
		{"native worker capacity", capacity, "native_session_capacity", true},
		{"native worker capacity non-passthrough", capacity, "native_session_capacity", false},
		{"native memory pressure", strings.Replace(capacity, "native_session_capacity", "native_memory_pressure", 1), "native_memory_pressure", true},
		{"upstream quota", strings.Replace(capacity, "native_session_capacity", "native_rate_limit", 1), "", true},
		{"provider type conflicts", strings.Replace(capacity, "api_error", "rate_limit_error", 1), "", true},
		{"provider status conflicts", strings.Replace(capacity, "429", "503", 1), "", true},
		{"provider reset present", strings.Replace(capacity, "null", "300", 1), "", true},
		{"session", session, "conversation_rate_limited", true},
		{"ingress", ingress, "native_ingress_busy", true},
		{"ingress non-passthrough", ingress, "native_ingress_busy", false},
		{"real provider 429", `{"error":{"code":"upstream_rate_limit","message":"Provider is at its rate limit","type":"rate_limit_error"}}`, "", true},
		{"contradictory embedded status", strings.Replace(ingress, `"error":`, `"status_code":503,"error":`, 1), "", true},
		{"conflicting error code", strings.Replace(session, "upstream_rate_limit", "provider_rate_limit", 1), "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			up := &queuedHTTPUpstreamStub{responses: []*http.Response{{
				StatusCode: 429,
				Header:     http.Header{"Retry-After": {"1"}},
				Body:       io.NopCloser(strings.NewReader(tc.body)),
			}}}
			s := newForwardPartialUsageServiceForTest(nil)
			s.httpUpstream = up
			repo := &nativeBusyRateLimitRepo{}
			s.rateLimitService = NewRateLimitService(repo, nil, s.cfg, nil, nil)
			account := newAnthropicAPIKeyAccountForTest()
			account.Extra["anthropic_passthrough"] = tc.passthrough
			parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(`{"model":"claude-opus-5-5","messages":[{"role":"user","content":"synthetic"}]}`)), PlatformAnthropic)
			require.NoError(t, err)
			result, err := s.Forward(context.Background(), c, account, parsed)
			require.Error(t, err)
			require.Nil(t, result, "pre-dispatch rejection has no inferred usage")
			require.Equal(t, 1, up.callCount)
			var failover *UpstreamFailoverError
			if tc.code == "" {
				require.ErrorAs(t, err, &failover)
				require.Equal(t, 1, repo.cooldowns)
				return
			}
			require.False(t, errors.As(err, &failover))
			require.Zero(t, repo.cooldowns)
			require.Equal(t, 429, rec.Code)
			require.Contains(t, rec.Body.String(), `"code":"`+tc.code+`"`)
			require.Equal(t, "1", rec.Header().Get("Retry-After"))
		})
	}
}
