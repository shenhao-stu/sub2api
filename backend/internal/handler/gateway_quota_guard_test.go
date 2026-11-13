package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/model"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestAnthropicQuotaGuardKeepsCodeThroughPassthroughRule(t *testing.T) {
	const upstream = `{"type":"error","error":{"type":"rate_limit_error","code":"quota_guard_request_too_large","message":"provider private message","details":{"reason":"quota_guard_request_too_large","attempt_count":0}}}`
	public := "The request is too large for the available upstream quota."
	repo := &errorPassthroughRepoStub{rules: []*model.ErrorPassthroughRule{{
		Name: "generic 429", Enabled: true, ErrorCodes: []int{429},
		Platforms: []string{model.PlatformAnthropic}, MatchMode: model.MatchModeAll,
		PassthroughCode: true, CustomMessage: &public,
	}}}
	h := &GatewayHandler{errorPassthroughService: service.NewErrorPassthroughService(repo, nil)}
	for _, tc := range []struct {
		name string
		body string
		want bool
	}{
		{"predispatch", upstream, true},
		{"already dispatched", `{"type":"error","error":{"type":"rate_limit_error","code":"quota_guard_request_too_large","details":{"reason":"quota_guard_request_too_large","attempt_count":1}}}`, false},
		{"missing dispatch evidence", `{"type":"error","error":{"type":"rate_limit_error","code":"quota_guard_request_too_large"}}`, false},
		{"duplicate evidence", `{"type":"error","error":{"type":"rate_limit_error","code":"quota_guard_request_too_large","details":{"reason":"quota_guard_request_too_large","attempt_count":1,"attempt_count":0}}}`, false},
		{"conflicting status", `{"type":"error","error":{"type":"rate_limit_error","code":"quota_guard_request_too_large","status_code":503,"details":{"reason":"quota_guard_request_too_large","attempt_count":0}}}`, false},
		{"generic rate limit", `{"type":"error","error":{"type":"rate_limit_error","code":"upstream_rate_limit"}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, stream := range []bool{false, true} {
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
				if stream {
					c.Header("Content-Type", "text/event-stream")
					c.Writer.Flush()
				}
				h.handleFailoverExhausted(c, &service.UpstreamFailoverError{StatusCode: 429, ResponseBody: []byte(tc.body)}, service.PlatformAnthropic, stream)
				if stream {
					require.Equal(t, http.StatusOK, rec.Code)
					require.Contains(t, rec.Body.String(), `"status_code":429`)
				} else {
					require.Equal(t, http.StatusTooManyRequests, rec.Code)
				}
				if tc.want {
					require.Contains(t, rec.Body.String(), `"type":"rate_limit_error"`)
					require.Contains(t, rec.Body.String(), public)
					require.Contains(t, rec.Body.String(), `"code":"quota_guard_request_too_large"`)
				} else {
					require.NotContains(t, rec.Body.String(), `"code":"quota_guard_request_too_large"`)
				}
				require.NotContains(t, rec.Body.String(), "provider private message")
			}
		})
	}
}
