package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestAnthropicUpstreamAdmissionVariants(t *testing.T) {
	const newSession = "请开新会话再使用."
	const clientOnly = "No available accounts: this group only allows Claude Code clients"
	for _, tc := range []struct {
		name, kind, message string
		status, want        int
		code                string
	}{
		{"new session", "upstream_error", newSession, 429, 400, "upstream_session_not_accepted"},
		{"Claude Code client required", "api_error", clientOnly, 503, 403, "claude_code_only"},
		{"account limit", "upstream_error", "Too many requests. Please try again later.", 429, 0, ""},
		{"upstream down", "api_error", "No available accounts", 503, 0, ""},
		{"different status", "api_error", clientOnly, 429, 0, ""},
		{"different type", "upstream_error", clientOnly, 503, 0, ""},
		{"conflicting status", "upstream_error", newSession, 503, 0, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
			body, err := json.Marshal(gin.H{"type": "error", "error": gin.H{"type": tc.kind, "message": tc.message}})
			require.NoError(t, err)
			res := &http.Response{StatusCode: tc.status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(string(body)))}
			svc := newForwardPartialUsageServiceForTest(nil)
			result, err, handled := svc.handleSessionAdmissionResponse(c, newAnthropicAPIKeyAccountForTest(), res, "test", "test", false, time.Now())
			require.Equal(t, tc.want != 0, handled)
			require.Nil(t, result)
			if !handled {
				require.NoError(t, err)
				rest, err := io.ReadAll(res.Body)
				require.NoError(t, err)
				require.Equal(t, body, rest)
				return
			}
			require.Error(t, err)
			require.Equal(t, tc.want, rec.Code)
			require.Contains(t, rec.Body.String(), tc.code)
			events, _ := c.Get(OpsUpstreamErrorsKey)
			require.Equal(t, tc.status, events.([]*OpsUpstreamErrorEvent)[0].UpstreamStatusCode)
		})
	}
}

func TestAnthropicAdmissionPrecedesPoolRetry(t *testing.T) {
	for _, tc := range []struct {
		status     int
		body, code string
	}{
		{429, `{"type":"error","error":{"type":"upstream_error","message":"请开新会话再使用."},"usage":{"input_tokens":9,"output_tokens":3}}`, "upstream_session_not_accepted"},
		{503, `{"type":"error","error":{"type":"api_error","message":"No available accounts: this group only allows Claude Code clients"},"usage":{"input_tokens":9,"output_tokens":3}}`, "claude_code_only"},
	} {
		for _, passthrough := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/passthrough=%t", tc.status, passthrough), func(t *testing.T) {
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
				c.Header("Content-Type", "text/event-stream")
				_, _ = c.Writer.WriteString(": ping\n\n")
				c.Writer.Flush()
				up := &queuedHTTPUpstreamStub{responses: []*http.Response{{
					StatusCode: tc.status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(tc.body)),
				}}}
				svc := newForwardPartialUsageServiceForTest(nil)
				svc.httpUpstream = up
				account := newAnthropicAPIKeyAccountForTest()
				account.Credentials["pool_mode"] = true
				account.Extra["anthropic_passthrough"] = passthrough
				parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(`{"model":"claude-opus-5-5","stream":true,"messages":[{"role":"user","content":"synthetic"}]}`)), PlatformAnthropic)
				require.NoError(t, err)
				result, err := svc.Forward(context.Background(), c, account, parsed)
				require.Error(t, err)
				require.NotErrorAs(t, err, new(*UpstreamFailoverError))
				require.Equal(t, 1, up.callCount)
				require.Equal(t, 1, strings.Count(rec.Body.String(), "event: error"))
				require.Contains(t, rec.Body.String(), tc.code)
				require.NotNil(t, result)
				require.Equal(t, 9, result.Usage.InputTokens)
				require.Equal(t, 3, result.Usage.OutputTokens)
			})
		}
	}
}
