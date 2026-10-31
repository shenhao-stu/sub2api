package service

import (
	"context"
	"errors"
	"fmt"
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

func TestContentRefusalAnthropicForwardStopsWithoutLosingUsage(t *testing.T) {
	for _, contract := range []struct {
		name, fields, kind string
		passthrough        bool
	}{
		{"provider", `"type":"content_refusal"`, "content_refusal", true},
		{"protocol_passthrough", `"type":"permission_error","code":"policy_blocked"`, "permission_error", true},
		{"protocol_standard", `"type":"permission_error","code":"policy_blocked"`, "permission_error", false},
	} {
		t.Run(contract.name, func(t *testing.T) {
			for _, measured := range []bool{false, true} {
				for _, committed := range []bool{false, true} {
					t.Run(strings.Join([]string{map[bool]string{true: "metered", false: "unmetered"}[measured], map[bool]string{true: "sse", false: "json"}[committed]}, "/"), func(t *testing.T) {
						rec := httptest.NewRecorder()
						c, _ := gin.CreateTestContext(rec)
						c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
						if committed {
							c.Header("Content-Type", "text/event-stream")
							_, _ = c.Writer.WriteString(": ping\n\n")
							c.Writer.Flush()
						}
						body := `{"type":"error","error":{` + contract.fields + `,"message":"This conversation is blocked by provider policy. See https://api.derouter.ai/v1/help?token=private or //derouter.ai/help or support.derouter.ai/help","request_id":"private-provider-id"}`
						if measured {
							body += `,"usage":{"input_tokens":12,"output_tokens":7,"cache_read_input_tokens":5,"cache_creation":{"ephemeral_1h_input_tokens":3}}`
						}
						body += "}"
						up := &queuedHTTPUpstreamStub{responses: []*http.Response{{StatusCode: 403, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}}}
						s := newForwardPartialUsageServiceForTest(nil)
						s.httpUpstream = up
						s.cfg.Gateway.LogUpstreamErrorBody = true
						s.cfg.Gateway.LogUpstreamErrorBodyMaxBytes = 2048
						a := newAnthropicAPIKeyAccountForTest()
						a.Credentials["pool_mode"] = true
						a.Extra["anthropic_passthrough"] = contract.passthrough
						parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(fmt.Sprintf(`{"model":"claude-sonnet-5-5","stream":%t,"messages":[{"role":"user","content":"synthetic"}]}`, committed))), PlatformAnthropic)
						require.NoError(t, err)
						result, err := s.Forward(context.Background(), c, a, parsed)
						require.Error(t, err)
						var failover *UpstreamFailoverError
						require.False(t, errors.As(err, &failover), "policy denial must not switch or retry accounts")
						require.Equal(t, 1, up.callCount, "policy denial must not be dispatched again")
						if measured {
							require.NotNil(t, result)
							require.Equal(t, 12, result.Usage.InputTokens)
							require.Equal(t, 7, result.Usage.OutputTokens)
							require.Equal(t, 5, result.Usage.CacheReadInputTokens)
							require.Equal(t, 3, result.Usage.CacheCreation1hTokens)
							require.Contains(t, rec.Body.String(), `"ephemeral_1h_input_tokens":3`)
						} else {
							require.Nil(t, result)
						}
						want := 403
						if committed {
							want = 200
							require.Equal(t, 1, strings.Count(rec.Body.String(), "event: error"))
						}
						require.Equal(t, want, rec.Code)
						require.Contains(t, rec.Body.String(), fmt.Sprintf(`"type":%q`, contract.kind))
						if strings.HasPrefix(contract.name, "protocol_") {
							require.Contains(t, rec.Body.String(), `"code":"policy_blocked"`)
						}
						require.Contains(t, rec.Body.String(), `"status_code":403`)
						require.NotContains(t, rec.Body.String(), "private-provider-id")
						require.NotContains(t, strings.ToLower(rec.Body.String()), "derouter.ai")
						require.NotContains(t, rec.Body.String(), "token=private")
						privateEvents, ok := c.Get(OpsUpstreamErrorsKey)
						require.True(t, ok)
						events := privateEvents.([]*OpsUpstreamErrorEvent)
						require.Len(t, events, 1)
						require.Equal(t, "private-provider-id", events[0].UpstreamRequestID)
						require.Contains(t, events[0].Detail, "private-provider-id")
					})
				}
			}
		})
	}
}

func TestContentRefusalDoesNotDisableAuthenticationRecovery(t *testing.T) {
	for _, body := range []string{`{"error":{"type":"permission_error"}}`, `{"error":{"message":"content_refusal"}}`, `{"error":{"type":"content_refusal"}`, `{"error":{"type":"content_refusal"}} trailing`} {
		require.False(t, isContentRefusal(403, []byte(body)))
	}
	require.False(t, isContentRefusal(502, []byte(`{"error":{"type":"content_refusal"}}`)))
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
	body := `{"error":{"type":"permission_error","message":"credentials invalid"}}`
	resp := &http.Response{StatusCode: 403, Body: io.NopCloser(strings.NewReader(body))}
	s := &GatewayService{}
	result, err, handled := s.handleContentRefusalResponse(c, newAnthropicAPIKeyAccountForTest(), resp, "model", "model", false, time.Now())
	require.False(t, handled)
	require.NoError(t, err)
	require.Nil(t, result)
	require.False(t, c.Writer.Written())
	remaining, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, body, string(remaining))
}

func TestProtocolPolicyRequiresExact403Code(t *testing.T) {
	body := []byte(`{"error":{"type":"permission_error","code":"policy_blocked"}}`)
	require.True(t, isContentRefusal(403, body))
	for _, status := range []int{400, 401, 429, 500, 502, 503} {
		require.False(t, isContentRefusal(status, body))
	}
	for _, body := range []string{
		`{"error":{"type":"policy_blocked"}}`,
		`{"error":{"type":"permission_error","message":"policy_blocked"}}`,
		`{"error":{"type":"permission_error","message":"请求被协议拦截"}}`,
		`{"error":{"code":403}}`,
		`{"code":"policy_blocked"}`,
		`{"error":{"code":"policy_blocked"}} trailing`,
	} {
		require.False(t, isContentRefusal(403, []byte(body)))
	}
}

func TestUnknownAnthropic403StillRetries(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
	up := &queuedHTTPUpstreamStub{responses: []*http.Response{
		{StatusCode: 403, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"error":{"type":"permission_error","message":"credentials temporarily rejected"}}`))},
		{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"id":"synthetic","type":"message","role":"assistant","model":"claude-sonnet-5-5","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":2}}`))},
	}}
	s := &GatewayService{cfg: &config.Config{}, httpUpstream: up}
	a := newAnthropicAPIKeyAccountForTest()
	a.Credentials["custom_error_codes_enabled"] = true
	a.Credentials["custom_error_codes"] = []any{float64(429)}
	result, err := s.forwardAnthropicAPIKeyPassthrough(context.Background(), c, a, []byte(`{"model":"claude-sonnet-5-5","messages":[{"role":"user","content":"synthetic"}]}`), "claude-sonnet-5-5", "claude-sonnet-5-5", false, time.Now())
	require.NoError(t, err)
	require.Equal(t, 2, up.callCount)
	require.Equal(t, 200, rec.Code)
	require.NotNil(t, result)
	require.Equal(t, 10, result.Usage.InputTokens)
	require.Equal(t, 2, result.Usage.OutputTokens)
}

func TestProtocolPolicySSEStopsWithoutLosingUsage(t *testing.T) {
	for _, phase := range []string{"none", "before", "error", "both", "zeros", "decrease", "reclassify", "output_only"} {
		t.Run(phase, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(`{"model":"claude-sonnet-5-5","stream":true,"messages":[{"role":"user","content":"synthetic"}]}`)), PlatformAnthropic)
			require.NoError(t, err)
			body := ""
			prior := phase != "none" && phase != "error"
			if prior {
				body = "event: message_start\ndata: " + `{"type":"message_start","message":{"usage":{"input_tokens":12,"cache_read_input_tokens":5,"cache_creation":{"ephemeral_1h_input_tokens":3}}}}` + "\n\n"
				body += "event: message_delta\ndata: " + `{"type":"message_delta","usage":{"output_tokens":4}}` + "\n\n"
			}
			errorBody := `{"type":"error","error":{"type":"permission_error","code":"policy_blocked","message":"request rejected https://private-origin.invalid/help?token=secret"}`
			if phase == "error" || phase == "both" {
				errorBody += `,"usage":{"input_tokens":12,"output_tokens":7,"cache_read_input_tokens":5,"cache_creation":{"ephemeral_1h_input_tokens":3}}`
			} else if phase == "zeros" {
				errorBody += `,"usage":{"input_tokens":0,"output_tokens":0,"cache_read_input_tokens":0,"cache_creation_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0}}`
			} else if phase == "decrease" {
				errorBody += `,"usage":{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":1,"cache_creation":{"ephemeral_1h_input_tokens":1}}`
			} else if phase == "reclassify" {
				errorBody += `,"usage":{"input_tokens":0,"output_tokens":7,"cache_read_input_tokens":17,"cache_creation_input_tokens":3,"cache_creation":{"ephemeral_5m_input_tokens":3,"ephemeral_1h_input_tokens":0}}`
			} else if phase == "output_only" {
				errorBody += `,"usage":{"output_tokens":7}`
			}
			body += "event: error\ndata: " + errorBody + "}\n\n"
			up := &queuedHTTPUpstreamStub{responses: []*http.Response{{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(body))}}}
			s := newForwardPartialUsageServiceForTest(nil)
			s.httpUpstream = up
			a := newAnthropicAPIKeyAccountForTest()
			a.Extra["anthropic_passthrough"] = false
			result, err := s.Forward(context.Background(), c, a, parsed)
			require.Error(t, err)
			var failover *UpstreamFailoverError
			require.False(t, errors.As(err, &failover), "structured policy SSE must not fail over")
			require.Equal(t, 1, up.callCount)
			if phase == "none" {
				require.Nil(t, result)
			} else {
				require.NotNil(t, result)
				require.Equal(t, 12, result.Usage.InputTokens)
				require.Equal(t, 5, result.Usage.CacheReadInputTokens)
				require.Equal(t, 3, result.Usage.CacheCreation1hTokens)
				require.Zero(t, result.Usage.CacheCreation5mTokens)
				output := 7
				if phase == "before" || phase == "zeros" || phase == "decrease" {
					output = 4
				}
				require.Equal(t, output, result.Usage.OutputTokens)
			}
			require.Contains(t, rec.Body.String(), `"code":"policy_blocked"`)
			require.Contains(t, rec.Body.String(), `"type":"permission_error"`)
			require.Contains(t, rec.Body.String(), `"status_code":403`)
			require.NotContains(t, rec.Body.String(), "private-origin.invalid")
			require.NotContains(t, rec.Body.String(), "token=secret")
			if prior {
				require.Equal(t, 200, rec.Code)
				require.Equal(t, 1, strings.Count(rec.Body.String(), "event: error"))
			} else {
				require.Equal(t, 403, rec.Code)
			}
			events := c.MustGet(OpsUpstreamErrorsKey).([]*OpsUpstreamErrorEvent)
			require.Len(t, events, 1)
			require.Equal(t, 403, events[0].UpstreamStatusCode)
			require.Equal(t, "request_rejection", events[0].Kind)
		})
	}
}
