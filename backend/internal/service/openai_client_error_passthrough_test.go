package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/model"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const invalidToolNameBody = `{"error":{"type":"invalid_request_error","code":"invalid_request","param":"input[535].name","message":"必填字段必须是非空字符串（字段：input[535].name）"}}`

func bindClientErrorPassthroughRule(c *gin.Context) {
	svc := &ErrorPassthroughService{}
	svc.setLocalCache([]*model.ErrorPassthroughRule{{
		ID: 2, Enabled: true, ErrorCodes: []int{400, 422}, Keywords: []string{`"message"`},
		MatchMode: "all", Platforms: []string{PlatformOpenAI}, PassthroughCode: true, PassthroughBody: true,
	}})
	BindErrorPassthroughService(c, svc)
}

func TestOpenAIClientErrorPassthrough_BoundsAndFiltersUntrustedFields(t *testing.T) {
	for _, tc := range []struct {
		name, body, wantMessage string
	}{
		{"non_string_fields", `{"error":{"type":{},"code":9,"param":[],"message":{}}}`, openAIUpstreamClientErrorFallbackMessage},
		{"nested_json_message", `{"error":{"message":"{\"input\":\"private\"}"}}`, openAIUpstreamClientErrorFallbackMessage},
		{"html_message", `{"error":{"message":"<html>private</html>"}}`, openAIUpstreamClientErrorFallbackMessage},
		{"oversize", `{"error":{"message":"` + strings.Repeat("x", 65<<10) + `"}}`, openAIUpstreamClientErrorFallbackMessage},
		{"malformed", `{"error":{"message":"private`, openAIUpstreamClientErrorFallbackMessage},
		{"credential_code", `{"error":{"type":"sk-private-type","code":"sk-private-code","message":"rejected"}}`, "rejected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fields := boundedOpenAIClientError([]byte(tc.body), 400)
			require.Equal(t, tc.wantMessage, fields.Message)
			require.Equal(t, "invalid_request_error", fields.Type)
			require.Empty(t, fields.Code)
			require.Empty(t, fields.Param)
		})
	}
	c, rec := newOpenAIUpstreamErrorTestContext(t)
	bindClientErrorPassthroughRule(c)
	body := []byte(`{"error":{"type":"invalid_request_error","code":"bad code\nsecret","param":"input[0]['private-value']","status_code":200,"message":"name rejected; api_key=PRIVATE cookie=PRIVATE Bearer SECRET sk-secret-value"},"request":"PRIVATE","headers":{"authorization":"PRIVATE"},"usage":{"input_tokens":999},"getoken":{"cost_usd":"0"}}`)
	require.True(t, WriteOpenAIClientErrorPassthrough(c, getBoundErrorPassthroughService(c), 422, body, false))
	response := rec.Body.String()
	require.Equal(t, int64(422), gjson.Get(response, "error.status_code").Int())
	require.False(t, gjson.Get(response, "error.code").Exists())
	require.False(t, gjson.Get(response, "error.param").Exists())
	for _, private := range []string{"PRIVATE", "SECRET", "sk-secret-value", "usage", "getoken", "headers"} {
		require.NotContains(t, response, private)
	}
	largeMessage, err := json.Marshal(map[string]any{"error": map[string]string{"message": strings.Repeat("错", 2000)}})
	require.NoError(t, err)
	fields := boundedOpenAIClientError(largeMessage, 400)
	require.LessOrEqual(t, len(fields.Message), 2048)
	require.True(t, utf8.ValidString(fields.Message))
}

func TestOpenAIClientErrorPassthrough_RuleScopeAndOverrides(t *testing.T) {
	for _, status := range []int{401, 402, 403, 413, 429, 500, 503} {
		c, rec := newOpenAIUpstreamErrorTestContext(t)
		bindClientErrorPassthroughRule(c)
		require.False(t, WriteOpenAIClientErrorPassthrough(c, getBoundErrorPassthroughService(c), status, []byte(invalidToolNameBody), false))
		require.Empty(t, rec.Body.String())
		require.False(t, c.Writer.Written())
	}
	c, rec := newOpenAIUpstreamErrorTestContext(t)
	bindClientErrorPassthroughRule(c)
	rules := getBoundErrorPassthroughService(c)
	responseStatus := http.StatusTeapot
	rule := &model.ErrorPassthroughRule{
		ID: 2, Enabled: true, ErrorCodes: []int{400}, PassthroughBody: true,
		PassthroughCode: false, ResponseCode: &responseStatus, SkipMonitoring: true,
	}
	rules.setLocalCache([]*model.ErrorPassthroughRule{rule})
	require.True(t, WriteOpenAIClientErrorPassthrough(c, rules, 400, []byte(invalidToolNameBody), false))
	require.Equal(t, http.StatusTeapot, rec.Code)
	require.Equal(t, int64(http.StatusTeapot), gjson.Get(rec.Body.String(), "error.status_code").Int())
	require.Equal(t, "invalid_request", gjson.Get(rec.Body.String(), "error.code").String())
	require.True(t, c.GetBool(OpsSkipPassthroughKey))

	c, rec = newOpenAIUpstreamErrorTestContext(t)
	rule.PassthroughBody = false
	customMessage := "custom administrator message"
	rule.CustomMessage = &customMessage
	rules.setLocalCache([]*model.ErrorPassthroughRule{rule})
	require.False(t, WriteOpenAIClientErrorPassthrough(c, rules, 400, []byte(invalidToolNameBody), false))
	require.Empty(t, rec.Body.String(), "custom messages remain owned by the existing rule path")
}

func TestOpenAIClientErrorPassthrough_StopsCompactHeartbeat(t *testing.T) {
	c, rec := newCompactBridgeTestContext(t, true)
	bindClientErrorPassthroughRule(c)
	stop := StartOpenAICompactSSEKeepalive(c, keepaliveTestInterval)
	defer stop()
	waitForKeepaliveBeats()
	require.True(t, WriteOpenAIClientErrorPassthrough(c, getBoundErrorPassthroughService(c), 400, []byte(invalidToolNameBody), false))
	length := rec.Body.Len()
	waitForKeepaliveBeats()
	require.Equal(t, length, rec.Body.Len(), "no heartbeat may follow the terminal")
	require.Equal(t, 1, strings.Count(rec.Body.String(), "event: response.created\n"))
	require.Equal(t, 1, strings.Count(rec.Body.String(), "event: response.failed\n"))
	streamErr, ok := GetOpsStreamError(c)
	require.True(t, ok)
	require.Equal(t, 400, streamErr.IntendedStatus)
	require.Equal(t, "invalid_request", streamErr.Code)
}

func TestOpenAIClientErrorPassthrough_DoesNotRestartExistingResponse(t *testing.T) {
	for _, responseID := range []string{"resp_existing", "", "nonstandard_id"} {
		t.Run(responseID, func(t *testing.T) {
			c, rec := newOpenAIUpstreamErrorTestContext(t)
			bindClientErrorPassthroughRule(c)
			c.Header("Content-Type", "text/event-stream")
			frame, err := json.Marshal(gin.H{"type": "response.output_text.delta", "sequence_number": 12, "response_id": responseID, "delta": "delivered"})
			require.NoError(t, err)
			_, _ = c.Writer.WriteString("data: " + string(frame) + "\n\n")
			c.Writer.Flush()
			before := rec.Body.String()
			MarkOpenAIClientErrorSemanticOutput(c)
			require.False(t, WriteOpenAIClientErrorPassthrough(c, getBoundErrorPassthroughService(c), 400, []byte(invalidToolNameBody), true))
			require.Equal(t, before, rec.Body.String(), "existing lifecycle is owned by the original stream handler")
		})
	}
}

func TestOpenAIClientErrorPassthrough_CommittedJSONRetainsLogicalStatus(t *testing.T) {
	c, rec := newOpenAIUpstreamErrorTestContext(t)
	bindClientErrorPassthroughRule(c)
	c.Header("Content-Type", "application/json")
	_, _ = c.Writer.WriteString(" \n")
	c.Writer.Flush()
	require.True(t, WriteOpenAIClientErrorPassthrough(c, getBoundErrorPassthroughService(c), 400, []byte(invalidToolNameBody), false))
	require.Equal(t, 200, rec.Code)
	require.Equal(t, int64(400), gjson.Get(rec.Body.String(), "error.status_code").Int())
	streamErr, ok := GetOpsStreamError(c)
	require.True(t, ok)
	require.True(t, streamErr.NonStream)
	require.Equal(t, 400, streamErr.IntendedStatus)
}

func TestOpenAIClientErrorPassthrough_PreservesStructuredRejection(t *testing.T) {
	for _, status := range []int{400, 422} {
		for _, path := range []string{"/v1/responses", "/v1/chat/completions", "/v1/messages"} {
			for _, committed := range []bool{false, true} {
				t.Run(http.StatusText(status)+path+map[bool]string{true: "/committed", false: "/json"}[committed], func(t *testing.T) {
					c, rec := newOpenAIUpstreamErrorTestContext(t)
					c.Request = httptest.NewRequest(http.MethodPost, path, nil)
					bindClientErrorPassthroughRule(c)
					if committed {
						c.Header("Content-Type", "text/event-stream")
						_, _ = c.Writer.WriteString(": ping\n\n")
						c.Writer.Flush()
					}
					svc := &OpenAIGatewayService{cfg: &config.Config{}}
					account := &Account{ID: 465, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true}
					resp := newOpenAIUpstreamErrorResponse(status, invalidToolNameBody)
					var result *OpenAIForwardResult
					var err error
					switch path {
					case "/v1/chat/completions":
						result, err = svc.handleChatCompletionsErrorResponse(resp, c, account)
					case "/v1/messages":
						result, err = svc.handleAnthropicErrorResponse(resp, c, account)
					default:
						result, err = svc.handleErrorResponse(context.Background(), resp, c, account, nil)
					}
					require.Error(t, err)
					require.Nil(t, result)
					var failover *UpstreamFailoverError
					require.False(t, errors.As(err, &failover), "deterministic rejection must not trigger account retry")
					require.Equal(t, StatusActive, account.Status)
					require.True(t, account.Schedulable)
					body := rec.Body.String()
					prefix := "error."
					if committed {
						require.Equal(t, 200, rec.Code)
						var frames []string
						for _, line := range strings.Split(body, "\n") {
							if strings.HasPrefix(line, "data: ") {
								frames = append(frames, strings.TrimPrefix(line, "data: "))
							}
						}
						if path == "/v1/responses" {
							require.Len(t, frames, 2, "create lifecycle before the single failure terminal")
							require.Equal(t, "response.created", gjson.Get(frames[0], "type").String())
							require.Equal(t, "in_progress", gjson.Get(frames[0], "response.status").String())
							require.Equal(t, "null", gjson.Get(frames[0], "response.error").Raw)
							require.Equal(t, "null", gjson.Get(frames[0], "response.usage").Raw)
							require.Equal(t, gjson.Get(frames[0], "response.id").String(), gjson.Get(frames[1], "response.id").String())
							body = frames[1]
							require.Equal(t, "response.failed", gjson.Get(body, "type").String())
							require.Equal(t, int64(1), gjson.Get(body, "sequence_number").Int())
							require.Equal(t, "response", gjson.Get(body, "response.object").String())
							require.Equal(t, "failed", gjson.Get(body, "response.status").String())
							require.True(t, gjson.Get(body, "response.created_at").Exists())
							require.Equal(t, "[]", gjson.Get(body, "response.output").Raw)
							prefix = "response.error."
						} else {
							require.Len(t, frames, 1, "exactly one failure terminal")
							body = frames[0]
						}
						if path == "/v1/messages" {
							require.Contains(t, rec.Body.String(), "event: error\n")
						}
					} else {
						require.Equal(t, status, rec.Code)
					}
					require.True(t, json.Valid([]byte(body)))
					require.Equal(t, "invalid_request_error", gjson.Get(body, prefix+"type").String())
					require.Equal(t, "invalid_request", gjson.Get(body, prefix+"code").String())
					require.Equal(t, "input[535].name", gjson.Get(body, prefix+"param").String())
					require.Equal(t, "必填字段必须是非空字符串（字段：input[535].name）", gjson.Get(body, prefix+"message").String())
					require.Equal(t, int64(status), gjson.Get(body, prefix+"status_code").Int())
					require.NotContains(t, body, `"usage"`)
					require.NotContains(t, body, `"getoken"`)
				})
			}
		}
	}
}
