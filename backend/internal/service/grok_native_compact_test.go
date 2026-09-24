//go:build unit

package service

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestGrokNativeCompactionBridge(t *testing.T) {
	for _, wire := range []string{"json", "sse", "missing_encrypted", "incomplete"} {
		t.Run(wire, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			r := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(r)
			body := []byte(`{"model":"grok-4.7","stream":true,"input":[{"type":"function_call","call_id":"call_1","name":"lookup","arguments":"{}"},{"type":"function_call_output","call_id":"call_1","output":"keep tool result"},{"type":"message","role":"user","content":"continue the existing task"},{"type":"compaction_trigger"}]}`)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
			MarkOpenAINativeCompactionV2(c)
			account := &Account{ID: 9247, Platform: PlatformGrok, Type: AccountTypeAPIKey, Credentials: map[string]any{"api_key": "test-only", "base_url": "https://api.x.ai/v1"}}
			response := `{"id":"resp_compact","object":"response","created_at":1,"status":"completed","model":"grok-4.7","output":[{"type":"reasoning","encrypted_content":"opaque-state","summary":[]},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"<summary>keep tool result and existing task</summary>"}]}],"usage":{"input_tokens":100,"output_tokens":20}}`
			contentType := "application/json"
			if wire == "missing_encrypted" {
				response = strings.ReplaceAll(response, `"encrypted_content":"opaque-state",`, "")
			}
			if wire == "incomplete" {
				response = strings.ReplaceAll(response, `"status":"completed"`, `"status":"incomplete"`)
			}
			if wire == "sse" {
				response = "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":" + response + "}\n\n"
				contentType = "text/event-stream"
			}
			up := &httpUpstreamRecorder{responses: []*http.Response{{StatusCode: 200, Header: http.Header{"Content-Type": []string{contentType}}, Body: io.NopCloser(strings.NewReader(response))}}}
			svc := &OpenAIGatewayService{httpUpstream: up}
			result, err := svc.forwardGrokResponses(context.Background(), c, account, body, "grok-4.7", true, time.Now())
			require.Len(t, up.bodies, 1)
			require.False(t, HasCompactionTriggerInInput(up.bodies[0]), "native trigger must be translated into a summarization turn")
			require.False(t, gjson.GetBytes(up.bodies[0], "stream").Bool())
			require.Contains(t, string(up.bodies[0]), "keep tool result")
			require.Contains(t, string(up.bodies[0]), "Primary Request and Intent")
			require.Contains(t, string(body), "compaction_trigger", "caller body is immutable")
			require.NotNil(t, result, "metered conversion failures still return usage")
			require.Equal(t, 100, result.Usage.InputTokens)
			require.Equal(t, 20, result.Usage.OutputTokens)
			if wire == "missing_encrypted" || wire == "incomplete" {
				require.Error(t, err)
				require.NotContains(t, r.Body.String(), "response.completed")
				return
			}
			require.NoError(t, err)
			require.Contains(t, r.Header().Get("Content-Type"), "text/event-stream")
			require.Contains(t, r.Body.String(), "response.output_item.done")
			require.Contains(t, r.Body.String(), `"type":"compaction"`)
			require.Contains(t, r.Body.String(), "opaque-state")
			require.Contains(t, r.Body.String(), "response.completed")
		})
	}
}

func TestGrokCompactBuilderKeepsHistoryAndIntegerPrecision(t *testing.T) {
	body := []byte(`{"input":[{"type":"compaction_trigger"},{"type":"message","role":"user","content":"literal compaction_trigger"},{"type":"function_call_output","call_id":"c1","output":{"id":9007199254740993}},{"type":"compaction_trigger"}]}`)
	out, err := buildGrokCompactRequestBody(body)
	require.NoError(t, err)
	require.False(t, HasCompactionTriggerInInput(out))
	require.Equal(t, "literal compaction_trigger", gjson.GetBytes(out, "input.0.content").String())
	require.Equal(t, "9007199254740993", gjson.GetBytes(out, "input.1.output.id").Raw)
}
