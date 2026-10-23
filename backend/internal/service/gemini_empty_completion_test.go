//go:build unit

package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestGeminiDeliveryBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name, model, delta, native string
		empty, measured, committed bool
	}{
		{name: "no_usage", model: "gemini-4-argon", delta: `{}`, empty: true},
		{name: "usage_only", model: "gemini-4-argon", delta: `{}`, empty: true, measured: true},
		{name: "heartbeat_committed", model: "gemini-4-argon", delta: `{}`, native: "max_tokens", empty: true, measured: true, committed: true},
		{name: "native_privacy", model: "gemini-4-argon", delta: `{}`, native: "https://private.invalid/secret", empty: true},
		{name: "text", model: "gemini-4-argon", delta: `{"content":"OK"}`, measured: true},
		{name: "tools", model: "gemini-4-argon", delta: `{"tool_calls":[{"index":0,"id":"call_a","function":{"name":"clock","arguments":"{}"}}]}`, measured: true},
		{name: "reasoning", model: "gemini-3.8-flash", delta: `{"reasoning_content":"synthetic"}`, measured: true},
		{name: "image", model: "gemini-4-argon", delta: `{"images":[{"image_url":{"url":"data:image/png;base64,c3ludGhldGlj"}}]}`, measured: true},
		{name: "unrelated_model", model: "unrelated", delta: `{}`, measured: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			native, _ := json.Marshal(tc.native)
			payload := `data: {"choices":[{"delta":` + tc.delta + `}]}` + "\n\n"
			payload += `data: {"choices":[{"delta":{},"finish_reason":"stop","native_finish_reason":` + string(native) + `}]}` + "\n\n"
			if tc.measured {
				payload += `data: {"choices":[],"usage":{"prompt_tokens":100,"completion_tokens":0,"prompt_tokens_details":{"cached_tokens":90}}}` + "\n\n"
			}
			payload += "data: [DONE]\n\n"
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest("POST", "/v1/chat/completions", nil)
			if tc.committed {
				c.Writer.Header().Set("Content-Type", "text/event-stream")
				_, _ = c.Writer.WriteString(": ping\n\n")
				c.Writer.Flush()
			}
			svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig()}
			resp := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(payload))}
			result, err := svc.streamRawChatCompletions(c, resp, rawChatCompletionsTestAccount(), tc.model, tc.model, tc.model, nil, nil, time.Now(), 1)
			require.NotNil(t, result)
			if tc.empty {
				require.Error(t, err)
				require.Contains(t, rec.Body.String(), "upstream_empty_completion")
				var failover *UpstreamFailoverError
				require.NotErrorAs(t, err, &failover)
				require.NotContains(t, rec.Body.String(), "[DONE]")
				if tc.committed {
					require.Equal(t, 200, rec.Code)
					require.Contains(t, rec.Body.String(), `"status_code":502`)
				} else {
					require.Equal(t, 502, rec.Code)
				}
			} else {
				require.NoError(t, err)
				require.Contains(t, rec.Body.String(), "[DONE]")
			}
			require.NotContains(t, rec.Body.String(), "private.invalid")
			if tc.measured {
				require.Equal(t, 100, result.Usage.InputTokens)
				require.Equal(t, 90, result.Usage.CacheReadInputTokens)
			} else {
				require.Zero(t, result.Usage.InputTokens)
			}
		})
	}
}

func TestGeminiRawEmptyCompletion(t *testing.T) {
	for _, stream := range []bool{true, false} {
		t.Run(map[bool]string{true: "stream", false: "json"}[stream], func(t *testing.T) {
			body := []byte(`{"model":"gemini-4-argon","messages":[{"role":"user","content":"synthetic"}],"stream":` + map[bool]string{true: "true", false: "false"}[stream] + `}`)
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(body)))
			payload := `{"model":"gemini-4-argon","choices":[{"index":0,"delta":{},"message":{"role":"assistant","content":""},"finish_reason":"stop","native_finish_reason":"malformed_function_call"}],"usage":{"prompt_tokens":764973,"completion_tokens":0,"prompt_tokens_details":{"cached_tokens":762924}}}`
			ct := "application/json"
			if stream {
				ct = "text/event-stream"
				payload = "data: " + payload + "\n\ndata: [DONE]\n\n"
			}
			upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{ct}}, Body: io.NopCloser(strings.NewReader(payload))}}
			svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig(), httpUpstream: upstream}
			result, err := svc.forwardAsRawChatCompletions(context.Background(), c, rawChatCompletionsTestAccount(), body, "")
			require.Error(t, err)
			var failover *UpstreamFailoverError
			require.NotErrorAs(t, err, &failover)
			require.NotNil(t, result)
			// The raw boundary retains inclusive OpenAI prompt tokens. The
			// existing billing layer subtracts cache exactly once.
			require.Equal(t, 764973, result.Usage.InputTokens)
			require.Equal(t, 762924, result.Usage.CacheReadInputTokens)
			require.Equal(t, 0, result.Usage.OutputTokens)
			require.Contains(t, rec.Body.String(), "upstream_empty_completion")
			require.Contains(t, rec.Body.String(), "malformed_function_call")
			require.NotContains(t, rec.Body.String(), "data: [DONE]")
			require.NotContains(t, rec.Body.String(), `"choices"`)
		})
	}
}
