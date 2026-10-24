//go:build unit

package service

import (
	"context"
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

// Exercise the deployed OpenAI-account route, including protocol selection:
// Gemini Chat Completions ingress -> Responses upstream -> Chat Completions.
// These fixtures contain no production request or response text.
func TestGeminiResponsesChatDelivery(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, stream := range []bool{true, false} {
		for _, tc := range []struct {
			name, events, output, model         string
			empty, noUsage, committed, activity bool
			outputTokens                        int
		}{
			{name: "role_usage_only", output: `[]`, empty: true},
			{name: "heartbeat_already_committed", output: `[]`, empty: true, committed: true},
			{name: "role_no_usage", output: `[]`, empty: true, noUsage: true},
			{name: "opaque_output_usage", output: `[{"type":"reasoning","encrypted_content":"synthetic","summary":[]}]`, empty: true, outputTokens: 13},
			{name: "unrelated_model_unchanged", model: "gpt-5.5", output: `[]`},
			{name: "empty_reasoning", events: `data: {"type":"response.reasoning_summary_text.delta","delta":""}` + "\n\n", output: `[]`, empty: true},
			{name: "encrypted_reasoning", activity: true, events: `data: {"type":"response.output_item.added","item":{"type":"reasoning","id":"rs_test","encrypted_content":"synthetic","summary":[]}}` + "\n\n", output: `[{"type":"reasoning","id":"rs_test","encrypted_content":"synthetic","summary":[]}]`, empty: true},
			{name: "text", events: `data: {"type":"response.output_text.delta","delta":"synthetic answer"}` + "\n\n", output: `[{"type":"message","content":[{"type":"output_text","text":"synthetic answer"}]}]`},
			{name: "reasoning_text", events: `data: {"type":"response.reasoning_summary_text.delta","delta":"synthetic reasoning"}` + "\n\n", output: `[{"type":"reasoning","summary":[{"type":"summary_text","text":"synthetic reasoning"}]}]`},
			{name: "tool", events: `data: {"type":"response.output_item.added","item":{"type":"function_call","call_id":"call_test","name":"clock","arguments":"{}"}}` + "\n\n", output: `[{"type":"function_call","call_id":"call_test","name":"clock","arguments":"{}"}]`},
			{name: "refusal", events: `data: {"type":"response.refusal.delta","delta":"synthetic refusal"}` + "\n\n", output: `[{"type":"message","content":[{"type":"refusal","refusal":"synthetic refusal"}]}]`},
		} {
			t.Run(fmt.Sprintf("stream_%t/%s", stream, tc.name), func(t *testing.T) {
				if tc.committed && !stream {
					t.Skip("only streaming keepalive commits HTTP headers before completion")
				}
				model, upstreamModel := tc.model, tc.model
				if model == "" {
					model, upstreamModel = "gemini-4-argon", "ultra/gemini-4-argon-high"
				}
				usage := fmt.Sprintf(`,"usage":{"input_tokens":100,"output_tokens":%d,"input_tokens_details":{"cached_tokens":90}}`, tc.outputTokens)
				if tc.noUsage {
					usage = ""
				}
				payload := fmt.Sprintf(`data: {"type":"response.created","response":{"id":"resp_test","model":%q}}`, upstreamModel) + "\n\n" + tc.events
				payload += fmt.Sprintf(`data: {"type":"response.completed","response":{"id":"resp_test","status":"completed","model":%q,"output":`, upstreamModel) + tc.output + usage + "}}\n\n"
				body := []byte(fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"synthetic"}],"stream":%t}`, model, stream))
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(body)))
				if tc.committed {
					c.Writer.Header().Set("Content-Type", "text/event-stream")
					_, _ = c.Writer.WriteString(": ping\n\n")
					c.Writer.Flush()
				}
				upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}, "X-Request-Id": []string{"rid_synthetic"}}, Body: io.NopCloser(strings.NewReader(payload))}}
				svc := &OpenAIGatewayService{cfg: structuredInputRetryTestConfig(), httpUpstream: upstream}
				account := structuredInputRetryTestAccount()
				account.Credentials["model_mapping"] = map[string]any{model: upstreamModel}
				result, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body, "", "")
				require.Len(t, upstream.requests, 1, "an empty metered response must never be replayed")
				require.Equal(t, "/v1/responses", upstream.requests[0].URL.Path)
				if tc.empty {
					require.Error(t, err)
					var failover *UpstreamFailoverError
					require.NotErrorAs(t, err, &failover)
					if tc.committed || (stream && tc.activity) {
						require.Equal(t, http.StatusOK, rec.Code)
						require.Contains(t, rec.Body.String(), `"status_code":502`)
					} else {
						require.Equal(t, http.StatusBadGateway, rec.Code)
					}
					require.Contains(t, rec.Body.String(), "upstream_empty_completion")
					require.Equal(t, 1, strings.Count(rec.Body.String(), `"error":`))
					require.NotContains(t, rec.Body.String(), "data: [DONE]")
					events, exists := c.Get(OpsUpstreamErrorsKey)
					require.True(t, exists)
					require.Len(t, events.([]*OpsUpstreamErrorEvent), 1)
				} else {
					require.NoError(t, err)
					require.Equal(t, http.StatusOK, rec.Code)
				}
				if !tc.noUsage {
					require.NotNil(t, result)
					require.Equal(t, 100, result.Usage.InputTokens)
					require.Equal(t, 90, result.Usage.CacheReadInputTokens)
					require.Equal(t, tc.outputTokens, result.Usage.OutputTokens)
					if tc.empty {
						require.Contains(t, rec.Body.String(), `"input_tokens":100`)
						require.Contains(t, rec.Body.String(), `"cache_read_input_tokens":90`)
					} else {
						require.Contains(t, rec.Body.String(), `"prompt_tokens":100`)
					}
				}
			})
		}
	}
}

func TestGeminiResponsesChatDeliveryPrefixAndExplicitError(t *testing.T) {
	for _, tc := range []struct {
		name           string
		idBytes        int
		upstreamError  bool
		empty          bool
		noUsage        bool
		noCreated      bool
		opaqueProgress bool
	}{
		{name: "prefix_within_limit", idBytes: 63 * 1024},
		{name: "prefix_over_limit", idBytes: 64 * 1024, empty: true},
		{name: "explicit_upstream_error", idBytes: 10, upstreamError: true},
		{name: "explicit_error_after_role_no_usage", idBytes: 10, upstreamError: true, noUsage: true},
		{name: "explicit_error_after_overflow_no_usage", idBytes: 64 * 1024, upstreamError: true, noUsage: true},
		{name: "explicit_error_after_opaque_progress_no_usage", upstreamError: true, noUsage: true, noCreated: true, opaqueProgress: true},
		{name: "pre_generation_rejection_unchanged", upstreamError: true, noUsage: true, noCreated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := fmt.Sprintf(`data: {"type":"response.created","response":{"id":%q,"model":"gemini-4-argon"}}`, strings.Repeat("r", tc.idBytes)) + "\n\n"
			if tc.noCreated {
				payload = ""
			}
			if tc.opaqueProgress {
				payload += `data: {"type":"response.output_item.added","item":{"type":"reasoning","encrypted_content":"synthetic","summary":[]}}` + "\n\n"
			}
			terminal := `data: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":100,"output_tokens":2,"input_tokens_details":{"cached_tokens":90}}}}` + "\n\n"
			if tc.upstreamError {
				terminal = `data: {"type":"response.failed","response":{"status":"failed","error":{"code":"server_error","message":"synthetic upstream failure"},"usage":{"input_tokens":100,"output_tokens":2,"input_tokens_details":{"cached_tokens":90}}}}` + "\n\n"
				if tc.noUsage {
					terminal = `data: {"type":"response.failed","response":{"status":"failed","error":{"code":"server_error","message":"synthetic upstream failure"}}}` + "\n\n"
				}
			} else {
				payload += `data: {"type":"response.output_text.delta","delta":"synthetic visible answer"}` + "\n\n"
			}
			payload += terminal
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
			resp := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(payload))}
			svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig()}
			result, err := svc.handleChatStreamingResponse(resp, c, rawChatCompletionsTestAccount(), "gemini-4-argon", "gemini-4-argon", "ultra/gemini-4-argon-high", time.Now(), 1)
			if tc.noCreated && !tc.opaqueProgress {
				var failover *UpstreamFailoverError
				require.ErrorAs(t, err, &failover)
				require.Nil(t, result)
				require.False(t, c.Writer.Written())
				return
			}
			require.NotNil(t, result)
			if tc.noUsage {
				require.Zero(t, result.Usage.InputTokens)
				require.Zero(t, result.Usage.CacheReadInputTokens)
				require.Zero(t, result.Usage.OutputTokens)
			} else {
				require.Equal(t, 100, result.Usage.InputTokens)
				require.Equal(t, 90, result.Usage.CacheReadInputTokens)
				require.Equal(t, 2, result.Usage.OutputTokens)
			}
			if tc.empty || tc.upstreamError {
				require.Error(t, err)
				var failover *UpstreamFailoverError
				require.NotErrorAs(t, err, &failover)
				require.Equal(t, 502, rec.Code)
				require.NotContains(t, rec.Body.String(), "data: [DONE]")
				require.NotContains(t, rec.Body.String(), "synthetic visible answer")
				require.Equal(t, 1, strings.Count(rec.Body.String(), `"error":`))
				events, exists := c.Get(OpsUpstreamErrorsKey)
				require.True(t, exists)
				require.Len(t, events.([]*OpsUpstreamErrorEvent), 1)
				if tc.empty {
					require.Contains(t, rec.Body.String(), "safe buffer limit")
				} else {
					require.Contains(t, rec.Body.String(), "synthetic upstream failure")
					require.NotContains(t, rec.Body.String(), "upstream_empty_completion")
				}
			} else {
				require.NoError(t, err)
				require.Contains(t, rec.Body.String(), "synthetic visible answer")
				require.Contains(t, rec.Body.String(), "data: [DONE]")
			}
		})
	}
}
