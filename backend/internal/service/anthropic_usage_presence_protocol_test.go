//go:build unit

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

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const anthropicUsageTextSSE = `event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"fixture answer"}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

`

func TestAnthropicUsagePresenceAcrossProtocols(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, native := range []bool{false, true} {
		for _, responses := range []bool{false, true} {
			for _, stream := range []bool{false, true} {
				for _, tc := range []struct {
					name, start, delta string
					want               bool
					input, output      int64
				}{
					{"absent", "", "", false, 0, 0},
					{"null", `,"usage":null`, `,"usage":null`, false, 0, 0},
					{"initial_zero_is_not_final", `,"usage":{"input_tokens":0,"output_tokens":0}`, "", false, 0, 0},
					{"terminal_zero", "", `,"usage":{"input_tokens":0,"output_tokens":0}`, true, 0, 0},
					{"split_zero", `,"usage":{"input_tokens":0}`, `,"usage":{"output_tokens":0}`, true, 0, 0},
					{"input_only", `,"usage":{"input_tokens":0}`, "", false, 0, 0},
					{"output_only", "", `,"usage":{"output_tokens":0}`, false, 0, 0},
					{"empty", `,"usage":{}`, `,"usage":{}`, false, 0, 0},
					{"negative", `,"usage":{"input_tokens":0}`, `,"usage":{"output_tokens":-1}`, false, 0, 0},
					{"overflow", `,"usage":{"input_tokens":0}`, `,"usage":{"output_tokens":9223372036854775808}`, false, 0, 0},
					{"duplicate", "", `,"usage":{"input_tokens":0,"output_tokens":0,"output_tokens":0}`, false, 0, 0},
					{"duplicate_usage", "", `,"usage":{"input_tokens":0,"output_tokens":0},"usage":{"input_tokens":0,"output_tokens":0}`, false, 0, 0},
					{"positive", `,"usage":{"input_tokens":10}`, `,"usage":{"output_tokens":4}`, true, 10, 4},
					{"cache_only_delta", "", `,"usage":{"input_tokens":0,"output_tokens":0,"cache_read_input_tokens":8}`, true, 8, 0},
				} {
					t.Run(fmt.Sprintf("native=%t/responses=%t/stream=%t/%s", native, responses, stream, tc.name), func(t *testing.T) {
						sse := fmt.Sprintf("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_fixture\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"k3\",\"content\":[]%s}}\n\n", tc.start) + anthropicUsageTextSSE
						sse += fmt.Sprintf("event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}%s}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n", tc.delta)
						upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(sse))}}
						payload := `"messages":[{"role":"user","content":"fixture"}]`
						path := "/v1/chat/completions"
						if responses {
							payload, path = `"input":"fixture"`, "/v1/responses"
						}
						body := []byte(fmt.Sprintf(`{"model":"k3","stream":%t,%s}`, stream, payload))
						rec := httptest.NewRecorder()
						c, _ := gin.CreateTestContext(rec)
						c.Request = httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
						if native {
							svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig(), httpUpstream: upstream}
							var err error
							if responses {
								_, err = svc.forwardResponsesViaNativeAnthropic(context.Background(), c, nativeAnthropicTestAccount(), body, "")
							} else {
								_, err = svc.forwardChatCompletionsViaNativeAnthropic(context.Background(), c, nativeAnthropicTestAccount(), body, "")
							}
							require.NoError(t, err)
						} else {
							svc := &GatewayService{cfg: rawChatCompletionsTestConfig(), httpUpstream: upstream}
							account := &Account{ID: 1, Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Credentials: map[string]any{"api_key": "sk-fixture"}}
							var err error
							if responses {
								_, err = svc.ForwardAsResponses(context.Background(), c, account, body, nil)
							} else {
								_, err = svc.ForwardAsChatCompletions(context.Background(), c, account, body, nil)
							}
							require.NoError(t, err)
						}
						require.Contains(t, rec.Body.String(), "fixture answer")
						var receipts []gjson.Result
						if stream {
							for _, line := range strings.Split(rec.Body.String(), "\n") {
								if !strings.HasPrefix(line, "data: ") {
									continue
								}
								field := "usage"
								if responses {
									field = "response.usage"
								}
								if u := gjson.Get(strings.TrimPrefix(line, "data: "), field); u.IsObject() {
									receipts = append(receipts, u)
								}
							}
						} else if u := gjson.Get(rec.Body.String(), "usage"); u.IsObject() {
							receipts = append(receipts, u)
						}
						if !tc.want {
							require.Empty(t, receipts)
							return
						}
						require.Len(t, receipts, 1)
						input, output := "prompt_tokens", "completion_tokens"
						if responses {
							input, output = "input_tokens", "output_tokens"
						}
						require.Equal(t, tc.input, receipts[0].Get(input).Int())
						require.Equal(t, tc.output, receipts[0].Get(output).Int())
					})
				}
			}
		}
	}
}

func TestGeminiSyntheticAnthropicZeroRemainsUnknown(t *testing.T) {
	raw := []byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"fixture answer"}]},"finishReason":"STOP"}]}`)
	var response map[string]any
	require.NoError(t, json.Unmarshal(raw, &response))
	converted, _, err := geminiResponseToChatCompletions(response, "gemini-fixture", raw, nil)
	require.NoError(t, err)
	require.Nil(t, converted.Usage)
}
