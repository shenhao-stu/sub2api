package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"io"
	"net/http"
	"strings"
	"testing"
)

func geminiResponsesFixture(t *testing.T, body, payload string) (*ForwardResult, error, *queuedHTTPUpstreamStub, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	up := &queuedHTTPUpstreamStub{responses: []*http.Response{{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(payload))}}}
	svc := &GeminiMessagesCompatService{httpUpstream: up, cfg: &config.Config{}}
	a := &Account{ID: 811, Platform: PlatformGemini, Type: AccountTypeServiceAccount, Credentials: map[string]any{"auth_mode": "apikey", "api_key": "isolated-test-key", "location": "global", "tier_id": "vertex"}, Concurrency: 1}
	c, rec := newAntigravityCompatContext(http.MethodPost, "/v1/responses", []byte(body))
	result, err := svc.ForwardAsResponses(context.Background(), c, a, []byte(body))
	return result, err, up, rec.Body.String()
}

func TestGeminiResponsesNativeForwardPreservesHistoryAndUsage(t *testing.T) {
	const history = `[{"role":"user","content":[{"type":"input_text","text":"before"},{"type":"input_image","image_url":"https://example.invalid/a.png","mime_type":"image/png"},{"type":"input_text","text":"after"}]},{"type":"function_call","call_id":"a","name":"lookup","arguments":"{\"id\":9007199254740993}","extra_content":{"google":{"thought_signature":"real-signature"}}},{"type":"function_call","call_id":"b","name":"lookup","arguments":"{\"id\":2}"},{"type":"function_call_output","call_id":"b","output":{"second":2}},{"type":"function_call_output","call_id":"a","output":{"first":9007199254740993}},{"role":"user","content":[{"type":"input_text","text":"continue"}]}]`
	const response = `{"candidates":[{"content":{"role":"model","parts":[{"text":"OK"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":19,"cachedContentTokenCount":5,"candidatesTokenCount":3,"thoughtsTokenCount":2}}`
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			payload := response
			if stream {
				payload = "data: " + response + "\n\n"
			}
			body := fmt.Sprintf(`{"model":"gemini-4-argon","stream":%t,"instructions":"system directive","input":%s,"tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}],"tool_choice":"none"}`, stream, history)
			result, err, up, wire := geminiResponsesFixture(t, body, payload)
			require.NoError(t, err, wire)
			require.NotNil(t, result)
			require.Equal(t, 1, up.callCount)
			require.Equal(t, 14, result.Usage.InputTokens)
			require.Equal(t, 5, result.Usage.OutputTokens)
			require.Equal(t, 5, result.Usage.CacheReadInputTokens)
			native := up.requestBodies[0]
			require.Equal(t, "real-signature", gjson.GetBytes(native, "contents.1.parts.0.thoughtSignature").String())
			require.Equal(t, "9007199254740993", gjson.GetBytes(native, "contents.1.parts.0.functionCall.args.id").Raw)
			require.Equal(t, "9007199254740993", gjson.GetBytes(native, "contents.2.parts.0.functionResponse.response.first").Raw)
			require.Equal(t, "2", gjson.GetBytes(native, "contents.2.parts.1.functionResponse.response.second").Raw)
			require.Equal(t, "continue", gjson.GetBytes(native, "contents.3.parts.0.text").String())
			require.Equal(t, "NONE", gjson.GetBytes(native, "toolConfig.functionCallingConfig.mode").String())
			require.Equal(t, "image/png", gjson.GetBytes(native, "contents.0.parts.1.fileData.mimeType").String())
			if stream {
				require.Contains(t, wire, "event: response.completed")
				require.NotContains(t, wire, "[DONE]")
				require.NotNil(t, result.Usage.receiptMeasured)
			} else {
				require.Equal(t, "response", gjson.Get(wire, "object").String())
				require.Equal(t, "OK", gjson.Get(wire, "output.0.content.0.text").String())
			}
		})
	}
}

func TestGeminiResponsesRejectsUnrepresentableStateWithoutDispatch(t *testing.T) {
	for _, extra := range []string{
		`"input":"x","previous_response_id":"resp_unknown"`,
		`"input":"x","background":true`,
		`"input":"x","store":true`,
		`"input":"x","parallel_tool_calls":false`,
		`"input":"x","text":{"format":{"type":"json_schema","schema":{"type":"object"}}}`,
		`"input":"x","service_tier":"priority"`,
		`"input":"x","truncation":"auto"`,
		`"input":"x","tools":[{"type":"web_search"}]`,
		`"input":[{"role":"user","content":"x"},{"role":"system","content":"later"}]`,
		`"input":[{"type":"function_call","call_id":"a","name":"f","arguments":"{"}]`,
		`"input":[{"type":"reasoning","encrypted_content":"opaque"}]`,
		`"input":[{"type":"unknown","content":"must not disappear"}]`,
	} {
		t.Run(extra, func(t *testing.T) {
			result, err, up, wire := geminiResponsesFixture(t, `{"model":"gemini-4-argon",`+extra+`}`, "")
			require.Error(t, err)
			require.Nil(t, result)
			require.Zero(t, up.callCount)
			require.Equal(t, "invalid_request_error", gjson.Get(wire, "error.type").String())
		})
	}
}

func TestGeminiResponsesToolSignatureRoundTrip(t *testing.T) {
	const response = `{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"lookup","args":{"id":9007199254740993}},"thoughtSignature":"original-signature"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":19,"candidatesTokenCount":3}}`
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			payload := response
			if stream {
				payload = "data: " + response + "\n\n"
			}
			_, err, _, wire := geminiResponsesFixture(t, fmt.Sprintf(`{"model":"gemini-4-argon","stream":%t,"input":"call lookup","tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}]}`, stream), payload)
			require.NoError(t, err)
			var item gjson.Result
			if stream {
				for _, line := range strings.Split(wire, "\n") {
					if strings.HasPrefix(line, "data:") {
						v := gjson.Parse(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
						if v.Get("type").String() == "response.completed" {
							item = v.Get("response.output.0")
						}
					}
				}
			} else {
				item = gjson.Get(wire, "output.0")
			}
			require.Equal(t, "function_call", item.Get("type").String(), wire)
			require.Equal(t, "original-signature", item.Get("extra_content.google.thought_signature").String())
			require.Contains(t, item.Get("arguments").String(), "9007199254740993")
			output, _ := json.Marshal(map[string]any{"type": "function_call_output", "call_id": item.Get("call_id").String(), "output": map[string]any{"value": "result"}})
			raw := `{"model":"gemini-4-argon","input":[` + item.Raw + `,` + string(output) + `]}`
			chat, _, err := geminiResponsesToChat([]byte(raw))
			require.NoError(t, err)
			native, fields := geminiChatContents(chat)
			require.Nil(t, fields)
			require.Equal(t, "original-signature", gjson.GetBytes(native, "0.parts.0.thoughtSignature").String())
		})
	}
}

func TestGeminiResponsesBufferedThinkingAndStringToolArguments(t *testing.T) {
	const payload = `{"candidates":[{"content":{"role":"model","parts":[{"thought":true,"text":"synthetic thought"},{"text":"visible answer"},{"functionCall":{"name":"lookup","args":"{\"id\":9007199254740993}"},"thoughtSignature":"signature"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":19,"candidatesTokenCount":3,"thoughtsTokenCount":2}}`
	result, err, _, wire := geminiResponsesFixture(t, `{"model":"gemini-4-argon","input":"fixture","tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}]}`, payload)
	require.NoError(t, err, wire)
	require.NotNil(t, result)
	var thought, visible, args string
	for _, item := range gjson.Get(wire, "output").Array() {
		switch item.Get("type").String() {
		case "reasoning":
			thought = item.Get("summary.0.text").String()
		case "message":
			visible = item.Get("content.0.text").String()
		case "function_call":
			args = item.Get("arguments").String()
		}
	}
	require.Equal(t, "synthetic thought", thought)
	require.Equal(t, "visible answer", visible)
	require.Equal(t, `{"id":9007199254740993}`, args)
}

func TestGeminiResponsesImageAndTruncatedTools(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, image := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%t/image=%t", stream, image), func(t *testing.T) {
				part := `{"functionCall":{"name":"f","args":null}}`
				finish := "MAX_TOKENS"
				detail := ""
				if image {
					part = `{"inlineData":{"mimeType":"image/png","data":"aW1hZ2U="}}`
					finish = "STOP"
					detail = `,"candidatesTokensDetails":[{"modality":"IMAGE","tokenCount":10}]`
				}
				payload := fmt.Sprintf(`{"candidates":[{"content":{"parts":[%s]},"finishReason":"%s"}],"usageMetadata":{"promptTokenCount":19,"candidatesTokenCount":13%s}}`, part, finish, detail)
				if stream {
					payload = "data: " + payload + "\n\n"
				}
				result, err, _, wire := geminiResponsesFixture(t, fmt.Sprintf(`{"model":"gemini-4-argon","stream":%t,"input":"fixture"}`, stream), payload)
				require.NoError(t, err, wire)
				require.NotNil(t, result.Usage.receiptMeasured)
				if image {
					require.Equal(t, 10, result.Usage.ImageOutputTokens)
					require.Contains(t, wire, "data:image/png;base64,aW1hZ2U=")
				} else if stream {
					require.Contains(t, wire, "event: response.incomplete")
					require.NotContains(t, wire, "event: response.completed")
				} else {
					require.Equal(t, "incomplete", gjson.Get(wire, "status").String())
					require.Equal(t, "{}", gjson.Get(wire, "output.0.arguments").String())
				}
			})
		}
	}
}

func TestGeminiResponsesCustomSignatureRoundTrip(t *testing.T) {
	const tools = `[{"type":"custom","name":"exec","format":{"type":"text"}}]`
	const native = `{"candidates":[{"content":{"parts":[{"functionCall":{"name":"exec","args":{"input":"echo fixture"}},"thoughtSignature":"custom-signature"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":19,"candidatesTokenCount":3}}`
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			payload := native
			if stream {
				payload = "data: " + native + "\n\n"
			}
			_, err, _, wire := geminiResponsesFixture(t, fmt.Sprintf(`{"model":"gemini-4-argon","stream":%t,"input":"fixture","tools":%s}`, stream, tools), payload)
			require.NoError(t, err, wire)
			item := gjson.Get(wire, "output.0")
			if stream {
				for _, line := range strings.Split(wire, "\n") {
					if !strings.HasPrefix(line, "data: ") {
						continue
					}
					event := gjson.Parse(strings.TrimPrefix(line, "data: "))
					if event.Get("type").String() == "response.output_item.done" {
						item = event.Get("item")
					}
				}
			}
			require.Equal(t, "custom_tool_call", item.Get("type").String(), wire)
			require.Equal(t, "custom-signature", item.Get("extra_content.google.thought_signature").String(), wire)
			body := `{"model":"gemini-4-argon","tools":` + tools + `,"input":[` + item.Raw + `,{"type":"custom_tool_call_output","call_id":"` + item.Get("call_id").String() + `","output":"fixture result"},{"role":"user","content":"continue"}]}`
			chat, _, err := geminiResponsesToChat([]byte(body))
			require.NoError(t, err)
			contents, fields := geminiChatContents(chat)
			require.Nil(t, fields)
			require.Equal(t, "custom-signature", gjson.GetBytes(contents, "0.parts.0.thoughtSignature").String())
			require.Equal(t, "echo fixture", gjson.GetBytes(contents, "0.parts.0.functionCall.args.input").String())
		})
	}
}
