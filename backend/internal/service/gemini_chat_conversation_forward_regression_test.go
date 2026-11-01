package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// These tests inspect the final HTTP body, after normalization and thinking
// helpers, rather than testing an intermediate converter in isolation.
func conversationForward(t *testing.T, messages string, stream bool) []byte {
	t.Helper()
	const response = `{"candidates":[{"content":{"role":"model","parts":[{"text":"OK"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":11,"candidatesTokenCount":3,"thoughtsTokenCount":2,"totalTokenCount":16}}`
	responseBody, contentType := response, "application/json"
	if stream {
		responseBody, contentType = "data: "+response+"\n\n", "text/event-stream"
	}
	upstream := &queuedHTTPUpstreamStub{responses: []*http.Response{{
		StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{contentType}},
		Body: io.NopCloser(strings.NewReader(responseBody)),
	}}}
	upstream.onCall = func(req *http.Request, _ *queuedHTTPUpstreamStub) {
		require.Equal(t, "aiplatform.googleapis.com", req.URL.Host)
		require.True(t, strings.HasPrefix(req.URL.Path, "/v1/"), "v1 positional tool-result contract")
	}
	svc := &GeminiMessagesCompatService{httpUpstream: upstream, cfg: &config.Config{}}
	account := &Account{ID: 811, Platform: PlatformGemini, Type: AccountTypeServiceAccount,
		Credentials: map[string]any{"auth_mode": "apikey", "api_key": "isolated-test-key", "location": "global", "tier_id": "vertex"}, Concurrency: 1}
	body := []byte(fmt.Sprintf(`{"model":"gemini-4-argon","stream":%t,"stream_options":{"include_usage":true},"messages":[%s]}`, stream, messages))
	c, recorder := newAntigravityCompatContext(http.MethodPost, "/v1/chat/completions", body)
	result, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body)
	require.NoError(t, err, recorder.Body.String())
	require.NotNil(t, result)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, 11, result.Usage.InputTokens)
	require.Equal(t, 5, result.Usage.OutputTokens)
	require.Equal(t, 1, upstream.callCount, "one isolated HTTP dispatch, no retry")
	require.Len(t, upstream.requestBodies, 1)
	wire := recorder.Body.String()
	if stream {
		require.Equal(t, 1, strings.Count(wire, "data: [DONE]"))
		var terminal gjson.Result
		for _, line := range strings.Split(wire, "\n") {
			if payload, ok := strings.CutPrefix(line, "data: "); ok && payload != "[DONE]" {
				if usage := gjson.Get(payload, "usage"); usage.Exists() {
					terminal = usage
				}
			}
		}
		require.EqualValues(t, 11, terminal.Get("prompt_tokens").Int())
		require.EqualValues(t, 5, terminal.Get("completion_tokens").Int())
	} else {
		require.Equal(t, "OK", gjson.Get(wire, "choices.0.message.content").String())
		require.EqualValues(t, 11, gjson.Get(wire, "usage.prompt_tokens").Int())
		require.EqualValues(t, 5, gjson.Get(wire, "usage.completion_tokens").Int())
	}
	return upstream.requestBodies[0]
}

func conversationJSON(t *testing.T, raw string) any {
	t.Helper()
	d := json.NewDecoder(strings.NewReader(raw))
	d.UseNumber()
	var value any
	require.NoError(t, d.Decode(&value))
	return value
}

func conversationParts(t *testing.T, body []byte) []any {
	t.Helper()
	var parts []any
	for _, content := range gjson.GetBytes(body, "contents").Array() {
		for _, part := range content.Get("parts").Array() {
			value := conversationJSON(t, part.Raw).(map[string]any)
			delete(value, "thoughtSignature")
			for _, kind := range []string{"functionCall", "functionResponse"} {
				if call, ok := value[kind].(map[string]any); ok {
					_, hasID := call["id"]
					require.False(t, hasID, "Vertex v1 does not support function IDs; match by scoped ID before encoding")
				}
			}
			parts = append(parts, map[string]any{"role": content.Get("role").String(), "part": value})
		}
	}
	return parts
}

func TestGeminiChatConversationForwardPreservesHistory(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct{ name, messages, parts string }{
		{
			"user parts and remote image",
			`{"role":"user","content":[{"type":"text","text":"before"},{"type":"image_url","image_url":{"url":"https://example.invalid/asset.png","mime_type":"image/png"}},{"type":"text","text":"after"}]}`,
			`[{"role":"user","part":{"text":"before"}},{"role":"user","part":{"fileData":{"fileUri":"https://example.invalid/asset.png","mimeType":"image/png"}}},{"role":"user","part":{"text":"after"}}]`,
		},
		{
			"user inline image and whitespace text",
			`{"role":"user","content":[{"type":"text","text":"before"},{"type":"image_url","image_url":{"url":"data:image/png;base64,aW1hZ2U="}},{"type":"text","text":"  "},{"type":"text","text":"after"}]}`,
			`[{"role":"user","part":{"text":"before"}},{"role":"user","part":{"inlineData":{"mimeType":"image/png","data":"aW1hZ2U="}}},{"role":"user","part":{"text":"  "}},{"role":"user","part":{"text":"after"}}]`,
		},
		{
			"same name parallel reversed results",
			`{"role":"assistant","tool_calls":[{"id":"a","type":"function","function":{"name":"lookup","arguments":"{\"n\":1}"}},{"id":"b","type":"function","function":{"name":"lookup","arguments":"{\"n\":2}"}}]},{"role":"tool","tool_call_id":"b","content":{"value":"second"}},{"role":"tool","tool_call_id":"a","content":{"value":"first"}}`,
			`[{"role":"model","part":{"functionCall":{"name":"lookup","args":{"n":1}}}},{"role":"model","part":{"functionCall":{"name":"lookup","args":{"n":2}}}},{"role":"user","part":{"functionResponse":{"name":"lookup","response":{"value":"first"}}}},{"role":"user","part":{"functionResponse":{"name":"lookup","response":{"value":"second"}}}}]`,
		},
		{
			"same id in separate completed rounds",
			`{"role":"assistant","tool_calls":[{"id":"reuse","type":"function","function":{"name":"first","arguments":"{\"round\":1}"}}]},{"role":"tool","tool_call_id":"reuse","content":{"value":1}},{"role":"assistant","content":"done"},{"role":"user","content":"again"},{"role":"assistant","tool_calls":[{"id":"reuse","type":"function","function":{"name":"second","arguments":"{\"round\":2}"}}]},{"role":"tool","tool_call_id":"reuse","content":{"value":2}}`,
			`[{"role":"model","part":{"functionCall":{"name":"first","args":{"round":1}}}},{"role":"user","part":{"functionResponse":{"name":"first","response":{"value":1}}}},{"role":"model","part":{"text":"done"}},{"role":"user","part":{"text":"again"}},{"role":"model","part":{"functionCall":{"name":"second","args":{"round":2}}}},{"role":"user","part":{"functionResponse":{"name":"second","response":{"value":2}}}}]`,
		},
		{
			"tool response followed by user parts",
			`{"role":"assistant","tool_calls":[{"id":"a","type":"function","function":{"name":"lookup","arguments":"{}"}}]},{"role":"tool","tool_call_id":"a","content":{"value":1}},{"role":"user","content":[{"type":"text","text":"look"},{"type":"image_url","image_url":{"url":"https://example.invalid/asset.png","mime_type":"image/png"}},{"type":"text","text":"next"}]}`,
			`[{"role":"model","part":{"functionCall":{"name":"lookup","args":{}}}},{"role":"user","part":{"functionResponse":{"name":"lookup","response":{"value":1}}}},{"role":"user","part":{"text":"look"}},{"role":"user","part":{"fileData":{"fileUri":"https://example.invalid/asset.png","mimeType":"image/png"}}},{"role":"user","part":{"text":"next"}}]`,
		},
		{
			"user text before results retains its position",
			`{"role":"assistant","tool_calls":[{"id":"a","type":"function","function":{"name":"lookup","arguments":"{\"n\":1}"}},{"id":"b","type":"function","function":{"name":"lookup","arguments":"{\"n\":2}"}}]},{"role":"user","content":"notice"},{"role":"tool","tool_call_id":"b","content":{"value":"second"}},{"role":"tool","tool_call_id":"a","content":{"value":"first"}}`,
			`[{"role":"model","part":{"functionCall":{"name":"lookup","args":{"n":1}}}},{"role":"model","part":{"functionCall":{"name":"lookup","args":{"n":2}}}},{"role":"user","part":{"text":"notice"}},{"role":"user","part":{"functionResponse":{"name":"lookup","response":{"value":"first"}}}},{"role":"user","part":{"functionResponse":{"name":"lookup","response":{"value":"second"}}}}]`,
		},
	} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%t", tc.name, stream), func(t *testing.T) {
				body := conversationForward(t, tc.messages, stream)
				require.Equal(t, conversationJSON(t, tc.parts), conversationParts(t, body))
				if strings.Contains(tc.name, "parallel") {
					contents := gjson.GetBytes(body, "contents").Array()
					require.Len(t, contents, 2)
					require.Len(t, contents[1].Get("parts").Array(), 2, "parallel responses belong in one user Content")
				}
			})
		}
	}
}

func TestGeminiChatConversationForwardPreservesJSONValues(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, value := range []string{`null`, `false`, `42`, `"text"`, `"false"`, `"null"`, `"{\"n\":1}"`, `[1,null,"x"]`, `9007199254740993`, `0.12345678901234567890123456789`, `{"id":9007199254740993,"decimal":0.12345678901234567890123456789}`} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%t", value, stream), func(t *testing.T) {
				messages := fmt.Sprintf(`{"role":"assistant","tool_calls":[{"id":"a","type":"function","function":{"name":"lookup","arguments":"{\"id\":9007199254740993,\"decimal\":0.12345678901234567890123456789}"}}]},{"role":"tool","tool_call_id":"a","content":%s}`, value)
				body := conversationForward(t, messages, stream)
				require.Equal(t, "9007199254740993", gjson.GetBytes(body, "contents.0.parts.0.functionCall.args.id").Raw)
				require.Equal(t, "0.12345678901234567890123456789", gjson.GetBytes(body, "contents.0.parts.0.functionCall.args.decimal").Raw)
				response := gjson.GetBytes(body, "contents.1.parts.0.functionResponse.response")
				want := `{"output":` + value + `}`
				if strings.HasPrefix(value, "{") {
					want = value
				}
				require.Equal(t, conversationJSON(t, want), conversationJSON(t, response.Raw))
			})
		}
	}
}

func TestGeminiChatConversationForwardLargeParallelReverseResults(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const count = 1024
	calls := make([]map[string]any, count)
	for i := range calls {
		calls[i] = map[string]any{"id": fmt.Sprintf("call_%d", i), "type": "function", "function": map[string]any{"name": "lookup", "arguments": fmt.Sprintf(`{"slot":%d}`, i)}}
	}
	encoded, err := json.Marshal(calls)
	require.NoError(t, err)
	messages := []string{`{"role":"assistant","tool_calls":` + string(encoded) + `}`}
	for i := count - 1; i >= 0; i-- {
		messages = append(messages, fmt.Sprintf(`{"role":"tool","tool_call_id":"call_%d","content":{"slot":%d}}`, i, i))
	}
	body := conversationForward(t, strings.Join(messages, ","), false)
	contents := gjson.GetBytes(body, "contents").Array()
	require.Len(t, contents, 2)
	requestParts := contents[0].Get("parts").Array()
	resultParts := contents[1].Get("parts").Array()
	require.Len(t, requestParts, count)
	require.Len(t, resultParts, count)
	for i := 0; i < count; i++ {
		require.EqualValues(t, i, requestParts[i].Get("functionCall.args.slot").Int())
		require.Equal(t, "lookup", resultParts[i].Get("functionResponse.name").String())
		require.EqualValues(t, i, resultParts[i].Get("functionResponse.response.slot").Int())
	}
}

func TestGeminiChatConversationForwardPreservesSignature(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%t", stream), func(t *testing.T) {
			messages := `{"role":"assistant","tool_calls":[{"id":"a","type":"function","extra_content":{"google":{"thought_signature":"c3ludGhldGljLXNpZ25hdHVyZQ=="}},"function":{"name":"lookup","arguments":"{}"}}]},{"role":"tool","tool_call_id":"a","content":"{}"}`
			body := conversationForward(t, messages, stream)
			require.Equal(t, "c3ludGhldGljLXNpZ25hdHVyZQ==", gjson.GetBytes(body, "contents.0.parts.0.thoughtSignature").String())
		})
	}
}

func TestGeminiChatConversationForwardSignatureRoundTrip(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const signature = "c3ludGhldGljLXNpZ25hdHVyZQ=="
	const response = `{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"lookup","args":{"n":1}},"thoughtSignature":"` + signature + `"},{"functionCall":{"name":"lookup","args":{"n":2}}}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":11,"candidatesTokenCount":5,"totalTokenCount":16}}`
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%t", stream), func(t *testing.T) {
			payload, contentType := response, "application/json"
			if stream {
				payload, contentType = "data: "+response+"\n\n", "text/event-stream"
			}
			upstream := &queuedHTTPUpstreamStub{responses: []*http.Response{{StatusCode: 200, Header: http.Header{"Content-Type": []string{contentType}}, Body: io.NopCloser(strings.NewReader(payload))}}}
			svc := &GeminiMessagesCompatService{httpUpstream: upstream, cfg: &config.Config{}}
			body := []byte(fmt.Sprintf(`{"model":"gemini-4-argon","stream":%t,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"two lookups"}]}`, stream))
			c, recorder := newAntigravityCompatContext(http.MethodPost, "/v1/chat/completions", body)
			result, err := svc.ForwardAsChatCompletions(context.Background(), c, geminiCompatThinkingTestAccount(), body)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, 1, upstream.callCount)
			calls := make([]map[string]any, 2)
			for i := range calls {
				calls[i] = map[string]any{"id": "", "type": "function", "function": map[string]any{"name": "", "arguments": ""}}
			}
			merge := func(tc gjson.Result, index int) {
				require.GreaterOrEqual(t, index, 0)
				require.Less(t, index, len(calls))
				call := calls[index]
				if id := tc.Get("id").String(); id != "" {
					call["id"] = id
				}
				fn := call["function"].(map[string]any)
				for _, field := range []string{"name", "arguments"} {
					fn[field] = fn[field].(string) + tc.Get("function."+field).String()
				}
				if extra := tc.Get("extra_content"); extra.Exists() {
					call["extra_content"] = conversationJSON(t, extra.Raw)
				}
			}
			if stream {
				for _, line := range strings.Split(recorder.Body.String(), "\n") {
					if data, ok := strings.CutPrefix(line, "data: "); ok && data != "[DONE]" {
						for _, tc := range gjson.Get(data, "choices.0.delta.tool_calls").Array() {
							merge(tc, int(tc.Get("index").Int()))
						}
					}
				}
			} else {
				toolCalls := gjson.Get(recorder.Body.String(), "choices.0.message.tool_calls").Array()
				require.Len(t, toolCalls, 2)
				for i, tc := range toolCalls {
					merge(tc, i)
				}
			}
			encoded, err := json.Marshal(calls)
			require.NoError(t, err)
			require.Equal(t, signature, gjson.GetBytes(encoded, "0.extra_content.google.thought_signature").String())
			require.False(t, gjson.GetBytes(encoded, "1.extra_content.google.thought_signature").Exists(), "do not copy one call's signature onto another call")
			require.NotEmpty(t, calls[0]["id"])
			require.NotEqual(t, calls[0]["id"], calls[1]["id"])
			messages := fmt.Sprintf(`{"role":"assistant","tool_calls":%s},{"role":"tool","tool_call_id":%q,"content":{"n":1}},{"role":"tool","tool_call_id":%q,"content":{"n":2}}`, encoded, calls[0]["id"], calls[1]["id"])
			outbound := conversationForward(t, messages, stream)
			require.Equal(t, signature, gjson.GetBytes(outbound, "contents.0.parts.0.thoughtSignature").String())
			require.False(t, gjson.GetBytes(outbound, "contents.0.parts.1.thoughtSignature").Exists(), "preserve the original signature-free parallel part")
			require.EqualValues(t, 1, gjson.GetBytes(outbound, "contents.0.parts.0.functionCall.args.n").Int())
			require.EqualValues(t, 2, gjson.GetBytes(outbound, "contents.0.parts.1.functionCall.args.n").Int())
		})
	}
}

func TestGeminiChatConversationForwardRejectsTrailingUpstreamJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const response = `{"candidates":[{"content":{"role":"model","parts":[{"text":"OK"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":11,"candidatesTokenCount":5,"totalTokenCount":16}}`
	for _, tc := range []struct {
		name, tail string
		valid      bool
	}{
		{"second JSON object", `{"error":{"code":"late_error"}}`, false},
		{"trailing garbage", "not-json", false},
		{"trailing whitespace", " \n\t", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := &queuedHTTPUpstreamStub{responses: []*http.Response{{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(response + tc.tail))}}}
			svc := &GeminiMessagesCompatService{httpUpstream: upstream, cfg: &config.Config{}}
			body := []byte(`{"model":"gemini-4-argon","messages":[{"role":"user","content":"hello"}]}`)
			c, recorder := newAntigravityCompatContext(http.MethodPost, "/v1/chat/completions", body)
			result, err := svc.ForwardAsChatCompletions(context.Background(), c, geminiCompatThinkingTestAccount(), body)
			require.Equal(t, 1, upstream.callCount)
			if tc.valid {
				require.NoError(t, err)
				require.NotNil(t, result)
				require.Equal(t, 11, result.Usage.InputTokens)
				require.Equal(t, 5, result.Usage.OutputTokens)
				require.Equal(t, http.StatusOK, recorder.Code)
			} else {
				require.Error(t, err)
				require.Nil(t, result)
				require.Equal(t, http.StatusBadGateway, recorder.Code)
				require.NotContains(t, recorder.Body.String(), "\"content\":\"OK\"")
			}
		})
	}
}

func TestGeminiChatConversationForwardDoesNotBindMalformedCallSignature(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, malformed := range []string{`null`, `"not a call"`, `[]`} {
		t.Run(malformed, func(t *testing.T) {
			payload := `{"candidates":[{"content":{"role":"model","parts":[{"functionCall":` + malformed + `,"thoughtSignature":"YmFk"},{"functionCall":{"name":"lookup","args":{"n":1}},"thoughtSignature":"Z29vZA=="}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":11,"candidatesTokenCount":5,"totalTokenCount":16}}`
			upstream := &queuedHTTPUpstreamStub{responses: []*http.Response{{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(payload))}}}
			svc := &GeminiMessagesCompatService{httpUpstream: upstream, cfg: &config.Config{}}
			body := []byte(`{"model":"gemini-4-argon","messages":[{"role":"user","content":"lookup"}]}`)
			c, recorder := newAntigravityCompatContext(http.MethodPost, "/v1/chat/completions", body)
			result, err := svc.ForwardAsChatCompletions(context.Background(), c, geminiCompatThinkingTestAccount(), body)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, 1, upstream.callCount)
			calls := gjson.Get(recorder.Body.String(), "choices.0.message.tool_calls").Array()
			require.Len(t, calls, 1, "malformed native call cannot create a phantom Chat tool-call slot")
			require.Equal(t, "lookup", calls[0].Get("function.name").String())
			require.NotEmpty(t, calls[0].Get("id").String())
			require.Equal(t, "Z29vZA==", calls[0].Get("extra_content.google.thought_signature").String())
			require.NotContains(t, recorder.Body.String(), "YmFk")
			require.Equal(t, 11, result.Usage.InputTokens)
			require.Equal(t, 5, result.Usage.OutputTokens)
		})
	}
}

func TestGeminiChatConversationForwardRejectsInvalidWithoutDispatch(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const call = `{"role":"assistant","tool_calls":[{"id":"a","type":"function","function":{"name":"lookup","arguments":"{}"}}]}`
	for _, tc := range []struct{ name, messages string }{
		{"orphan result", `{"role":"user","content":"hi"},{"role":"tool","tool_call_id":"missing","content":"{}"}`},
		{"late result after model", call + `,{"role":"user","content":"notice"},{"role":"assistant","content":"working"},{"role":"tool","tool_call_id":"a","content":"{}"}`},
		{"duplicate result", call + `,{"role":"tool","tool_call_id":"a","content":"{}"},{"role":"tool","tool_call_id":"a","content":"{}"}`},
		{"missing parallel result", `{"role":"assistant","tool_calls":[{"id":"a","type":"function","function":{"name":"lookup","arguments":"{}"}},{"id":"b","type":"function","function":{"name":"lookup","arguments":"{}"}}]},{"role":"tool","tool_call_id":"a","content":"{}"}`},
		{"invalid arguments", `{"role":"assistant","tool_calls":[{"id":"a","type":"function","function":{"name":"lookup","arguments":"{not_json}"}}]},{"role":"tool","tool_call_id":"a","content":"{}"}`},
		{"nonobject arguments", `{"role":"assistant","tool_calls":[{"id":"a","type":"function","function":{"name":"lookup","arguments":"[1,2]"}}]},{"role":"tool","tool_call_id":"a","content":"{}"}`},
		{"remote image unknown mime", `{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.invalid/image-without-type"}}]}`},
	} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%t", tc.name, stream), func(t *testing.T) {
				upstream := &queuedHTTPUpstreamStub{responses: []*http.Response{{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"candidates":[{"content":{"parts":[{"text":"OK"}]},"finishReason":"STOP"}]}`))}}}
				svc := &GeminiMessagesCompatService{httpUpstream: upstream, cfg: &config.Config{}}
				body := []byte(fmt.Sprintf(`{"model":"gemini-4-argon","stream":%t,"messages":[%s]}`, stream, tc.messages))
				c, recorder := newAntigravityCompatContext(http.MethodPost, "/v1/chat/completions", body)
				result, err := svc.ForwardAsChatCompletions(context.Background(), c, geminiCompatThinkingTestAccount(), body)
				require.Error(t, err)
				require.Nil(t, result)
				var failover *UpstreamFailoverError
				require.False(t, errors.As(err, &failover))
				require.Zero(t, upstream.callCount)
				require.Equal(t, http.StatusBadRequest, recorder.Code)
				require.Equal(t, "invalid_request_error", gjson.Get(recorder.Body.String(), "error.type").String())
				require.False(t, gjson.Get(recorder.Body.String(), "usage").Exists())
				require.NotContains(t, recorder.Body.String(), "example.invalid")
			})
		}
	}
}
