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

func TestGeminiChatFinalTurnConversion(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const history = `{"role":"user","content":"hello"},{"role":"assistant","content":"reply"},`
	const call = `{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{}"}}]}`
	for _, tc := range []struct {
		name, messages, wantRole string
	}{
		{"user text", history + `{"role":"user","content":"next"}`, "user"},
		{"user text parts", history + `{"role":"user","content":[{"type":"text","text":"next"}]}`, "user"},
		{"user data image", history + `{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,aW1hZ2U="}}]}`, "user"},
		{"matched tool result", call + `,{"role":"tool","tool_call_id":"call_1","content":"found"}`, "user"},
		{"empty tool result", call + `,{"role":"tool","tool_call_id":"call_1","content":""}`, "user"},
		{"tool result followed by user text", call + `,{"role":"tool","tool_call_id":"call_1","content":"found"},{"role":"user","content":"continue"}`, "user"},
		{"parallel tools in reverse result order", `{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{}"}},{"id":"call_2","type":"function","function":{"name":"read","arguments":"{}"}}]},{"role":"tool","tool_call_id":"call_2","content":"second"},{"role":"tool","tool_call_id":"call_1","content":"first"}`, "user"},
		{"empty assistant", `{"role":"user","content":"hello"},{"role":"assistant","content":""}`, "user"},
		{"blank assistant", `{"role":"user","content":"hello"},{"role":"assistant","content":"   "}`, "user"},
		{"reasoning only assistant", `{"role":"user","content":"hello"},{"role":"assistant","content":[{"type":"thinking","thinking":"private"}]}`, "user"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := &queuedHTTPUpstreamStub{responses: []*http.Response{{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":1}}`)),
			}}}
			svc := &GeminiMessagesCompatService{httpUpstream: upstream, cfg: &config.Config{}}
			body := []byte(fmt.Sprintf(`{"model":"gemini-4-argon","messages":[%s]}`, tc.messages))
			c, recorder := newAntigravityCompatContext(http.MethodPost, "/v1/chat/completions", body)
			result, err := svc.ForwardAsChatCompletions(context.Background(), c, geminiCompatThinkingTestAccount(), body)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, 3, result.Usage.InputTokens)
			require.Equal(t, 1, result.Usage.OutputTokens)
			require.Equal(t, http.StatusOK, recorder.Code)
			require.Len(t, upstream.requestBodies, 1)
			contents := gjson.GetBytes(upstream.requestBodies[0], "contents").Array()
			require.NotEmpty(t, contents)
			roles := make([]string, len(contents))
			for i, content := range contents {
				roles[i] = content.Get("role").String()
			}
			t.Logf("outbound roles: %v", roles)
			require.Equal(t, tc.wantRole, contents[len(contents)-1].Get("role").String())
			if strings.Contains(tc.name, "tool") {
				var calls, results int
				for _, content := range contents {
					for _, part := range content.Get("parts").Array() {
						if part.Get("functionCall").Exists() {
							calls++
							require.Equal(t, geminiDummyThoughtSignature, part.Get("thoughtSignature").String())
						}
						if part.Get("functionResponse").Exists() {
							results++
						}
					}
				}
				wantCalls := 1
				if strings.HasPrefix(tc.name, "parallel") {
					wantCalls = 2
				}
				require.Equal(t, wantCalls, calls)
				require.Equal(t, wantCalls, results)
			}
			if tc.name == "user data image" {
				require.Equal(t, "aW1hZ2U=", contents[len(contents)-1].Get("parts.0.inlineData.data").String())
			}
		})
	}
}

func TestGeminiChatFinalTurnValidation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const history = `{"role":"user","content":"hello"},{"role":"assistant","content":"reply"},`
	const call = `{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{}"}}]}`
	for _, tc := range []struct{ name, messages, code, param string }{
		{"prefill text", `{"role":"user","content":"hello"},{"role":"assistant","content":"prefix"}`, "unsupported_assistant_prefill", "messages[1].content"},
		{"prefill parts", `{"role":"user","content":"hello"},{"role":"assistant","content":[{"type":"text","text":"prefix"}]}`, "unsupported_assistant_prefill", "messages[1].content"},
		{"remote image", history + `{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.invalid/private-image.png"}}]}`, "unsupported_conversation_turn", "messages[2]"},
		{"late tool result", call + `,{"role":"user","content":"notice"},{"role":"assistant","content":"working"},{"role":"tool","tool_call_id":"call_1","content":"found"}`, "unsupported_conversation_turn", "messages[3]"},
		{"empty user", history + `{"role":"user","content":""}`, "unsupported_conversation_turn", "messages[2]"},
		{"blank user", history + `{"role":"user","content":"   "}`, "unsupported_conversation_turn", "messages[2]"},
	} {
		for _, mode := range []string{"json", "stream", "committed stream"} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				upstream := &queuedHTTPUpstreamStub{responses: []*http.Response{{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":1}}`))}}}
				svc := &GeminiMessagesCompatService{httpUpstream: upstream, cfg: &config.Config{}}
				body := []byte(fmt.Sprintf(`{"model":"public-gemini","stream":%t,"messages":[%s]}`, mode != "json", tc.messages))
				account := geminiCompatThinkingTestAccount()
				account.Credentials["model_mapping"] = map[string]any{"public-gemini": "gemini-4-argon"}
				before, err := json.Marshal(account)
				require.NoError(t, err)
				c, recorder := newAntigravityCompatContext(http.MethodPost, "/v1/chat/completions", body)
				if mode == "committed stream" {
					c.Header("Content-Type", "text/event-stream")
					_, err := c.Writer.WriteString(": keepalive\n\n")
					require.NoError(t, err)
					c.Writer.Flush()
				}
				result, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body)
				require.Error(t, err)
				require.Nil(t, result)
				var failover *UpstreamFailoverError
				require.False(t, errors.As(err, &failover))
				require.Empty(t, upstream.requestBodies)
				after, marshalErr := json.Marshal(account)
				require.NoError(t, marshalErr)
				require.JSONEq(t, string(before), string(after))
				response := recorder.Body.String()
				if mode == "committed stream" {
					require.Equal(t, http.StatusOK, recorder.Code)
					require.Equal(t, 1, strings.Count(response, "data: "))
					response = strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(response, ": keepalive\n\n"), "data: "))
				} else {
					require.Equal(t, http.StatusBadRequest, recorder.Code)
				}
				require.True(t, gjson.Valid(response))
				require.Equal(t, tc.code, gjson.Get(response, "error.code").String())
				require.Equal(t, tc.param, gjson.Get(response, "error.param").String())
				require.EqualValues(t, http.StatusBadRequest, gjson.Get(response, "error.status_code").Int())
				require.False(t, gjson.Get(response, "usage").Exists())
				require.NotContains(t, response, "private-image")
				require.NotContains(t, response, "prefix")
			})
		}
	}
}

func TestGeminiChatFinalTurnMappedModelCapability(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		model  string
		reject bool
	}{
		{"gemini-4-argon", true}, {"gemini-3.8-flash", true}, {" models/GEMINI-4-ARGON ", true},
		{"gemini-3.5-flash", false}, {"gemini-2.5-pro", false}, {"gemini-future", false},
	} {
		t.Run(tc.model, func(t *testing.T) {
			upstream := &queuedHTTPUpstreamStub{responses: []*http.Response{{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":1}}`))}}}
			svc := &GeminiMessagesCompatService{httpUpstream: upstream, cfg: &config.Config{}}
			body := []byte(`{"model":"gemini-4-argon","messages":[{"role":"user","content":"hello"},{"role":"assistant","content":"prefix"}]}`)
			account := geminiCompatThinkingTestAccount()
			account.Credentials["model_mapping"] = map[string]any{"gemini-4-argon": tc.model}
			c, recorder := newAntigravityCompatContext(http.MethodPost, "/v1/chat/completions", body)
			result, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body)
			if tc.reject {
				require.Error(t, err)
				require.Nil(t, result)
				require.Empty(t, upstream.requestBodies)
				require.Equal(t, http.StatusBadRequest, recorder.Code)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
				require.Len(t, upstream.requestBodies, 1)
				require.Equal(t, http.StatusOK, recorder.Code)
			}
		})
	}
}
