package service

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestGeminiChatRemoteImageRetainsUserTurn(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := []byte(`{"model":"gemini-4-argon","messages":[{"role":"user","content":"first"},{"role":"assistant","content":"reply"},{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.invalid/picture.png","mime_type":"image/png"}}]}]}`)
	upstream := &queuedHTTPUpstreamStub{responses: []*http.Response{{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":1}}`))}}}
	svc := &GeminiMessagesCompatService{cfg: &config.Config{}, httpUpstream: upstream}
	before := string(body)
	c, recorder := newAntigravityCompatContext(http.MethodPost, "/v1/chat/completions", body)
	result, err := svc.ForwardAsChatCompletions(context.Background(), c, geminiCompatThinkingTestAccount(), body)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 200, recorder.Code)
	require.Len(t, upstream.requestBodies, 1)
	native := upstream.requestBodies[0]
	require.Equal(t, "user", gjson.GetBytes(native, "contents.2.role").String())
	require.Equal(t, "https://example.invalid/picture.png", gjson.GetBytes(native, "contents.2.parts.0.fileData.fileUri").String())
	require.Equal(t, "image/png", gjson.GetBytes(native, "contents.2.parts.0.fileData.mimeType").String())
	require.Equal(t, before, string(body))
	require.Equal(t, 3, result.Usage.InputTokens)
	require.Equal(t, 1, result.Usage.OutputTokens)
}

func TestGeminiChatToolChoiceNativeConfiguration(t *testing.T) {
	for _, tc := range []struct{ name, choice, mode, named string }{
		{"none", `"none"`, "NONE", ""}, {"auto", `"auto"`, "AUTO", ""}, {"required", `"required"`, "ANY", ""}, {"named", `{"type":"function","function":{"name":"lookup"}}`, "ANY", "lookup"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`{"model":"gemini-4-argon","messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}],"tool_choice":` + tc.choice + `}`)
			upstream := &queuedHTTPUpstreamStub{responses: []*http.Response{{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":1}}`))}}}
			svc := &GeminiMessagesCompatService{cfg: &config.Config{}, httpUpstream: upstream}
			c, _ := newAntigravityCompatContext(http.MethodPost, "/v1/chat/completions", body)
			_, err := svc.ForwardAsChatCompletions(context.Background(), c, geminiCompatThinkingTestAccount(), body)
			require.NoError(t, err)
			require.Len(t, upstream.requestBodies, 1)
			require.Equal(t, tc.mode, gjson.GetBytes(upstream.requestBodies[0], "toolConfig.functionCallingConfig.mode").String())
			require.Equal(t, tc.named, gjson.GetBytes(upstream.requestBodies[0], "toolConfig.functionCallingConfig.allowedFunctionNames.0").String())
		})
	}
}

func TestGeminiChatLegacyNameMatchesOnlyUnansweredCalls(t *testing.T) {
	const calls = `{"role":"assistant","tool_calls":[{"id":"one","type":"function","function":{"name":"lookup","arguments":"{}"}},{"id":"two","type":"function","function":{"name":"lookup","arguments":"{}"}}]}`
	for _, tc := range []struct {
		name, messages string
		valid          bool
	}{
		{"ambiguous name", calls + `,{"role":"function","name":"lookup","content":"unknown"}`, false},
		{"one unanswered name", calls + `,{"role":"tool","tool_call_id":"one","content":"first"},{"role":"function","name":"lookup","content":"second"}`, true},
		{"legacy call", `{"role":"assistant","function_call":{"name":"lookup","arguments":"{}"}},{"role":"function","name":"lookup","content":"legacy"}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`{"model":"gemini-4-argon","messages":[` + tc.messages + `]}`)
			upstream := &queuedHTTPUpstreamStub{responses: []*http.Response{{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":1}}`))}}}
			svc := &GeminiMessagesCompatService{cfg: &config.Config{}, httpUpstream: upstream}
			c, recorder := newAntigravityCompatContext(http.MethodPost, "/v1/chat/completions", body)
			result, err := svc.ForwardAsChatCompletions(context.Background(), c, geminiCompatThinkingTestAccount(), body)
			if !tc.valid {
				require.Error(t, err)
				require.Nil(t, result)
				require.Empty(t, upstream.requestBodies)
				require.Equal(t, 400, recorder.Code)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Len(t, upstream.requestBodies, 1)
			if tc.name == "one unanswered name" {
				require.Equal(t, "first", gjson.GetBytes(upstream.requestBodies[0], "contents.1.parts.0.functionResponse.response.output").String())
				require.Equal(t, "second", gjson.GetBytes(upstream.requestBodies[0], "contents.1.parts.1.functionResponse.response.output").String())
			}
		})
	}
}
