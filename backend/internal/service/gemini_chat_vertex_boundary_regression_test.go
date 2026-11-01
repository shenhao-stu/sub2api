package service

import (
	"context"
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

// This is the complete shape of live-vertex-tool/live.py second_body, with an
// isolated signature instead of the real provider signature. The outbound
// boundary follows the controlled Vertex reproduction in CLIProxyAPI #5607.
func TestGeminiChatVertexLiveSecondTurnPreservesBoundary(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%t", stream), func(t *testing.T) {
			const signature = "c3ludGhldGljLW5vdC1hLXByb3ZpZGVyLXNpZ25hdHVyZQ=="
			request := `{"model":"gemini-4-argon","max_tokens":1024,"stream":%t,"stream_options":{"include_usage":true},"tools":[{"type":"function","function":{"name":"read_synthetic_counter","description":"Return a deterministic synthetic counter; no external action.","parameters":{"type":"object","properties":{"label":{"type":"string","enum":["synthetic"]}},"required":["label"],"additionalProperties":false}}}],"tool_choice":"none","messages":[{"role":"user","content":[{"type":"text","text":"Call read_synthetic_counter with label synthetic once."},{"type":"text","text":"Wait for the tool result; do not invent its value."}]},{"role":"assistant","content":null,"tool_calls":[{"id":"synthetic_call","type":"function","function":{"name":"read_synthetic_counter","arguments":"{\"label\":\"synthetic\"}"},"extra_content":{"google":{"thought_signature":"` + signature + `"}}}]},{"role":"tool","tool_call_id":"synthetic_call","content":"{\"label\":\"synthetic\",\"value\":7}"},{"role":"user","content":[{"type":"text","text":"This is only a synthetic compatibility check."},{"type":"text","text":"Reply exactly TOOL_OK_<value>, replacing <value> with the numeric value from the supplied tool result."}]}]}`
			body := []byte(fmt.Sprintf(request, stream))
			const response = `{"candidates":[{"content":{"role":"model","parts":[{"text":"TOOL_OK_7"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":11,"candidatesTokenCount":5,"totalTokenCount":16}}`
			payload, contentType := response, "application/json"
			if stream {
				payload, contentType = "data: "+response+"\n\n", "text/event-stream"
			}
			upstream := &queuedHTTPUpstreamStub{responses: []*http.Response{{StatusCode: 200, Header: http.Header{"Content-Type": []string{contentType}}, Body: io.NopCloser(strings.NewReader(payload))}}}
			upstream.onCall = func(req *http.Request, _ *queuedHTTPUpstreamStub) {
				require.Equal(t, "aiplatform.googleapis.com", req.URL.Host)
				require.True(t, strings.HasPrefix(req.URL.Path, "/v1/"))
			}
			svc := &GeminiMessagesCompatService{httpUpstream: upstream, cfg: &config.Config{}}
			account := &Account{ID: 811, Platform: PlatformGemini, Type: AccountTypeServiceAccount,
				Credentials: map[string]any{"auth_mode": "apikey", "api_key": "isolated-test-key", "location": "global", "tier_id": "vertex"}, Concurrency: 1}
			c, recorder := newAntigravityCompatContext(http.MethodPost, "/v1/chat/completions", body)
			result, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, 200, recorder.Code)
			require.Equal(t, 1, upstream.callCount)
			require.Equal(t, 11, result.Usage.InputTokens)
			require.Equal(t, 5, result.Usage.OutputTokens)
			native := upstream.requestBodies[0]
			contents := gjson.GetBytes(native, "contents").Array()
			require.Len(t, contents, 4, "trailing user text must not share the functionResponse Content")
			require.Equal(t, []string{"user", "model", "user", "user"}, []string{contents[0].Get("role").String(), contents[1].Get("role").String(), contents[2].Get("role").String(), contents[3].Get("role").String()})
			require.Len(t, contents[0].Get("parts").Array(), 2)
			require.Equal(t, signature, contents[1].Get("parts.0.thoughtSignature").String())
			require.Len(t, contents[2].Get("parts").Array(), 1)
			require.Equal(t, "read_synthetic_counter", contents[2].Get("parts.0.functionResponse.name").String())
			require.Equal(t, `{"label":"synthetic","value":7}`, contents[2].Get("parts.0.functionResponse.response.output").String())
			require.Equal(t, "This is only a synthetic compatibility check.", contents[3].Get("parts.0.text").String())
			require.Equal(t, "Reply exactly TOOL_OK_<value>, replacing <value> with the numeric value from the supplied tool result.", contents[3].Get("parts.1.text").String())
			require.Equal(t, "NONE", gjson.GetBytes(native, "toolConfig.functionCallingConfig.mode").String())
			require.Equal(t, "read_synthetic_counter", gjson.GetBytes(native, "tools.0.functionDeclarations.0.name").String())
		})
	}
}

func TestGeminiChatVertexFunctionResponseBoundaries(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const call = `{"role":"assistant","tool_calls":[{"id":"a","type":"function","function":{"name":"lookup","arguments":"{}"}}]}`
	const tool = `{"role":"tool","tool_call_id":"a","content":{"value":7}}`
	for _, tc := range []struct {
		name, messages string
		groups         []string
	}{
		{"trailing text", call + `,` + tool + `,{"role":"user","content":"after"}`, []string{"C", "R", "T"}},
		{"leading text", call + `,{"role":"user","content":"before"},` + tool, []string{"C", "TR"}},
		{"text on both sides", call + `,{"role":"user","content":"before"},` + tool + `,{"role":"user","content":"after"}`, []string{"C", "TR", "T"}},
		{"trailing remote image", call + `,` + tool + `,{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.invalid/image.png","mime_type":"image/png"}}]}`, []string{"C", "R", "F"}},
		{"trailing inline image", call + `,` + tool + `,{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,aW1hZ2U="}}]}`, []string{"C", "R", "I"}},
		{"past turn is preserved", call + `,` + tool + `,{"role":"user","content":"after"},{"role":"assistant","content":"done"},{"role":"user","content":"next"}`, []string{"C", "R", "T", "T", "T"}},
		{"reverse parallel plus trailing text", `{"role":"assistant","tool_calls":[{"id":"a","type":"function","function":{"name":"lookup","arguments":"{\"n\":1}"}},{"id":"b","type":"function","function":{"name":"lookup","arguments":"{\"n\":2}"}}]},{"role":"tool","tool_call_id":"b","content":{"n":2}},{"role":"tool","tool_call_id":"a","content":{"n":1}},{"role":"user","content":"after"}`, []string{"CC", "RR", "T"}},
	} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%t", tc.name, stream), func(t *testing.T) {
				native := conversationForward(t, tc.messages, stream)
				var groups []string
				for _, content := range gjson.GetBytes(native, "contents").Array() {
					var types strings.Builder
					seenResponse := false
					for _, part := range content.Get("parts").Array() {
						kind := ""
						for _, entry := range []struct{ field, code string }{{"functionCall", "C"}, {"functionResponse", "R"}, {"text", "T"}, {"fileData", "F"}, {"inlineData", "I"}} {
							if part.Get(entry.field).Exists() {
								kind = entry.code
							}
						}
						require.NotEmpty(t, kind)
						if content.Get("role").String() == "user" {
							require.False(t, seenResponse && kind != "R", "no text/media trails a response in the same user Content")
							seenResponse = seenResponse || kind == "R"
						}
						types.WriteString(kind)
					}
					groups = append(groups, types.String())
				}
				require.Equal(t, tc.groups, groups)
				if strings.HasPrefix(tc.name, "reverse parallel") {
					require.EqualValues(t, 1, gjson.GetBytes(native, "contents.1.parts.0.functionResponse.response.n").Int())
					require.EqualValues(t, 2, gjson.GetBytes(native, "contents.1.parts.1.functionResponse.response.n").Int())
				}
			})
		}
	}
}
