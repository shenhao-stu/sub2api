//go:build unit

package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestMonitorCommandCodeGoDoesNotDiscardUnsupportedSemantics(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cases := []struct{ name, path, patch string }{
		{"compact", "/v1/responses/compact", `{}`},
		{"previous_response", "/v1/responses", `{"previous_response_id":"resp_previous"}`},
		{"persistent_store", "/v1/responses", `{"store":true}`},
		{"background", "/v1/responses", `{"background":true}`},
		{"prompt_cache", "/v1/responses", `{"prompt_cache_key":"session-cache"}`},
		{"reasoning_summary", "/v1/responses", `{"reasoning":{"effort":"medium","summary":"detailed"}}`},
		{"verbosity", "/v1/responses", `{"text":{"verbosity":"low"}}`},
		{"server_tool", "/v1/responses", `{"tools":[{"type":"web_search_preview"}],"tool_choice":"required"}`},
		{"strict_tool", "/v1/responses", `{"tools":[{"type":"function","name":"f","parameters":{"type":"object"},"strict":true}]}`},
		{"response_format", "/v1/responses", `{"text":{"format":{"type":"json_object"}}}`},
		{"stop_sequences", "/v1/messages", `{"stop_sequences":["END"]}`},
		{"system_cache_control", "/v1/messages", `{"system":[{"type":"text","text":"instructions","cache_control":{"type":"ephemeral"}}]}`},
		{"server_tool_messages", "/v1/messages", `{"tools":[{"type":"web_search_20250305","name":"web_search"}],"tool_choice":{"type":"any"}}`},
		{"disable_parallel", "/v1/messages", `{"tools":[{"name":"f","input_schema":{"type":"object"}}],"tool_choice":{"type":"auto","disable_parallel_tool_use":true}}`},
		{"output_format", "/v1/messages", `{"output_config":{"format":{"type":"json_schema","schema":{"type":"object"}}}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := map[string]any{"model": "glm-5.3-flash", "input": "hello"}
			if tc.path == "/v1/messages" {
				input = map[string]any{"model": "glm-5.3-flash", "max_tokens": 128, "messages": []any{map[string]any{"role": "user", "content": "hello"}}}
			}
			var patch map[string]any
			require.NoError(t, json.Unmarshal([]byte(tc.patch), &patch))
			for k, v := range patch {
				input[k] = v
			}
			body, err := json.Marshal(input)
			require.NoError(t, err)
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, tc.path, bytes.NewReader(body))
			wire := `{"type":"text-delta","text":"wrong-success"}` + "\n" + `{"type":"finish","finishReason":"stop","usage":{"inputTokens":3,"outputTokens":2}}` + "\n"
			upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(wire))}}
			svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig(), httpUpstream: upstream}
			account := commandCodePolicyTestAccount(true)
			if tc.path == "/v1/messages" {
				_, err = svc.ForwardAsAnthropic(context.Background(), c, account, body, "", "")
			} else {
				_, err = svc.Forward(context.Background(), c, account, body)
			}
			require.Error(t, err)
			require.Equal(t, http.StatusBadRequest, rec.Code)
			require.Nil(t, upstream.lastReq, "functional input must not disappear before the native adapter validates it")
		})
	}
}

func TestMonitorCommandCodeGoRejectsZDRHeaderOverrides(t *testing.T) {
	for _, spelling := range []string{"X-Cmd-Zdr", "x-cmd-zdr", "X-CMD-ZDR"} {
		req, err := http.NewRequest(http.MethodPost, CommandCodeGoBaseURL+"/alpha/generate", nil)
		require.NoError(t, err)
		req.Header[spelling] = []string{"1"}
		require.Error(t, prepareCommandCodeRequest(req, commandCodePolicyTestAccount(true)), spelling)
	}
}

func TestMonitorCommandCodeGoBridgePreservesCoreInputs(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for protocol, input := range map[string]string{
		"responses": `{"model":"glm-5.3-flash","instructions":"system text","input":[{"role":"user","content":[{"type":"input_text","text":"see image"},{"type":"input_image","image_url":"data:image/png;base64,aA=="}]},{"type":"function_call","call_id":"call-1","name":"lookup","arguments":"{\"q\":\"x\"}"},{"type":"function_call_output","call_id":"call-1","output":"verified result"}],"tools":[{"type":"function","name":"lookup","parameters":{"type":"object"},"strict":false}],"tool_choice":"auto","reasoning":{"effort":"high"},"store":false}`,
		"messages":  `{"model":"glm-5.3-flash","max_tokens":256,"system":"system text","messages":[{"role":"user","content":[{"type":"text","text":"see image"},{"type":"image","source":{"type":"base64","media_type":"image/png","data":"aA=="}}]},{"role":"assistant","content":[{"type":"tool_use","id":"call-1","name":"lookup","input":{"q":"x"}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"call-1","content":"verified result"}]}],"tools":[{"name":"lookup","input_schema":{"type":"object"}}],"tool_choice":{"type":"auto"},"output_config":{"effort":"high"}}`,
	} {
		t.Run(protocol, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/"+protocol, strings.NewReader(input))
			wire := `{"type":"text-delta","text":"ok"}` + "\n" + `{"type":"finish","finishReason":"stop","usage":{"inputTokens":3,"outputTokens":2}}` + "\n"
			upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(wire))}}
			svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig(), httpUpstream: upstream}
			account := commandCodePolicyTestAccount(true)
			var err error
			if protocol == "responses" {
				_, err = svc.Forward(context.Background(), c, account, []byte(input))
			} else {
				_, err = svc.ForwardAsAnthropic(context.Background(), c, account, []byte(input), "", "")
			}
			require.NoError(t, err, rec.Body.String())
			for _, want := range []string{`"toolCallId":"call-1"`, `"toolName":"lookup"`, `"value":"verified result"`, `"image":"data:image/png;base64,aA=="`, `"reasoning_effort":"high"`, "system text"} {
				require.Contains(t, string(upstream.lastBody), want)
			}
		})
	}
}

func TestMonitorCommandCodeGoMessagesPreservesTemperatureAcrossModelAlias(t *testing.T) {
	input := `{"model":"gpt-5-alias","max_tokens":128,"temperature":0.25,"messages":[{"role":"user","content":"hello"}]}`
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(input))
	wire := `{"type":"finish","finishReason":"stop","usage":{"inputTokens":3,"outputTokens":2}}` + "\n"
	upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(wire))}}
	svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig(), httpUpstream: upstream}
	account := commandCodePolicyTestAccount(true)
	account.Credentials["model_mapping"] = map[string]any{"gpt-5-alias": "glm-5.3-flash"}
	_, err := svc.ForwardAsAnthropic(context.Background(), c, account, []byte(input), "", "")
	require.NoError(t, err, rec.Body.String())
	var native struct {
		Params struct {
			Model       string   `json:"model"`
			Temperature *float64 `json:"temperature"`
		} `json:"params"`
	}
	require.NoError(t, json.Unmarshal(upstream.lastBody, &native))
	require.Equal(t, "glm-5.3-flash", native.Params.Model)
	require.NotNil(t, native.Params.Temperature)
	require.Equal(t, 0.25, *native.Params.Temperature)
}

func TestMonitorCommandCodeGoRejectsResponsesShapeOnChatURL(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"glm-5.3-flash","input":"must not be discarded"}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	upstream := &httpUpstreamRecorder{}
	svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig(), httpUpstream: upstream}
	_, err := svc.ForwardAsChatCompletions(context.Background(), c, commandCodePolicyTestAccount(true), body, "", "")
	require.Error(t, err)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Nil(t, upstream.lastReq)
}

func TestMonitorCommandCodeGoClientPolicyFailsBeforeTransport(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for name, hdr := range map[string]string{"zdr_required": "1", "zdr_invalid": "anything"} {
		t.Run(name, func(t *testing.T) {
			input := []byte(`{"model":"glm-5.3-flash","messages":[{"role":"user","content":"hello"}]}`)
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(input))
			c.Request.Header.Set("x-cmd-zdr", hdr)
			upstream := &httpUpstreamRecorder{}
			svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig(), httpUpstream: upstream}
			_, err := svc.ForwardAsChatCompletions(context.Background(), c, commandCodePolicyTestAccount(true), input, "", "")
			require.Error(t, err)
			require.Equal(t, http.StatusBadRequest, rec.Code)
			require.Nil(t, upstream.lastReq)
		})
	}
}

func TestMonitorCommandCodeFailuresRetainCacheCreation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, endpoint := range []string{"chat", "responses", "messages"} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%t", endpoint, stream), func(t *testing.T) {
				input := fmt.Sprintf(`{"model":"glm-5.3-flash","messages":[{"role":"user","content":"hello"}],"stream":%t}`, stream)
				path := "/v1/chat/completions"
				if endpoint == "responses" {
					path = "/v1/responses"
					input = fmt.Sprintf(`{"model":"glm-5.3-flash","input":"hello","stream":%t}`, stream)
				} else if endpoint == "messages" {
					path = "/v1/messages"
					input = fmt.Sprintf(`{"model":"glm-5.3-flash","messages":[{"role":"user","content":"hello"}],"max_tokens":128,"stream":%t}`, stream)
				}
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPost, path, strings.NewReader(input))
				wire := `{"type":"text-delta","text":"partial"}` + "\n" +
					`{"type":"finish","finishReason":"stop","totalUsage":{"inputTokens":100,"outputTokens":10,"inputTokenDetails":{"cacheReadTokens":60,"cacheWriteTokens":20},"outputTokenDetails":{"reasoningTokens":3}}}` + "\n" +
					`{"type":"error","message":"private-upstream-error"}` + "\n"
				upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(wire))}}
				svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig(), httpUpstream: upstream}
				account := commandCodePolicyTestAccount(true)
				var result *OpenAIForwardResult
				var err error
				switch endpoint {
				case "responses":
					result, err = svc.Forward(context.Background(), c, account, []byte(input))
				case "messages":
					result, err = svc.ForwardAsAnthropic(context.Background(), c, account, []byte(input), "", "")
				default:
					result, err = svc.ForwardAsChatCompletions(context.Background(), c, account, []byte(input), "", "")
				}
				require.Error(t, err)
				var retry *UpstreamFailoverError
				require.NotErrorAs(t, err, &retry)
				require.NotNil(t, result)
				require.Equal(t, 100, result.Usage.InputTokens)
				require.Equal(t, 60, result.Usage.CacheReadInputTokens)
				require.Equal(t, 20, result.Usage.CacheCreationInputTokens)
				require.Equal(t, 10, result.Usage.OutputTokens)
				require.NotContains(t, rec.Body.String(), "private-upstream-error")
				require.NotContains(t, rec.Body.String(), "response.completed")
				require.NotContains(t, rec.Body.String(), "data: [DONE]")
			})
		}
	}
}

type commandCodeMonitorReadFailure struct{}

func (commandCodeMonitorReadFailure) Read([]byte) (int, error) {
	return 0, errors.New("private-upstream-read-error")
}

func TestMonitorCommandCodePartialJSONWithoutUsageDoesNotReplay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, endpoint := range []string{"chat", "responses", "messages"} {
		for _, failure := range []string{"eof", "error_event", "read_error"} {
			t.Run(endpoint+"/"+failure, func(t *testing.T) {
				input := `{"model":"glm-5.3-flash","messages":[{"role":"user","content":"hello"}],"stream":false}`
				path := "/v1/chat/completions"
				if endpoint == "responses" {
					path = "/v1/responses"
					input = `{"model":"glm-5.3-flash","input":"hello","stream":false}`
				} else if endpoint == "messages" {
					path = "/v1/messages"
					input = `{"model":"glm-5.3-flash","messages":[{"role":"user","content":"hello"}],"max_tokens":128,"stream":false}`
				}
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPost, path, strings.NewReader(input))
				partial := `{"type":"text-delta","text":"private-partial-answer"}` + "\n"
				var reader io.Reader = strings.NewReader(partial)
				switch failure {
				case "error_event":
					reader = strings.NewReader(partial + `{"type":"error","message":"private-upstream-error"}` + "\n")
				case "read_error":
					reader = io.MultiReader(reader, commandCodeMonitorReadFailure{})
				}
				upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(reader)}}
				svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig(), httpUpstream: upstream}
				account := commandCodePolicyTestAccount(true)
				var result *OpenAIForwardResult
				var err error
				switch endpoint {
				case "responses":
					result, err = svc.Forward(context.Background(), c, account, []byte(input))
				case "messages":
					result, err = svc.ForwardAsAnthropic(context.Background(), c, account, []byte(input), "", "")
				default:
					result, err = svc.ForwardAsChatCompletions(context.Background(), c, account, []byte(input), "", "")
				}
				require.Error(t, err)
				var retry *UpstreamFailoverError
				require.NotErrorAs(t, err, &retry)
				require.Nil(t, result, "no authoritative usage was received")
				require.Equal(t, http.StatusBadGateway, rec.Code)
				require.Len(t, upstream.requests, 1)
				require.NotContains(t, rec.Body.String(), "private-")
				require.NotContains(t, err.Error(), "private-")
				require.NotContains(t, rec.Body.String(), "response.completed")
				require.NotContains(t, rec.Body.String(), "data: [DONE]")
			})
		}
	}
}
