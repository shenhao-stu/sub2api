//go:build unit

package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func commandCodeTestAccount(goMode bool) *Account {
	provider, base := CommandCodeProvider, CommandCodeBaseURL
	if goMode {
		provider, base = CommandCodeGoProvider, CommandCodeGoBaseURL
	}
	return &Account{ID: 1001, Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
		Credentials: map[string]any{"api_key": "upstream-only-secret", "base_url": base},
		Extra:       map[string]any{"provider": provider},
	}
}

func TestCommandCodeCredentialBoundary(t *testing.T) {
	account := commandCodeTestAccount(false)
	for _, target := range []string{
		"https://evil.example/provider/v1/responses", "http://api.commandcode.ai/provider/v1/responses",
		"https://api.commandcode.ai.evil.example/provider/v1/responses", "https://key@api.commandcode.ai/provider/v1/responses",
		"https://api.commandcode.ai/provider/v1/../internal/export", "https://api.commandcode.ai:8443/provider/v1/responses",
		"https://api.commandcode.ai/alpha/generate",
	} {
		req, err := http.NewRequest(http.MethodPost, target, nil)
		require.NoError(t, err)
		require.Error(t, prepareCommandCodeRequest(req, account), target)
	}
	req, err := http.NewRequest(http.MethodPost, CommandCodeBaseURL+"/v1/responses", nil)
	require.NoError(t, err)
	req.Header["authorization"] = []string{"Bearer incoming-secret"}
	req.Header["cookie"] = []string{"private-session"}
	req.Header["X-Api-Key"] = []string{"incoming-key"}
	require.NoError(t, prepareCommandCodeRequest(req, account))
	require.True(t, HTTPUpstreamRedirectsDisabled(req.Context()))
	require.Equal(t, "Bearer upstream-only-secret", req.Header.Get("Authorization"))
	require.Empty(t, req.Header.Get("Cookie"))
	require.Empty(t, req.Header.Get("X-Api-Key"))
	require.NotContains(t, fmt.Sprint(req.Header), "incoming-secret")
}

func TestCommandCodeAccountPolicy(t *testing.T) {
	for _, mutate := range []func(*Account){
		func(a *Account) { a.Credentials["base_url"] = "https://evil.example" },
		func(a *Account) { a.Credentials["api_key"] = "" },
		func(a *Account) { a.Type = AccountTypeOAuth },
		func(a *Account) { a.Platform = PlatformGrok },
		func(a *Account) { a.Credentials["pool_mode"] = true },
		func(a *Account) { a.Extra["commandcode_zdr"] = "true" },
	} {
		a := commandCodeTestAccount(false)
		mutate(a)
		require.Error(t, ValidateCommandCodeAccount(a))
	}
	a := commandCodeTestAccount(true)
	a.Extra["openai_responses_supported"] = true
	a.Extra["openai_responses_mode"] = "force_responses"
	a.Extra["openai_passthrough"] = true
	a.Extra["openai_apikey_responses_websockets_v2_enabled"] = true
	require.True(t, shouldForwardOpenAIResponsesViaRawChatCompletions(a))
	require.False(t, a.IsOpenAIPassthroughEnabled())
	require.False(t, a.IsOpenAIResponsesWebSocketV2Enabled())
	require.Equal(t, OpenAIWSIngressModeOff, a.ResolveOpenAIResponsesWebSocketV2Mode("ctx_pool"))
	a.Extra["commandcode_zdr"] = true
	require.Error(t, ValidateCommandCodeAccount(a))
}

func TestCommandCodeGoGatewayContracts(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, endpoint := range []string{"chat", "responses", "messages"} {
		for _, stream := range []bool{false, true} {
			for _, broken := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/stream=%t/broken=%t", endpoint, stream, broken), func(t *testing.T) {
					input := fmt.Sprintf(`{"model":"glm-5.3-flash","messages":[{"role":"user","content":"hello"}],"stream":%t}`, stream)
					path := "/v1/chat/completions"
					if endpoint == "responses" {
						path = "/v1/responses"
						input = fmt.Sprintf(`{"model":"glm-5.3-flash","input":"hello","stream":%t}`, stream)
					} else if endpoint == "messages" {
						path = "/v1/messages"
						input = fmt.Sprintf(`{"model":"glm-5.3-flash","max_tokens":128,"messages":[{"role":"user","content":"hello"}],"stream":%t}`, stream)
					}
					rec := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(rec)
					c.Request = httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(input))
					c.Request.Header.Set("Authorization", "Bearer caller-secret")
					wire := "data: {\"type\":\"text-delta\",\"text\":\"hello\"}\n\n" +
						"data: {\"type\":\"finish\",\"finishReason\":\"stop\",\"totalUsage\":{\"inputTokens\":20,\"outputTokens\":3,\"cachedInputTokens\":7}}\n\n"
					if broken {
						wire += "data: {\"type\":\"error\",\"message\":\"private-upstream-detail\"}\n\n"
					}
					upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(wire))}}
					svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig(), httpUpstream: upstream}
					account := commandCodeTestAccount(true)
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
					if broken {
						require.Error(t, err)
						var retry *UpstreamFailoverError
						require.NotErrorAs(t, err, &retry)
						require.NotContains(t, rec.Body.String(), "response.completed")
						require.NotContains(t, rec.Body.String(), "private-upstream-detail")
					} else {
						require.NoError(t, err)
						require.Contains(t, rec.Body.String(), "hello")
					}
					require.NotNil(t, result)
					require.Equal(t, 20, result.Usage.InputTokens)
					require.Equal(t, 3, result.Usage.OutputTokens)
					require.Equal(t, 7, result.Usage.CacheReadInputTokens)
					require.Equal(t, CommandCodeGoBaseURL+"/alpha/generate", upstream.lastReq.URL.String())
					require.True(t, HTTPUpstreamRedirectsDisabled(upstream.lastReq.Context()))
					require.Equal(t, "Bearer upstream-only-secret", upstream.lastReq.Header.Get("Authorization"))
					require.Equal(t, "/alpha/generate", GetActualOpenAIUpstreamEndpoint(c))
				})
			}
		}
	}
}

func TestCommandCodeProviderOpenAIProtocols(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, endpoint := range []string{"chat/completions", "responses"} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%t", endpoint, stream), func(t *testing.T) {
				input := fmt.Sprintf(`{"model":"gpt-5.4","messages":[{"role":"user","content":"hello"}],"stream":%t}`, stream)
				wire := `{"id":"cc_1","object":"chat.completion","model":"gpt-5.4","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":20,"completion_tokens":3,"total_tokens":23,"prompt_tokens_details":{"cached_tokens":7}}}`
				if stream {
					wire = "data: " + strings.Replace(wire, `"message":{"role":"assistant","content":"hello"}`, `"delta":{"role":"assistant","content":"hello"}`, 1) + "\n\ndata: [DONE]\n\n"
				}
				if endpoint == "responses" {
					input = fmt.Sprintf(`{"model":"gpt-5.4","input":"hello","stream":%t}`, stream)
					wire = `{"id":"resp_1","object":"response","model":"gpt-5.4","status":"completed","output":[{"id":"msg_1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"hello"}]}],"usage":{"input_tokens":20,"output_tokens":3,"total_tokens":23,"input_tokens_details":{"cached_tokens":7}}}`
					if stream {
						wire = "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":" + wire + "}\n\n"
					}
				}
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/"+endpoint, strings.NewReader(input))
				c.Request.Header.Set("Authorization", "Bearer client-secret")
				c.Request.Header.Set("x-cmd-zdr", "1")
				contentType := "application/json"
				if stream {
					contentType = "text/event-stream"
				}
				upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(wire))}}
				svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig(), httpUpstream: upstream}
				account := commandCodeTestAccount(false)
				account.Extra["openai_responses_supported"] = true
				var result *OpenAIForwardResult
				var err error
				if endpoint == "responses" {
					result, err = svc.Forward(context.Background(), c, account, []byte(input))
				} else {
					result, err = svc.ForwardAsChatCompletions(context.Background(), c, account, []byte(input), "", "")
				}
				require.NoError(t, err)
				require.NotNil(t, result)
				require.Equal(t, 20, result.Usage.InputTokens)
				require.Equal(t, 3, result.Usage.OutputTokens)
				require.Equal(t, 7, result.Usage.CacheReadInputTokens)
				require.Equal(t, CommandCodeBaseURL+"/v1/"+endpoint, upstream.lastReq.URL.String())
				require.Equal(t, "Bearer upstream-only-secret", upstream.lastReq.Header.Get("Authorization"))
				require.Equal(t, "1", upstream.lastReq.Header.Get("X-Cmd-Zdr"))
				require.True(t, HTTPUpstreamRedirectsDisabled(upstream.lastReq.Context()))
			})
		}
	}
}

func TestCommandCodeProviderAnthropicBuilders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, passthrough := range []bool{false, true} {
		t.Run(fmt.Sprint(passthrough), func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			c.Request.Header.Set("X-Api-Key", "client-secret")
			c.Request.Header.Set("Cookie", "session=private")
			c.Request.Header.Set("x-cmd-zdr", "1")
			account := commandCodeTestAccount(false)
			account.Platform = PlatformAnthropic
			svc := &GatewayService{cfg: rawChatCompletionsTestConfig()}
			body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":128,"messages":[{"role":"user","content":"hello"}]}`)
			var req *http.Request
			var err error
			if passthrough {
				req, _, err = svc.buildUpstreamRequestAnthropicAPIKeyPassthrough(context.Background(), c, account, body, "ignored-builder-token")
			} else {
				req, _, err = svc.buildUpstreamRequest(context.Background(), c, account, body, "ignored-builder-token", "apikey", "claude-sonnet-4-5", false, false)
			}
			require.NoError(t, err)
			require.Equal(t, CommandCodeBaseURL+"/v1/messages", req.URL.String())
			require.Equal(t, "Bearer upstream-only-secret", req.Header.Get("Authorization"))
			require.Equal(t, "1", req.Header.Get("X-Cmd-Zdr"))
			require.Empty(t, req.Header.Get("Cookie"))
			require.Empty(t, req.Header.Get("X-Api-Key"))
			require.True(t, HTTPUpstreamRedirectsDisabled(req.Context()))
		})
	}
}
