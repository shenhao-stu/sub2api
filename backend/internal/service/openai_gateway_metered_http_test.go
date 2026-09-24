//go:build unit

package service

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIMeteredHTTPForwardStopsReplay(t *testing.T) {
	for _, path := range []string{"responses", "passthrough", "grok", "chat", "messages", "grok_messages", "raw_chat", "responses_cc", "messages_cc"} {
		for _, status := range []int{400, 429, 524} {
			for _, metered := range []bool{false, true} {
				if status == 400 && !metered {
					continue
				}
				t.Run(fmt.Sprintf("%s/%d/metered_%v", path, status, metered), func(t *testing.T) {
					body := `{"error":{"type":"rate_limit_error","code":"rate_limit_exceeded","message":"Rate limit reached"},"model":"actual-model","service_tier":"priority"`
					if metered {
						body += `,"usage":{"input_tokens":37,"output_tokens":2,"input_tokens_details":{"cached_tokens":11}}`
					}
					body += "}"
					result, err, rec, count := runMeteredHTTPForward(t, path, status, body)
					require.Error(t, err)
					require.Equal(t, 1, count, "a measured failed attempt must not be replayed")
					var failover *UpstreamFailoverError
					if !metered {
						require.ErrorAs(t, err, &failover)
						require.Nil(t, result)
						require.Empty(t, rec.Body.String())
						return
					}
					require.NotErrorAs(t, err, &failover)
					require.NotNil(t, result)
					require.Equal(t, 37, result.Usage.InputTokens)
					require.Equal(t, 2, result.Usage.OutputTokens)
					require.Equal(t, 11, result.Usage.CacheReadInputTokens)
					require.Equal(t, "metered-http", result.RequestID)
					require.Equal(t, "actual-model", result.UpstreamResponseModel)
					require.Nil(t, result.ServiceTier, "outbound request did not request a tier")
					require.Equal(t, "priority", result.UpstreamResponseServiceTier)
					require.Equal(t, status, rec.Code)
					require.Equal(t, "rate_limit_exceeded", gjson.Get(rec.Body.String(), "error.code").String())
					require.Equal(t, "priority", gjson.Get(rec.Body.String(), "service_tier").String())
					wantInput := int64(37)
					if strings.Contains(path, "messages") {
						wantInput = 26
					}
					require.Equal(t, wantInput, gjson.Get(rec.Body.String(), "usage.input_tokens").Int())
				})
			}
		}
	}
}

func TestOpenAIMeteredHTTPRejectsUntrustedQuantities(t *testing.T) {
	for _, body := range []string{
		`<html>524 timeout</html>`, `{"usage":{"input_tokens":37`,
		`{"usage":{"input_tokens":-1,"output_tokens":2}}`,
		`{"usage":{"input_tokens":"37","output_tokens":2}}`,
		`{"usage":{"input_tokens":3.7,"output_tokens":2}}`,
		`{"usage":{"input_tokens":9223372036854775808,"output_tokens":2}}`,
		`{"usage":{"input_tokens":37,"input_tokens_details":{"cached_tokens":-1}}}`,
		`{"usage":{"input_tokens":0,"output_tokens":0}}`,
		`{"usage":null}`, `{"usage":[]}`, `{"usage":{"input_tokens":true}}`,
	} {
		require.Nil(t, openAIMeteredHTTPUsage([]byte(body)), body)
	}
	for _, usage := range []string{`{"input_tokens":37}`, `{"prompt_tokens":37,"completion_tokens":2}`, `{"cache_read_input_tokens":11}`, `{"input_tokens":37,"metadata":{"tier":"priority"},"cost":0.01}`, `{"input_tokens":37,"input_tokens_details":{"image_tokens":0,"provider_metadata":true}}`} {
		require.NotNil(t, openAIMeteredHTTPUsage([]byte(`{"usage":`+usage+`}`)))
	}
	for _, body := range []string{`<html>524 timeout</html>`, `{"error":{"message":"Rate limit"},"usage":{"input_tokens":-1,"output_tokens":2}}`} {
		result, err, rec, count := runMeteredHTTPForward(t, "grok", 524, body)
		var failover *UpstreamFailoverError
		require.ErrorAs(t, err, &failover)
		require.Nil(t, result)
		require.Empty(t, rec.Body.String())
		require.Equal(t, 1, count)
	}
}

func TestOpenAIMeteredHTTPCompactHeartbeatEndsWithFailedSSE(t *testing.T) {
	c, rec := newCompactBridgeTestContext(t, true)
	stop := StartOpenAICompactSSEKeepalive(c, keepaliveTestInterval)
	defer stop()
	waitForKeepaliveBeats()
	body := []byte(`{"error":{"code":"server_error","message":"upstream failed"},"usage":{"input_tokens":37,"output_tokens":2}}`)
	resp := &http.Response{StatusCode: 524, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body)))}
	svc := &OpenAIGatewayService{}
	result, err := svc.handleMeteredOpenAIHTTPError(c.Request.Context(), c, &Account{ID: 7, Platform: PlatformOpenAI}, resp, body, nil, "model", "model", "model", true, time.Now())
	require.Error(t, err)
	require.NotNil(t, result)
	require.Equal(t, "text/event-stream", rec.Header().Get("Content-Type"))
	terminal := stripKeepaliveComments(rec.Body.String())
	require.Contains(t, terminal, "event: response.failed")
	require.Equal(t, 1, strings.Count(terminal, "data: "))
	usage := svc.parseSSEUsageFromBody(terminal)
	require.Equal(t, 37, usage.InputTokens)
	require.Equal(t, 2, usage.OutputTokens)
}

func TestOpenAIMeteredHTTPCyberRetainsUsage(t *testing.T) {
	for _, path := range []string{"responses", "passthrough", "chat", "messages"} {
		t.Run(path, func(t *testing.T) {
			result, err, rec, count := runMeteredHTTPForward(t, path, 400, `{"error":{"code":"cyber_policy","message":"blocked by cyber policy"},"usage":{"input_tokens":37,"output_tokens":2}}`)
			require.Error(t, err)
			require.NotNil(t, result)
			require.Equal(t, 37, result.Usage.InputTokens)
			require.Equal(t, 1, count)
			require.Contains(t, rec.Body.String(), `"usage"`)
		})
	}
}

func runMeteredHTTPForward(t *testing.T, path string, status int, responseBody string) (*OpenAIForwardResult, error, *httptest.ResponseRecorder, int) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	url := "/v1/responses"
	request := []byte(`{"model":"gpt-5.4","input":"hello","stream":false}`)
	if strings.Contains(path, "messages") || path == "chat" || path == "raw_chat" {
		url = "/v1/chat/completions"
		if strings.Contains(path, "messages") {
			url = "/v1/messages"
		}
		request = []byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"hello"}],"max_tokens":32,"stream":false}`)
	}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, url, strings.NewReader(string(request)))
	resp := &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}, "X-Request-Id": {"metered-http"}}, Body: &meteredHTTPTrackedBody{Reader: strings.NewReader(responseBody)}}
	originalBody := resp.Body.(*meteredHTTPTrackedBody)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{resp}}
	svc := openAIClientToolsTestService(upstream)
	account := &Account{ID: 7, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"api_key": "test-only-token"}, Extra: map[string]any{"openai_responses_supported": true}}
	if strings.HasPrefix(path, "grok") {
		account.Platform = PlatformGrok
		account.Credentials["base_url"] = "https://api.x.ai/v1"
	}
	var result *OpenAIForwardResult
	var err error
	switch path {
	case "responses":
		result, err = svc.Forward(c.Request.Context(), c, account, request)
	case "passthrough":
		result, err = svc.forwardOpenAIPassthrough(c.Request.Context(), c, account, request, request, "gpt-5.4", false, nil, false, time.Now())
	case "grok":
		result, err = svc.forwardGrokResponses(c.Request.Context(), c, account, request, "gpt-5.4", false, time.Now())
	case "chat":
		result, err = svc.ForwardAsChatCompletions(c.Request.Context(), c, account, request, "", "")
	case "messages", "grok_messages":
		result, err = svc.ForwardAsAnthropic(c.Request.Context(), c, account, request, "", "")
	case "raw_chat":
		result, err = svc.forwardAsRawChatCompletions(c.Request.Context(), c, account, request, "")
	case "responses_cc":
		result, err = svc.forwardResponsesViaRawChatCompletions(c.Request.Context(), c, account, request)
	case "messages_cc":
		result, err = svc.forwardAnthropicViaRawChatCompletions(c.Request.Context(), c, account, request, "")
	}
	require.True(t, originalBody.closed, "original upstream response body must close")
	return result, err, rec, len(upstream.requests)
}

type meteredHTTPTrackedBody struct {
	io.Reader
	closed bool
}

func (b *meteredHTTPTrackedBody) Close() error { b.closed = true; return nil }

func TestOpenAIMeteredHTTPQueuedHeartbeatRetainsProtocol(t *testing.T) {
	for _, path := range []string{"/v1/responses", "/v1/chat/completions", "/v1/messages"} {
		t.Run(path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, path, nil)
			c.Header("Content-Type", "text/event-stream")
			_, _ = c.Writer.WriteString(": ping\n\n")
			c.Writer.Flush()
			body := []byte(`{"model":"actual-model","service_tier":"priority","error":{"code":"rate_limit_exceeded","message":"rate limit reached"},"usage":{"input_tokens":37,"output_tokens":2,"input_tokens_details":{"cached_tokens":11}}}`)
			resp := &http.Response{StatusCode: 429, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body)))}
			svc := &OpenAIGatewayService{}
			result, err := svc.handleMeteredOpenAIHTTPError(c.Request.Context(), c, &Account{ID: 7, Platform: PlatformOpenAI}, resp, body, nil, "model", "model", "model", true, time.Now())
			require.Error(t, err)
			require.NotNil(t, result)
			require.Equal(t, 200, rec.Code)
			require.Equal(t, "text/event-stream", rec.Header().Get("Content-Type"))
			require.Equal(t, 1, strings.Count(rec.Body.String(), "data: "))
			if path == "/v1/responses" {
				require.Contains(t, rec.Body.String(), "event: response.failed")
			}
			if path == "/v1/messages" {
				require.Contains(t, rec.Body.String(), "event: error")
			}
			wantInput := 37
			if path == "/v1/messages" {
				wantInput = 26
			}
			usage := svc.parseSSEUsageFromBody(rec.Body.String())
			require.Equal(t, wantInput, usage.InputTokens)
			require.Equal(t, 11, usage.CacheReadInputTokens)
			require.Contains(t, rec.Body.String(), `"service_tier":"priority"`)
		})
	}
}

func TestOpenAIMeteredHTTPGrokPreservesTeamCooldown(t *testing.T) {
	for _, capacity := range []bool{false, true} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			account := &Account{ID: 7, Platform: PlatformGrok, Type: AccountTypeOAuth, Credentials: map[string]any{"team_id": t.Name()}}
			msg := "rate limit reached"
			if capacity {
				msg = "The model is currently at capacity due to high demand"
			}
			body := []byte(fmt.Sprintf(`{"error":{"message":%q},"usage":{"input_tokens":37,"output_tokens":2}}`, msg))
			resp := &http.Response{StatusCode: 429, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body)))}
			svc := &OpenAIGatewayService{}
			result, err := svc.handleMeteredOpenAIHTTPError(c.Request.Context(), c, account, resp, body, nil, "grok-test", "grok-test", "grok-test", false, time.Now())
			require.NotNil(t, result)
			require.Error(t, err)
			require.Equal(t, !capacity, isGrokTeamModelRateLimited(account, "grok-test", time.Now()))
			require.False(t, isGrokTeamModelRateLimited(account, "other-model", time.Now()))
		})
	}
}
