//go:build unit

package service

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

const chatResponsesFinalWire = `data: {"type":"response.output_text.delta","delta":"OK"}

data: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":100,"output_tokens":7}}}

`

func chatResponsesKeepaliveAccount() *Account {
	a := rawChatCompletionsTestAccount()
	a.Extra = map[string]any{"openai_responses_supported": true}
	return a
}
func TestChatResponsesKeepaliveCoversHeaderAndFirstEventWait(t *testing.T) {
	for _, headers := range []bool{true, false} {
		t.Run(fmt.Sprintf("headers_%t", headers), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := rawChatCompletionsTestConfig()
				cfg.Gateway.StreamKeepaliveInterval = 1
				delayedBody := &rawChatDelayedBody{Reader: strings.NewReader(chatResponsesFinalWire)}
				upstream := &rawChatDelayedUpstream{httpUpstreamRecorder: httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: delayedBody}}}
				if headers {
					upstream.headerDelay = 205 * time.Second
				} else {
					delayedBody.delay = 205 * time.Second
				}
				svc := &OpenAIGatewayService{cfg: cfg, httpUpstream: upstream}
				rec := &rawChatTimedRecorder{ResponseRecorder: httptest.NewRecorder()}
				c, _ := gin.CreateTestContext(rec)
				body := []byte(`{"model":"gemini-4-argon","stream":true,"messages":[{"role":"user","content":"synthetic"}]}`)
				capture := &chatReceiptCapture{claim: &ChatUsageReceiptClaim{APIKeyID: 1, RequestID: "raw-chat-synthetic", Model: "gemini-4-argon", Endpoint: ChatReceiptEndpoint}}
				c.Request = httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(body))).WithContext(context.WithValue(context.Background(), chatReceiptContextKey{}, capture))
				started := time.Now()
				result, err := svc.forwardAsChatCompletions(c.Request.Context(), c, chatResponsesKeepaliveAccount(), body, "", "", false)
				require.NoError(t, err)
				require.NotNil(t, result)
				require.Len(t, upstream.requests, 1)
				require.Equal(t, "/v1/responses", upstream.requests[0].URL.Path)
				require.NotEmpty(t, rec.writes)
				require.LessOrEqual(t, rec.writes[0].Sub(started), time.Second)
				require.Contains(t, rec.Body.String(), ": keepalive\n\n")
				require.Contains(t, rec.Body.String(), "data: [DONE]")
				require.Equal(t, 100, result.Usage.InputTokens)
				require.Equal(t, 7, result.Usage.OutputTokens)
				require.GreaterOrEqual(t, *result.FirstTokenMs, 205000)
				require.Equal(t, "v2", rec.Result().Header.Get(ChatReceiptVersionHeader), "receipt ACK must precede the first heartbeat")
				require.Equal(t, "raw-chat-synthetic", rec.Result().Header.Get(ChatReceiptIDHeader))
				require.NotNil(t, capture.meter)
				require.EqualValues(t, 100, capture.meter.InputTokens)
				require.EqualValues(t, 7, capture.meter.OutputTokens)
			})
		})
	}
}

func TestChatResponsesKeepaliveBeforeRefusalDecisionAndDuringIdle(t *testing.T) {
	for _, prefix := range []string{"", "data: {\"type\":\"response.output_text.delta\",\"delta\":\"prefix\"}\n\n"} {
		t.Run(fmt.Sprintf("prefix_%t", prefix != ""), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := rawChatCompletionsTestConfig()
				cfg.Gateway.StreamKeepaliveInterval = 1
				svc := &OpenAIGatewayService{cfg: cfg}
				rec := &rawChatTimedRecorder{ResponseRecorder: httptest.NewRecorder()}
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest("POST", ChatReceiptEndpoint, nil)
				rb := io.NopCloser(io.MultiReader(strings.NewReader(prefix), &rawChatDelayedBody{Reader: strings.NewReader(chatResponsesFinalWire), delay: 205 * time.Second}))
				started := time.Now()
				result, err := svc.handleChatStreamingResponse(&http.Response{StatusCode: 200, Header: http.Header{}, Body: rb}, c, chatResponsesKeepaliveAccount(), "gemini-4-argon", "gemini-4-argon", "gemini-4-argon", started, openAISilentRefusalMinRequestBodyBytes+1)
				require.NoError(t, err)
				require.Equal(t, 100, result.Usage.InputTokens)
				require.Equal(t, 7, result.Usage.OutputTokens)
				require.NotEmpty(t, rec.writes)
				require.LessOrEqual(t, rec.writes[0].Sub(started), time.Second)
				for i := 1; i < len(rec.writes); i++ {
					require.LessOrEqual(t, rec.writes[i].Sub(rec.writes[i-1]), time.Second)
				}
				require.GreaterOrEqual(t, strings.Count(rec.Body.String(), ": keepalive\n\n"), 200)
				require.Greater(t, c.Writer.Size(), OpenAICompactKeepaliveAdjustedWrittenSize(c))
			})
		})
	}
}

func TestChatResponsesKeepaliveHTTPErrorPreservesStatusAndUsage(t *testing.T) {
	for _, tc := range []struct {
		name    string
		delay   time.Duration
		status  int
		metered bool
	}{
		{"early_400", 0, 400, false}, {"late_400", 205 * time.Second, 400, false},
		{"late_metered_400", 205 * time.Second, 400, true}, {"late_metered_503", 205 * time.Second, 503, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := rawChatCompletionsTestConfig()
				cfg.Gateway.StreamKeepaliveInterval = 1
				wire := `{"error":{"type":"invalid_request_error","message":"synthetic rejected"}}`
				if tc.metered {
					wire = `{"error":{"type":"upstream_error","message":"synthetic rejected"},"usage":{"prompt_tokens":100,"completion_tokens":7}}`
				}
				upstream := &rawChatDelayedUpstream{headerDelay: tc.delay, httpUpstreamRecorder: httpUpstreamRecorder{resp: &http.Response{StatusCode: tc.status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(wire))}}}
				svc := &OpenAIGatewayService{cfg: cfg, httpUpstream: upstream}
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				body := []byte(`{"model":"gemini-4-argon","stream":true,"messages":[{"role":"user","content":"synthetic"}]}`)
				c.Request = httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(body)))
				result, err := svc.forwardAsChatCompletions(c.Request.Context(), c, chatResponsesKeepaliveAccount(), body, "", "", false)
				require.Error(t, err)
				require.Len(t, upstream.requests, 1)
				var failover *UpstreamFailoverError
				require.NotErrorAs(t, err, &failover)
				if tc.metered {
					require.NotNil(t, result)
					require.Equal(t, 100, result.Usage.InputTokens)
					require.Equal(t, 7, result.Usage.OutputTokens)
				} else {
					require.Nil(t, result)
				}
				if tc.delay == 0 {
					require.Equal(t, 400, rec.Code)
					require.NotContains(t, rec.Body.String(), ": keepalive")
					return
				}
				require.Equal(t, 200, rec.Code)
				require.Contains(t, rec.Body.String(), fmt.Sprintf(`"status_code":%d`, tc.status))
				require.Contains(t, rec.Body.String(), "data: {")
				require.NotContains(t, rec.Body.String(), "data: [DONE]")
				require.NotContains(t, rec.Body.String(), "\n\n{", "no bare JSON after heartbeat")
				v, ok := c.Get(OpsStreamErrorKey)
				require.True(t, ok)
				require.Equal(t, tc.status, v.(OpsStreamError).IntendedStatus)
			})
		})
	}
}

func TestChatResponsesKeepaliveFailoverRetainsTransportState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := rawChatCompletionsTestConfig()
		cfg.Gateway.StreamKeepaliveInterval = 1
		responses := []*http.Response{}
		for _, status := range []int{503, 400} {
			responses = append(responses, &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"error":{"type":"upstream_error","message":"synthetic"}}`))})
		}
		u := &rawChatDelayedUpstream{headerDelay: 205 * time.Second, httpUpstreamRecorder: httpUpstreamRecorder{responses: responses}}
		svc := &OpenAIGatewayService{cfg: cfg, httpUpstream: u}
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		body := []byte(`{"model":"gemini-4-argon","stream":true,"messages":[{"role":"user","content":"synthetic"}]}`)
		c.Request = httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(body)))
		result, err := svc.forwardAsChatCompletions(c.Request.Context(), c, chatResponsesKeepaliveAccount(), body, "", "", false)
		require.Nil(t, result)
		var failover *UpstreamFailoverError
		require.ErrorAs(t, err, &failover)
		require.Equal(t, -1, OpenAICompactKeepaliveAdjustedWrittenSize(c), "heartbeat does not authorize or prevent a replay")
		result, err = svc.forwardAsChatCompletions(c.Request.Context(), c, chatResponsesKeepaliveAccount(), body, "", "", false)
		require.Error(t, err)
		require.Nil(t, result)
		require.Len(t, u.requests, 2)
		require.Equal(t, 200, rec.Code)
		require.Contains(t, rec.Body.String(), `"status_code":400`)
		require.Equal(t, len(stripKeepaliveComments(rec.Body.String()))+2, OpenAICompactKeepaliveAdjustedWrittenSize(c))
	})
}

func TestChatResponsesKeepaliveUpstreamCommentsDoNotStarveClient(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := rawChatCompletionsTestConfig()
		cfg.Gateway.StreamKeepaliveInterval = 1
		readers := []io.Reader{}
		for i := 0; i < 410; i++ {
			readers = append(readers, &rawChatDelayedBody{Reader: strings.NewReader(": upstream comment\n\n"), delay: 500 * time.Millisecond})
		}
		readers = append(readers, strings.NewReader(chatResponsesFinalWire))
		rec := &rawChatTimedRecorder{ResponseRecorder: httptest.NewRecorder()}
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest("POST", ChatReceiptEndpoint, nil)
		started := time.Now()
		result, err := (&OpenAIGatewayService{cfg: cfg}).handleChatStreamingResponse(&http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(io.MultiReader(readers...))}, c, chatResponsesKeepaliveAccount(), "gemini-4-argon", "gemini-4-argon", "gemini-4-argon", started, openAISilentRefusalMinRequestBodyBytes+1)
		require.NoError(t, err)
		require.GreaterOrEqual(t, *result.FirstTokenMs, 205000)
		require.GreaterOrEqual(t, strings.Count(rec.Body.String(), ": keepalive\n\n"), 200)
		require.LessOrEqual(t, rec.writes[0].Sub(started), time.Second)
		for i := 1; i < len(rec.writes); i++ {
			require.LessOrEqual(t, rec.writes[i].Sub(rec.writes[i-1]), time.Second)
		}
	})
}

func TestChatResponsesKeepaliveTerminalFailureAndUnknownUsage(t *testing.T) {
	for _, tc := range []struct {
		name, wire string
		status     int
		tokens     int
	}{
		{"failed_400", `data: {"type":"response.failed","response":{"status":"failed","error":{"type":"invalid_request_error","code":"invalid_request","message":"synthetic bad request","status_code":400},"usage":{"input_tokens":100,"output_tokens":7}}}

`, 400, 100},
		{"failed_503", `data: {"type":"response.failed","response":{"status":"failed","error":{"type":"server_error","code":"overloaded_error","message":"synthetic unavailable","status_code":503},"usage":{"input_tokens":100,"output_tokens":7}}}

`, 503, 100},
		{"missing_terminal", "", 0, 0},
		{"empty_completed", `data: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":100,"output_tokens":7}}}

`, 502, 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := rawChatCompletionsTestConfig()
				cfg.Gateway.StreamKeepaliveInterval = 1
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				capture := &chatReceiptCapture{claim: &ChatUsageReceiptClaim{APIKeyID: 1, RequestID: "responses-synthetic", Model: "gemini-4-argon", Endpoint: ChatReceiptEndpoint}}
				c.Request = httptest.NewRequest("POST", ChatReceiptEndpoint, nil).WithContext(context.WithValue(context.Background(), chatReceiptContextKey{}, capture))
				rb := &rawChatDelayedBody{Reader: strings.NewReader(tc.wire), delay: 205 * time.Second}
				result, err := (&OpenAIGatewayService{cfg: cfg}).handleChatStreamingResponse(&http.Response{StatusCode: 200, Header: http.Header{}, Body: rb}, c, chatResponsesKeepaliveAccount(), "gemini-4-argon", "gemini-4-argon", "gemini-4-argon", time.Now(), openAISilentRefusalMinRequestBodyBytes+1)
				require.Error(t, err)
				var failover *UpstreamFailoverError
				require.NotErrorAs(t, err, &failover)
				require.Equal(t, tc.tokens, result.Usage.InputTokens)
				require.NotContains(t, rec.Body.String(), "data: [DONE]")
				if tc.status > 0 {
					require.Contains(t, rec.Body.String(), fmt.Sprintf(`"status_code":%d`, tc.status))
					value, ok := c.Get(OpsStreamErrorKey)
					require.True(t, ok)
					require.Equal(t, tc.status, value.(OpsStreamError).IntendedStatus)
				} else {
					require.Nil(t, capture.meter)
					require.Equal(t, -1, OpenAICompactKeepaliveAdjustedWrittenSize(c))
				}
			})
		})
	}
}

func TestChatResponsesKeepaliveDisconnectRetainsFinalReceipt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		capture := &chatReceiptCapture{claim: &ChatUsageReceiptClaim{APIKeyID: 1, RequestID: "responses-synthetic", Model: "gemini-4-argon", Endpoint: ChatReceiptEndpoint}}
		cfg := rawChatCompletionsTestConfig()
		cfg.Gateway.StreamKeepaliveInterval = 1
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Writer = &openAIChatFailingWriter{ResponseWriter: c.Writer, failAfter: 0}
		c.Request = httptest.NewRequest("POST", ChatReceiptEndpoint, nil).WithContext(context.WithValue(ctx, chatReceiptContextKey{}, capture))
		r, w := io.Pipe()
		defer r.Close()
		go func() {
			defer w.Close()
			time.Sleep(2 * time.Second)
			cancel()
			time.Sleep(20 * time.Second)
			_, _ = io.WriteString(w, chatResponsesFinalWire)
		}()
		result, err := (&OpenAIGatewayService{cfg: cfg}).handleChatStreamingResponse(&http.Response{StatusCode: 200, Header: http.Header{}, Body: r}, c, chatResponsesKeepaliveAccount(), "gemini-4-argon", "gemini-4-argon", "gemini-4-argon", time.Now(), 1)
		require.NoError(t, err)
		require.Equal(t, 100, result.Usage.InputTokens)
		require.Equal(t, 7, result.Usage.OutputTokens)
		require.NotNil(t, capture.meter)
		require.NotContains(t, rec.Body.String(), "data: [DONE]")
	})
}

func TestChatResponsesKeepaliveDisconnectedDrainRemainsBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		capture := &chatReceiptCapture{claim: &ChatUsageReceiptClaim{APIKeyID: 1, RequestID: "responses-synthetic", Model: "gemini-4-argon", Endpoint: ChatReceiptEndpoint}}
		cfg := rawChatCompletionsTestConfig()
		cfg.Gateway.StreamKeepaliveInterval = 1
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Writer = &openAIChatFailingWriter{ResponseWriter: c.Writer, failAfter: 0}
		c.Request = httptest.NewRequest("POST", ChatReceiptEndpoint, nil).WithContext(context.WithValue(ctx, chatReceiptContextKey{}, capture))
		r, w := io.Pipe()
		defer r.Close()
		defer w.Close()
		started := time.Now()
		cancel()
		result, err := (&OpenAIGatewayService{cfg: cfg}).handleChatStreamingResponse(&http.Response{StatusCode: 200, Header: http.Header{}, Body: r}, c, chatResponsesKeepaliveAccount(), "gemini-4-argon", "gemini-4-argon", "gemini-4-argon", started, 1)
		require.Error(t, err)
		require.Equal(t, disconnectedResponseDrainTimeout, time.Since(started))
		require.Zero(t, result.Usage.InputTokens)
		require.Nil(t, capture.meter)
	})
}

func TestChatResponsesKeepaliveNonStreamingUnchanged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := rawChatCompletionsTestConfig()
		cfg.Gateway.StreamKeepaliveInterval = 1
		up := &rawChatDelayedUpstream{headerDelay: 205 * time.Second, httpUpstreamRecorder: httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(chatResponsesFinalWire))}}}
		svc := &OpenAIGatewayService{cfg: cfg, httpUpstream: up}
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		body := []byte(`{"model":"gemini-4-argon","stream":false,"messages":[{"role":"user","content":"synthetic"}]}`)
		c.Request = httptest.NewRequest("POST", ChatReceiptEndpoint, strings.NewReader(string(body)))
		result, err := svc.forwardAsChatCompletions(c.Request.Context(), c, chatResponsesKeepaliveAccount(), body, "", "", false)
		require.NoError(t, err)
		require.Equal(t, 100, result.Usage.InputTokens)
		require.Equal(t, 7, result.Usage.OutputTokens)
		require.NotContains(t, rec.Body.String(), ": keepalive")
		require.NotContains(t, rec.Body.String(), "data:")
		require.Len(t, up.requests, 1)
	})
}
