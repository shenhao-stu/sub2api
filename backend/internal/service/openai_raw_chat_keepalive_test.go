//go:build unit

package service

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type rawChatDelayedUpstream struct {
	httpUpstreamRecorder
	headerDelay time.Duration
}

func (u *rawChatDelayedUpstream) Do(r *http.Request, p string, a int64, n int) (*http.Response, error) {
	time.Sleep(u.headerDelay)
	return u.httpUpstreamRecorder.Do(r, p, a, n)
}

type rawChatDelayedBody struct {
	io.Reader
	delay time.Duration
}

func (r *rawChatDelayedBody) Read(p []byte) (int, error) {
	time.Sleep(r.delay)
	r.delay = 0
	return r.Reader.Read(p)
}
func (r *rawChatDelayedBody) Close() error { return nil }

type rawChatTimedRecorder struct {
	*httptest.ResponseRecorder
	writes []time.Time
}

func (r *rawChatTimedRecorder) Write(p []byte) (int, error) {
	r.writes = append(r.writes, time.Now())
	return r.ResponseRecorder.Write(p)
}

func (r *rawChatTimedRecorder) WriteString(s string) (int, error) { return r.Write([]byte(s)) }

const rawChatFinalWire = "data: {\"model\":\"gemini-4-argon\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"OK\"},\"finish_reason\":\"stop\"}]}\n\n" +
	"data: {\"model\":\"gemini-4-argon\",\"choices\":[],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":7,\"total_tokens\":107}}\n\ndata: [DONE]\n\n"

func TestRawChatKeepaliveCoversHeaderAndFirstFrameWait(t *testing.T) {
	for _, headers := range []bool{true, false} {
		t.Run(fmt.Sprintf("headers_%t", headers), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := rawChatCompletionsTestConfig()
				cfg.Gateway.StreamKeepaliveInterval = 1
				delayedBody := &rawChatDelayedBody{Reader: strings.NewReader(rawChatFinalWire)}
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
				result, err := svc.forwardAsRawChatCompletions(c.Request.Context(), c, rawChatCompletionsTestAccount(), body, "")
				require.NoError(t, err)
				require.NotNil(t, result)
				require.Len(t, upstream.requests, 1)
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

func TestRawChatKeepaliveHTTPErrorPreservesStatusAndUsage(t *testing.T) {
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
				result, err := svc.forwardAsRawChatCompletions(c.Request.Context(), c, rawChatCompletionsTestAccount(), body, "")
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

func TestRawChatKeepaliveNonStreamingUnchanged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := rawChatCompletionsTestConfig()
		cfg.Gateway.StreamKeepaliveInterval = 1
		wire := `{"model":"gemini-4-argon","choices":[{"message":{"content":"OK"},"finish_reason":"stop"}],"usage":{"prompt_tokens":100,"completion_tokens":7}}`
		u := &rawChatDelayedUpstream{headerDelay: 205 * time.Second, httpUpstreamRecorder: httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(wire))}}}
		svc := &OpenAIGatewayService{cfg: cfg, httpUpstream: u}
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		body := []byte(`{"model":"gemini-4-argon","stream":false,"messages":[{"role":"user","content":"synthetic"}]}`)
		c.Request = httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(body)))
		result, err := svc.forwardAsRawChatCompletions(c.Request.Context(), c, rawChatCompletionsTestAccount(), body, "")
		require.NoError(t, err)
		require.NotNil(t, result)
		require.NotContains(t, rec.Body.String(), ": keepalive")
		require.Equal(t, 200, rec.Code)
	})
}

func TestRawChatKeepaliveEmptyCompletionDoesNotBecomeSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := rawChatCompletionsTestConfig()
		cfg.Gateway.StreamKeepaliveInterval = 1
		wire := "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":0}}\n\ndata: [DONE]\n\n"
		u := &rawChatDelayedUpstream{httpUpstreamRecorder: httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: &rawChatDelayedBody{Reader: strings.NewReader(wire), delay: 205 * time.Second}}}}
		svc := &OpenAIGatewayService{cfg: cfg, httpUpstream: u}
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		body := []byte(`{"model":"gemini-4-argon","stream":true,"messages":[{"role":"user","content":"synthetic"}]}`)
		c.Request = httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(body)))
		result, err := svc.forwardAsRawChatCompletions(c.Request.Context(), c, rawChatCompletionsTestAccount(), body, "")
		require.Error(t, err)
		var failover *UpstreamFailoverError
		require.NotErrorAs(t, err, &failover)
		require.Equal(t, 100, result.Usage.InputTokens)
		require.Equal(t, 0, result.Usage.OutputTokens)
		require.Equal(t, 200, rec.Code)
		require.Contains(t, rec.Body.String(), `"status_code":502`)
		require.Contains(t, rec.Body.String(), "upstream_empty_completion")
		require.NotContains(t, rec.Body.String(), "data: [DONE]")
		v, ok := c.Get(OpsStreamErrorKey)
		require.True(t, ok)
		require.Equal(t, 502, v.(OpsStreamError).IntendedStatus)
	})
}

func TestRawChatKeepaliveFailoverRetainsTransportState(t *testing.T) {
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
		result, err := svc.forwardAsRawChatCompletions(c.Request.Context(), c, rawChatCompletionsTestAccount(), body, "")
		require.Nil(t, result)
		var failover *UpstreamFailoverError
		require.ErrorAs(t, err, &failover)
		require.Equal(t, -1, OpenAICompactKeepaliveAdjustedWrittenSize(c), "heartbeat does not authorize or prevent a replay")
		result, err = svc.forwardAsRawChatCompletions(c.Request.Context(), c, rawChatCompletionsTestAccount(), body, "")
		require.Error(t, err)
		require.Nil(t, result)
		require.Len(t, u.requests, 2)
		require.Equal(t, 200, rec.Code)
		require.Contains(t, rec.Body.String(), `"status_code":400`)
		require.Equal(t, len(stripKeepaliveComments(rec.Body.String()))+2, OpenAICompactKeepaliveAdjustedWrittenSize(c))
	})
}

func TestRawChatKeepaliveSurvivesMidstreamSilence(t *testing.T) {
	for _, blank := range []bool{true, false} {
		t.Run(fmt.Sprintf("closed_frame_%t", blank), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := rawChatCompletionsTestConfig()
				cfg.Gateway.StreamKeepaliveInterval = 1
				prefix := "data: {\"choices\":[{\"delta\":{\"content\":\"prefix\"}}]}\n"
				if blank {
					prefix += "\n"
				}
				tail := "\n" + rawChatFinalWire
				rb := io.NopCloser(io.MultiReader(strings.NewReader(prefix), &rawChatDelayedBody{Reader: strings.NewReader(tail), delay: 205 * time.Second}))
				svc := &OpenAIGatewayService{cfg: cfg}
				rec := &rawChatTimedRecorder{ResponseRecorder: httptest.NewRecorder()}
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest("POST", "/v1/chat/completions", nil)
				result, err := svc.streamRawChatCompletions(c, &http.Response{StatusCode: 200, Header: http.Header{}, Body: rb}, rawChatCompletionsTestAccount(), "gemini-4-argon", "gemini-4-argon", "gemini-4-argon", nil, nil, time.Now(), 1)
				require.NoError(t, err)
				require.Equal(t, 100, result.Usage.InputTokens)
				require.Equal(t, 7, result.Usage.OutputTokens)
				for i := 1; i < len(rec.writes); i++ {
					require.LessOrEqual(t, rec.writes[i].Sub(rec.writes[i-1]), time.Second)
				}
				require.Contains(t, rec.Body.String(), ": keepalive\n")
				require.Contains(t, rec.Body.String(), "data: [DONE]")
				if !blank {
					require.Contains(t, rec.Body.String(), "prefix\"}}]}\n: keepalive\n: keepalive\n", "do not close an unfinished SSE frame")
				}
				require.Greater(t, c.Writer.Size(), OpenAICompactKeepaliveAdjustedWrittenSize(c), "idle comments are not semantic output even without a header keeper")
			})
		})
	}
}

func TestRawChatKeepaliveDisconnectRetainsFinalReceipt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		capture := &chatReceiptCapture{claim: &ChatUsageReceiptClaim{APIKeyID: 1, RequestID: "raw-chat-synthetic", Model: "gemini-4-argon", Endpoint: ChatReceiptEndpoint}}
		ctx = context.WithValue(ctx, chatReceiptContextKey{}, capture)
		cfg := rawChatCompletionsTestConfig()
		cfg.Gateway.StreamKeepaliveInterval = 1
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest("POST", "/v1/chat/completions", nil).WithContext(ctx)
		r, w := io.Pipe()
		defer r.Close()
		go func() {
			defer w.Close()
			_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"initial\"}}]}\n\n")
			cancel()
			time.Sleep(20 * time.Second)
			_, _ = io.WriteString(w, rawChatFinalWire)
		}()
		result, err := (&OpenAIGatewayService{cfg: cfg}).streamRawChatCompletions(c, &http.Response{StatusCode: 200, Header: http.Header{}, Body: r}, rawChatCompletionsTestAccount(), "gemini-4-argon", "gemini-4-argon", "gemini-4-argon", nil, nil, time.Now(), 1)
		require.NoError(t, err)
		require.Equal(t, 100, result.Usage.InputTokens)
		require.Equal(t, 7, result.Usage.OutputTokens)
		require.NotNil(t, capture.meter, "late terminal plus usage plus DONE remains authoritative")
		require.NotContains(t, rec.Body.String(), "data: [DONE]", "a disconnected client is not written to again")
	})
}

func TestRawChatKeepaliveDisconnectedDrainIsBoundedAndUnknown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		capture := &chatReceiptCapture{claim: &ChatUsageReceiptClaim{APIKeyID: 1, RequestID: "raw-chat-synthetic", Model: "gemini-4-argon", Endpoint: ChatReceiptEndpoint}}
		ctx = context.WithValue(ctx, chatReceiptContextKey{}, capture)
		cfg := rawChatCompletionsTestConfig()
		cfg.Gateway.StreamKeepaliveInterval = 1
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest("POST", "/v1/chat/completions", nil).WithContext(ctx)
		r, w := io.Pipe()
		defer r.Close()
		writerDone := make(chan struct{})
		go func() {
			defer close(writerDone)
			defer w.Close()
			_, _ = io.WriteString(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":7}}\n\n")
			cancel()
			for {
				time.Sleep(time.Second)
				if _, err := io.WriteString(w, ": upstream heartbeat\n\n"); err != nil {
					return
				}
			}
		}()
		started := time.Now()
		result, _ := (&OpenAIGatewayService{cfg: cfg}).streamRawChatCompletions(c, &http.Response{StatusCode: 200, Header: http.Header{}, Body: r}, rawChatCompletionsTestAccount(), "gemini-4-argon", "gemini-4-argon", "gemini-4-argon", nil, nil, started, 1)
		require.Equal(t, disconnectedResponseDrainTimeout, time.Since(started))
		require.Equal(t, 100, result.Usage.InputTokens)
		require.Equal(t, 7, result.Usage.OutputTokens)
		require.Nil(t, capture.meter, "heartbeat and partial usage cannot fabricate a final receipt")
		<-writerDone
	})
}

func TestRawChatKeepaliveScanPanicReleasesReader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		scanner := bufio.NewScanner(strings.NewReader(strings.Repeat("data: synthetic\n", 100)))
		require.Panics(t, func() { scanOpenAIChatLines(scanner, time.Second, func(string) { panic("synthetic") }, func() {}) })
		synctest.Wait() // The test bubble must finish without a blocked channel sender.
	})
}
