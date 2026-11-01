package service

import (
	"context"
	"errors"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

const geminiMeterText = `data: {"candidates":[{"content":{"role":"model","parts":[{"text":"fixture"}]}}]}` + "\n\n"
const geminiMeterFinal = `data: {"candidates":[{"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":19,"cachedContentTokenCount":5,"candidatesTokenCount":3,"thoughtsTokenCount":2}}` + "\n\n"

type geminiMeterFailWriter struct {
	gin.ResponseWriter
	cancel context.CancelFunc
	writes int
}

func (w *geminiMeterFailWriter) Write(p []byte) (int, error) {
	w.writes++
	w.cancel()
	return 0, errors.New("synthetic disconnected")
}
func (w *geminiMeterFailWriter) WriteString(p string) (int, error) { return w.Write([]byte(p)) }

type geminiMeterReadError struct{ io.Reader }

func (r geminiMeterReadError) Read(p []byte) (int, error) {
	n, e := r.Reader.Read(p)
	if e == io.EOF {
		return n, errors.New("synthetic transport EOF")
	}
	return n, e
}

func TestGeminiOpenAIMeterPreservesLateUsageOnDisconnect(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var upstreamContext context.Context
	up := &queuedHTTPUpstreamStub{responses: []*http.Response{{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(geminiMeterText + geminiMeterFinal))}}}
	up.onCall = func(req *http.Request, _ *queuedHTTPUpstreamStub) { upstreamContext = req.Context() }
	svc := &GeminiMessagesCompatService{httpUpstream: up, cfg: &config.Config{}}
	account := &Account{ID: 811, Platform: PlatformGemini, Type: AccountTypeServiceAccount, Credentials: map[string]any{"auth_mode": "apikey", "api_key": "isolated-test-key", "location": "global", "tier_id": "vertex"}, Concurrency: 1}
	body := []byte(`{"model":"gemini-4-argon","stream":true,"messages":[{"role":"user","content":"fixture"}]}`)
	c, _ := newAntigravityCompatContext(http.MethodPost, "/v1/chat/completions", body)
	c.Request = c.Request.WithContext(ctx)
	writer := &geminiMeterFailWriter{ResponseWriter: c.Writer, cancel: func() {
		cancel()
		require.NoError(t, upstreamContext.Err(), "client cancellation must leave the metering read alive")
	}}
	c.Writer = writer
	result, _ := svc.ForwardAsChatCompletions(ctx, c, account, body)
	require.NotNil(t, result)
	require.Equal(t, 14, result.Usage.InputTokens)
	require.Equal(t, 5, result.Usage.OutputTokens)
	require.Equal(t, 5, result.Usage.CacheReadInputTokens)
	require.True(t, result.ClientDisconnect)
	require.Equal(t, 1, writer.writes)
	require.ErrorIs(t, upstreamContext.Err(), context.Canceled, "forward completion releases its transport context")
	require.Equal(t, 1, up.callCount)
}

func TestGeminiOpenAIMeterRejectsUnfinishedAndKeepsObservedUsage(t *testing.T) {
	for _, readError := range []bool{false, true} {
		t.Run(map[bool]string{false: "EOF", true: "transport"}[readError], func(t *testing.T) {
			body := geminiMeterText + `data: {"usageMetadata":{"promptTokenCount":19,"cachedContentTokenCount":5,"candidatesTokenCount":3,"thoughtsTokenCount":2}}` + "\n\n"
			var reader io.Reader = strings.NewReader(body)
			if readError {
				reader = geminiMeterReadError{reader}
			}
			svc := &GeminiMessagesCompatService{cfg: &config.Config{}}
			c, rec := newAntigravityCompatContext(http.MethodPost, "/v1/chat/completions", nil)
			result, err := svc.handleChatCompletionsStreamingResponseFromGemini(c, &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(reader)}, time.Now(), "gemini-4-argon", false, true)
			require.Error(t, err, "unfinished stream must not become successful completion")
			require.NotNil(t, result)
			require.Equal(t, 14, result.usage.InputTokens)
			require.Equal(t, 5, result.usage.OutputTokens)
			require.NotContains(t, rec.Body.String(), "[DONE]")
		})
	}
}

func TestGeminiOpenAIMeterIntermediateAndInvalidCounters(t *testing.T) {
	for _, tc := range []struct {
		name, first, final   string
		invalid              bool
		input, output, image int
	}{
		{"partial then complete", `{"promptTokenCount":19}`, `{"promptTokenCount":19,"candidatesTokenCount":3,"thoughtsTokenCount":2}`, false, 19, 5, 0},
		{"regression", `{"promptTokenCount":19,"candidatesTokenCount":3}`, `{"promptTokenCount":19,"candidatesTokenCount":2}`, true, 19, 3, 0},
		{"negative", `{"promptTokenCount":19,"candidatesTokenCount":3}`, `{"promptTokenCount":19,"candidatesTokenCount":-1}`, true, 19, 3, 0},
		{"string count", `{}`, `{"promptTokenCount":"19","candidatesTokenCount":3}`, true, 0, 0, 0},
		{"duplicate count", `{}`, `{"promptTokenCount":19,"promptTokenCount":20,"candidatesTokenCount":3}`, true, 0, 0, 0},
		{"image", `{}`, `{"promptTokenCount":19,"candidatesTokenCount":13,"candidatesTokensDetails":[{"modality":"IMAGE","tokenCount":10},{"modality":"TEXT","tokenCount":3}]}`, false, 19, 13, 10},
		{"bad image", `{}`, `{"promptTokenCount":19,"candidatesTokenCount":13,"candidatesTokensDetails":[{"modality":"IMAGE","tokenCount":-1}]}`, true, 0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var meter geminiOpenAIMeterState
			meter.observe([]byte(`{"usageMetadata":` + tc.first + `}`))
			meter.observe([]byte(`{"usageMetadata":` + tc.final + `}`))
			require.Equal(t, tc.invalid, meter.invalid)
			require.Equal(t, tc.input, meter.usage.InputTokens)
			require.Equal(t, tc.output, meter.usage.OutputTokens)
			require.Equal(t, tc.image, meter.usage.ImageOutputTokens)
			if !tc.invalid {
				require.NotNil(t, meter.receipt)
			}
		})
	}
}

func TestGeminiOpenAIStreamRejectsUnreliableTerminal(t *testing.T) {
	for _, tc := range []struct{ name, tail, code string }{
		{"meter only before terminal", `data: {"usageMetadata":{"promptTokenCount":19,"cachedContentTokenCount":5,"candidatesTokenCount":3,"thoughtsTokenCount":2}}` + "\n\n" + `data: {"candidates":[{"finishReason":"STOP"}]}` + "\n\n", "upstream_incomplete_response"},
		{"data after DONE", geminiMeterFinal + "data: [DONE]\n\n" + geminiMeterText, "invalid_upstream_terminal"},
		{"error after final", geminiMeterFinal + `data: {"error":{"code":503,"message":"synthetic unavailable"}}` + "\n\n", `"status_code":503`},
		{"malformed after final", geminiMeterFinal + "data: {broken\n\n", "malformed_upstream_stream"},
		{"content after final", geminiMeterFinal + geminiMeterText, "invalid_upstream_terminal"},
		{"bad terminal", strings.Replace(geminiMeterFinal, "STOP", "MALFORMED_FUNCTION_CALL", 1), "invalid_upstream_terminal"},
		{"missing final usage", `data: {"candidates":[{"finishReason":"STOP"}]}` + "\n\n", "upstream_incomplete_response"},
		{"invalid tool", `data: {"candidates":[{"content":{"parts":[{"functionCall":{"name":"","args":{}}}]}}]}` + "\n\n" + geminiMeterFinal, "invalid_tool_call"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := &GeminiMessagesCompatService{cfg: &config.Config{}}
			c, rec := newAntigravityCompatContext(http.MethodPost, "/v1/chat/completions", nil)
			result, err := svc.handleChatCompletionsStreamingResponseFromGemini(c, &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(geminiMeterText + tc.tail))}, time.Now(), "gemini-4-argon", false, true)
			require.Error(t, err)
			require.NotNil(t, result)
			require.Nil(t, result.usage.receiptMeasured)
			require.NotContains(t, rec.Body.String(), "[DONE]")
			require.Contains(t, rec.Body.String(), tc.code)
			if tc.name != "missing final usage" {
				require.Equal(t, 5, result.usage.OutputTokens)
			}
		})
	}
}

func TestGeminiOpenAIDisconnectDrainIsBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c, _ := newAntigravityCompatContext(http.MethodPost, "/v1/chat/completions", nil)
		c.Request = c.Request.WithContext(ctx)
		reader, writer := io.Pipe()
		defer reader.Close()
		done := make(chan struct{})
		go func() {
			defer close(done)
			defer writer.Close()
			_, _ = io.WriteString(writer, geminiMeterText+`data: {"usageMetadata":{"promptTokenCount":19,"candidatesTokenCount":3}}`+"\n\n")
			cancel()
			for {
				time.Sleep(time.Second)
				if _, err := io.WriteString(writer, ": heartbeat\n\n"); err != nil {
					return
				}
			}
		}()
		start := time.Now()
		result, err := (&GeminiMessagesCompatService{cfg: &config.Config{}}).handleChatCompletionsStreamingResponseFromGemini(c, &http.Response{StatusCode: 200, Header: http.Header{}, Body: reader}, start, "gemini-4-argon", false, true)
		require.Error(t, err)
		require.Equal(t, disconnectedResponseDrainTimeout, time.Since(start))
		require.Equal(t, 3, result.usage.OutputTokens)
		require.Nil(t, result.usage.receiptMeasured)
		<-done
	})
}

func TestGeminiOpenAICancellationBeforeDispatchAndDuringHeaders(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	up := &queuedHTTPUpstreamStub{}
	svc := &GeminiMessagesCompatService{httpUpstream: up, cfg: &config.Config{}}
	account := &Account{ID: 811, Platform: PlatformGemini, Type: AccountTypeServiceAccount, Credentials: map[string]any{"auth_mode": "apikey", "api_key": "fixture", "location": "global", "tier_id": "vertex"}, Concurrency: 1}
	body := []byte(`{"model":"gemini-4-argon","stream":true,"input":"fixture"}`)
	c, _ := newAntigravityCompatContext(http.MethodPost, "/v1/responses", body)
	c.Request = c.Request.WithContext(ctx)
	_, err := svc.ForwardAsResponses(ctx, c, account, body)
	require.Error(t, err)
	require.Zero(t, up.callCount)
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		upstream, finish := boundedGeminiUpstreamContext(ctx)
		defer finish()
		cancel()
		synctest.Wait()
		require.NoError(t, upstream.Err())
		time.Sleep(disconnectedResponseDrainTimeout)
		synctest.Wait()
		require.ErrorIs(t, upstream.Err(), context.Canceled)
	})
}
