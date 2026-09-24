package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

const grokStreamConcurrencyMessage = "Concurrency limit exceeded for account, please retry later"

func TestGrokStreamPreOutput429AllowsFailover(t *testing.T) {
	preamble := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_failed_attempt\"}}\n\n" +
		"event: response.in_progress\ndata: {\"type\":\"response.in_progress\",\"response\":{\"id\":\"resp_failed_attempt\"}}\n\n"
	largePreamble := "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_failed_attempt\",\"metadata\":\"" +
		strings.Repeat("m", 8192) + "\"}}\n\n"
	for _, terminal := range []string{"error", "response.failed"} {
		for _, tt := range []struct {
			name      string
			prefix    string
			keepalive bool
		}{
			{name: "immediate"},
			{name: "metadata_exceeds_writer_buffer", prefix: largePreamble},
			{name: "metadata_then_keepalive", prefix: preamble, keepalive: true},
		} {
			t.Run(terminal+"/"+tt.name, func(t *testing.T) {
				run := runGrokPreOutputStream(t, tt.prefix, terminal,
					"rate_limit_exceeded", grokStreamConcurrencyMessage, tt.keepalive, tt.keepalive)

				var failoverErr *UpstreamFailoverError
				require.ErrorAs(t, run.err, &failoverErr)
				require.Equal(t, http.StatusTooManyRequests, failoverErr.StatusCode)
				require.True(t, failoverErr.RetryableOnSameAccount)
				require.Equal(t, -1, OpenAICompactKeepaliveAdjustedWrittenSize(run.context))
				require.NotContains(t, run.recorder.Body.String(), "data:")
				require.NotContains(t, run.recorder.Body.String(), "resp_failed_attempt")
				if tt.keepalive {
					require.Contains(t, run.firstFlush, ":\n\n")
					require.NotContains(t, run.firstFlush, "data:")
					require.Equal(t, http.StatusOK, run.recorder.Code)
					require.Empty(t, run.recorder.Result().Header.Get("X-Request-Id"))
				}
				assertGrokStreamUpstreamFailure(t, run.context, "failover")
			})
		}
	}
}

func TestGrokStreamCommittedOutputNeverReplays429(t *testing.T) {
	for _, terminal := range []string{"error", "response.failed"} {
		for _, tt := range []struct {
			name  string
			event string
		}{
			{name: "text", event: `{"type":"response.output_text.delta","delta":"visible output"}`},
			{name: "tool", event: `{"type":"response.function_call_arguments.delta","item_id":"call_test","delta":"{}"}`},
			{name: "reasoning", event: `{"type":"response.reasoning_summary_text.delta","delta":"visible reasoning"}`},
		} {
			t.Run(terminal+"/"+tt.name, func(t *testing.T) {
				run := runGrokPreOutputStream(t, "data: "+tt.event+"\n\n", terminal,
					"rate_limit_exceeded", grokStreamConcurrencyMessage, false, true)

				require.Error(t, run.err)
				var failoverErr *UpstreamFailoverError
				require.False(t, errors.As(run.err, &failoverErr), "committed semantic output cannot be replayed")
				require.Contains(t, run.firstFlush, tt.event)
				require.Equal(t, 1, strings.Count(run.recorder.Body.String(), grokStreamConcurrencyMessage))
				assertGrokStreamUpstreamFailure(t, run.context, "stream_failed")
			})
		}
	}
}

func TestGrokStreamPreOutputRetryPolicyMatchesGrokHTTP(t *testing.T) {
	for _, terminal := range []string{"error", "response.failed"} {
		for _, tt := range []struct {
			name      string
			code      string
			message   string
			retryable bool
			maxRetry  int
		}{
			{name: "pool_rate_limit", code: "rate_limit_exceeded", message: grokStreamConcurrencyMessage, retryable: true},
			{name: "quota_exhausted", code: "subscription:free-usage-exhausted", message: "Free usage exhausted"},
			{name: "model_capacity", code: "rate_limit_exceeded", message: "The model is currently at capacity due to high demand", retryable: true, maxRetry: 1},
		} {
			t.Run(terminal+"/"+tt.name, func(t *testing.T) {
				run := runGrokPreOutputStream(t, "", terminal, tt.code, tt.message, false, false)
				var failoverErr *UpstreamFailoverError
				require.ErrorAs(t, run.err, &failoverErr)
				require.Equal(t, http.StatusTooManyRequests, failoverErr.StatusCode)
				require.Equal(t, tt.retryable, failoverErr.RetryableOnSameAccount)
				require.Equal(t, tt.maxRetry, failoverErr.SameAccountRetryMax)
				if tt.maxRetry > 0 {
					require.Equal(t, 500*time.Millisecond, failoverErr.SameAccountRetryDelay)
					require.WithinDuration(t, time.Now().Add(30*time.Second), failoverErr.SameAccountRetryDeadline, 2*time.Second)
				} else {
					require.True(t, failoverErr.SameAccountRetryDeadline.IsZero())
				}
				require.Empty(t, run.recorder.Body.String())
			})
		}
	}
}

func TestGrokStreamTerminalPairRecordsOneUpstreamFailure(t *testing.T) {
	run := runGrokPreOutputStream(t,
		"data: {\"type\":\"response.output_text.delta\",\"delta\":\"visible output\"}\n\n",
		"error", "rate_limit_exceeded", grokStreamConcurrencyMessage, false, true, "response.failed")
	require.Error(t, run.err)
	var failoverErr *UpstreamFailoverError
	require.False(t, errors.As(run.err, &failoverErr))
	require.Equal(t, 1, strings.Count(run.recorder.Body.String(), grokStreamConcurrencyMessage))
	assertGrokStreamUpstreamFailure(t, run.context, "stream_failed")
}

func TestGrokStreamLocalReadFailurePreservesExistingPolicy(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	account := &Account{ID: 922812, Platform: PlatformGrok, Type: AccountTypeOAuth}
	svc := &OpenAIGatewayService{}
	message := "OpenAI stream disconnected before completion: unexpected EOF"
	err := svc.newOpenAIStreamFailoverError(c, account, false, "upstream-read-test", nil, message)
	require.Equal(t, http.StatusBadGateway, err.StatusCode)
	require.JSONEq(t, `{"error":{"type":"upstream_error","message":"`+message+`"}}`, string(err.ResponseBody))
	require.False(t, svc.isOpenAIAccountRuntimeBlocked(account),
		"a local read or staging error must not acquire a new provider-error cooldown")
}

func TestGrokStreamErrorBodyExcludesUnrelatedResponseContent(t *testing.T) {
	for _, tt := range []struct {
		name    string
		payload string
		want    string
	}{
		{
			name: "responses_error_wins_over_output_and_outer_error",
			payload: `{"type":"response.failed","error":{"message":"free usage exhausted"},` +
				`"response":{"error":{"type":"rate_limit_error","message":"model at capacity"},` +
				`"output":[{"text":"subscription:free-usage-exhausted tokens 100/100"}]}}`,
			want: `{"error":{"type":"rate_limit_error","message":"model at capacity"}}`,
		},
		{
			name: "bare_error_excludes_response_metadata",
			payload: `{"error":{"message":"model at capacity"},` +
				`"response":{"metadata":{"message":"free usage exhausted"}}}`,
			want: `{"error":{"message":"model at capacity"}}`,
		},
		{
			name: "string_error_keeps_provider_shape",
			payload: `{"response":{"error":"model at capacity",` +
				`"output":[{"text":"free usage exhausted"}]}}`,
			want: `{"error":"model at capacity"}`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			payload := []byte(tt.payload)
			body := grokStreamErrorBody(payload)
			require.JSONEq(t, tt.want, string(body))
			require.Equal(t, tt.payload, string(payload))
			decision := classifyGrokUpstreamFailure(http.StatusTooManyRequests, body, "grok-4.6")
			require.Equal(t, GrokFailureModelCapacity, decision.Class)
		})
	}
}

type grokPreOutputStreamResult struct {
	context    *gin.Context
	recorder   *httptest.ResponseRecorder
	firstFlush string
	err        error
}

type grokPreOutputFlushRecorder struct {
	*httptest.ResponseRecorder
	flushed chan string
}

func (w *grokPreOutputFlushRecorder) Flush() {
	w.ResponseRecorder.Flush()
	select {
	case w.flushed <- w.Body.String():
	default:
	}
}

func runGrokPreOutputStream(t *testing.T, prefix, terminal, code, message string, keepalive, awaitFlush bool, followingTerminals ...string) grokPreOutputStreamResult {
	t.Helper()
	gin.SetMode(gin.TestMode)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reader, writer := io.Pipe()
	defer func() { _ = reader.Close() }()
	defer func() { _ = writer.Close() }()
	recorder := &grokPreOutputFlushRecorder{
		ResponseRecorder: httptest.NewRecorder(),
		flushed:          make(chan string, 1),
	}
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(ctx)
	errorBody := map[string]any{"type": "rate_limit_error", "code": code, "message": message}
	terminalFrame := ""
	for _, eventType := range append([]string{terminal}, followingTerminals...) {
		payload := map[string]any{"error": errorBody}
		if eventType == "response.failed" {
			payload = map[string]any{
				"type":     eventType,
				"response": map[string]any{"id": "resp_failed_attempt", "status": "failed", "error": errorBody},
			}
		}
		encoded, err := json.Marshal(payload)
		require.NoError(t, err)
		terminalFrame += "event: " + eventType + "\ndata: " + string(encoded) + "\n\n"
	}
	writerDone := make(chan string, 1)
	go func() {
		firstFlush := ""
		defer func() {
			_ = writer.Close()
			writerDone <- firstFlush
		}()
		if _, writeErr := io.WriteString(writer, prefix); writeErr != nil {
			return
		}
		if awaitFlush {
			select {
			case firstFlush = <-recorder.flushed:
			case <-ctx.Done():
				_ = writer.CloseWithError(ctx.Err())
				return
			}
		}
		_, _ = io.WriteString(writer, terminalFrame)
	}()
	interval := 0
	if keepalive {
		interval = 1
	}
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{
		MaxLineSize:             defaultMaxLineSize,
		StreamKeepaliveInterval: interval,
	}}}
	account := &Account{
		ID: 922801, Name: "grok-stream-test", Platform: PlatformGrok, Type: AccountTypeAPIKey,
		Credentials: map[string]any{"pool_mode": true, "pool_mode_retry_count": 3},
	}
	resp := &http.Response{
		StatusCode: http.StatusOK, Body: reader,
		Header: http.Header{"X-Request-Id": []string{"grok-upstream-attempt"}},
	}
	_, err := svc.handleStreamingResponse(ctx, resp, c, account, time.Now(), "grok-4.6", "grok-4.6")
	_ = reader.Close()
	select {
	case firstFlush := <-writerDone:
		return grokPreOutputStreamResult{context: c, recorder: recorder.ResponseRecorder, firstFlush: firstFlush, err: err}
	case <-ctx.Done():
		t.Fatal("synthetic Grok upstream did not finish")
		return grokPreOutputStreamResult{}
	}
}

func assertGrokStreamUpstreamFailure(t *testing.T, c *gin.Context, kind string) {
	t.Helper()
	rawEvents, ok := c.Get(OpsUpstreamErrorsKey)
	require.True(t, ok, "upstream error must not fall back to local request attribution")
	events, ok := rawEvents.([]*OpsUpstreamErrorEvent)
	require.True(t, ok)
	require.Len(t, events, 1)
	require.Equal(t, PlatformGrok, events[0].Platform)
	require.Equal(t, int64(922801), events[0].AccountID)
	require.Equal(t, http.StatusTooManyRequests, events[0].UpstreamStatusCode)
	require.Equal(t, "grok-upstream-attempt", events[0].UpstreamRequestID)
	require.Equal(t, kind, events[0].Kind)
	require.Equal(t, grokStreamConcurrencyMessage, events[0].Message)
}
