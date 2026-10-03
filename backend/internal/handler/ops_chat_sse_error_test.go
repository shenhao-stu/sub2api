//go:build unit

package handler

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestOpsErrorLogger_ChatSSEProtocolOutcome(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload string
		failed  bool
	}{
		{"terminal_error", `{"error":{"type":"upstream_error","message":"Concurrency limit exceeded for account, please retry later"}}`, true},
		{"terminal_error_no_type", `{"error":{"message":"Upstream request failed"}}`, true},
		{"successful_retry", `{"choices":[{"delta":{"content":"recovered"},"finish_reason":"stop"}]}`, false},
		{"error_word_in_content", `{"choices":[{"delta":{"content":"An error object is data, not a failure"}}]}`, false},
		{"nested_error_object", `{"choices":[{"delta":{"error":{"message":"tool result"}}}]}`, false},
		{"null_error", `{"error":null,"choices":[]}`, false},
		{"nonobject_error", `{"error":"an extension field","choices":[]}`, false},
		{"typed_nonterminal_event", `{"type":"response.output_text.delta","error":{"message":"extension"}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setupOpsErrorLogTestQueue(t, 2)
			gin.SetMode(gin.TestMode)
			ops := service.NewOpsService(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
			wire := "data: " + tc.payload + "\n\ndata: [DONE]\n\n"
			router := gin.New()
			router.Use(OpsErrorLoggerMiddleware(ops))
			router.POST("/v1/chat/completions", func(c *gin.Context) {
				setOpsRequestContext(c, "gpt-5.1", true)
				service.SetOpsUpstreamError(c, http.StatusTooManyRequests, "earlier rate limit", "")
				c.Header("Content-Type", "text/event-stream")
				_, err := c.Writer.WriteString(wire)
				require.NoError(t, err)
			})
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))
			require.Equal(t, http.StatusOK, rec.Code)
			require.Equal(t, wire, rec.Body.String(), "observability must not modify the response")
			require.Equal(t, int64(1), OpsErrorLogQueueLength())
			entry := (<-opsErrorLogQueue).entry
			if tc.failed {
				require.Equal(t, http.StatusBadGateway, entry.StatusCode)
				require.NotContains(t, entry.ErrorMessage, "Recovered")
				require.NotEqual(t, "earlier rate limit", entry.ErrorMessage)
				require.True(t, entry.Stream)
			} else {
				require.Equal(t, http.StatusOK, entry.StatusCode)
				require.Equal(t, "Recovered upstream error 429: earlier rate limit", entry.ErrorMessage)
			}
			require.Equal(t, "provider", entry.ErrorOwner)
		})
	}
}

func TestOpsCaptureWriter_UntypedChatErrorFrames(t *testing.T) {
	payload := `data: {"error":{"type":"upstream_error","code":"test_failure","message":"failed"}}`
	for _, ending := range []string{"\n\n", "\r\n\r\n", "\r\r", ""} {
		for _, width := range []int{1, 7, len(payload) + 4} {
			t.Run(fmt.Sprintf("ending_%q_width_%d", ending, width), func(t *testing.T) {
				state := &opsCaptureWriterState{limit: opsCaptureWriterLimit}
				frame := payload + ending
				for start := 0; start < len(frame); start += width {
					end := min(start+width, len(frame))
					state.captureResponseChunk([]byte(frame[start:end]), http.StatusOK)
				}
				state.finalizeResponseCapture()
				require.True(t, state.terminalFound)
				require.True(t, state.terminalError.StreamFailure)
				require.Equal(t, "test_failure", state.terminalError.Code)
				require.Equal(t, "failed", state.terminalError.Message)
				require.LessOrEqual(t, len(state.probe), opsTerminalSSEFrameProbeLimit)
			})
		}
	}
}

type opsChatFailedUpstream struct {
	service.HTTPUpstream
	body  string
	calls int
}

func (u *opsChatFailedUpstream) Do(_ *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	u.calls++
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(u.body)),
	}, nil
}

func TestOpsErrorLogger_ResponsesToChatFailedStream(t *testing.T) {
	for _, tokens := range []int{0, 37} {
		t.Run(fmt.Sprintf("input_tokens_%d", tokens), func(t *testing.T) {
			setupOpsErrorLogTestQueue(t, 2)
			gin.SetMode(gin.TestMode)
			upstream := &opsChatFailedUpstream{body: "data: " + `{"type":"response.output_text.delta","delta":"Visible text before the final upstream failure."}` + "\n\n" +
				fmt.Sprintf("data: "+`{"type":"response.failed","response":{"status":"failed","error":{"code":"upstream_error","message":"Upstream request failed"},"usage":{"input_tokens":%d,"output_tokens":0}}}`+"\n\n", tokens),
			}
			gateway := service.NewOpenAIGatewayService(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
				nil, nil, nil, upstream, nil, nil, nil, nil, nil, nil, nil, nil)
			ops := service.NewOpsService(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
			router := gin.New()
			router.Use(OpsErrorLoggerMiddleware(ops))
			router.POST("/v1/chat/completions", func(c *gin.Context) {
				setOpsRequestContext(c, "gpt-5.1", true)
				result, err := gateway.ForwardAsChatCompletions(context.Background(), c, &service.Account{
					ID: 442, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
					Credentials: map[string]any{"api_key": "test-only"},
					Extra:       map[string]any{"openai_responses_supported": true},
				}, []byte(`{"model":"gpt-5.1","stream":true,"messages":[{"role":"user","content":"test"}]}`), "", "gpt-5.1")
				require.Error(t, err)
				require.NotNil(t, result)
				require.Equal(t, tokens, result.Usage.InputTokens)
				require.Zero(t, result.Usage.OutputTokens)
			})
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))
			require.Equal(t, 1, upstream.calls)
			require.Equal(t, http.StatusOK, rec.Code, "already emitted text keeps the HTTP status committed")
			require.Contains(t, rec.Body.String(), "Visible text before")
			require.Contains(t, rec.Body.String(), `"error":{"message":"Upstream request failed","type":"upstream_error"}`)
			require.Equal(t, int64(1), OpsErrorLogQueueLength())
			entry := (<-opsErrorLogQueue).entry
			require.Equal(t, http.StatusBadGateway, entry.StatusCode)
			require.Equal(t, "Upstream request failed", entry.ErrorMessage)
			require.Equal(t, "provider", entry.ErrorOwner)
			require.NotContains(t, entry.ErrorMessage, "Recovered")
		})
	}
}
