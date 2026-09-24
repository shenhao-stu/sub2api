//go:build unit

package service

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Independently verify the downstream contract, not only the partial result:
// an upstream usage observation followed by EOF/reset must remain chargeable
// without an account retry or a fabricated successful terminal.
func TestMonitorOpenAIMeteredReadFailureWire(t *testing.T) {
	for _, path := range []string{"responses_stream", "passthrough_stream", "chat_stream", "chat_buffered", "messages_stream", "messages_buffered"} {
		for _, brokenRead := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reset_%v", path, brokenRead), func(t *testing.T) {
				body := "event: response.in_progress\ndata: {\"type\":\"response.in_progress\",\"response\":{\"id\":\"resp_monitor\",\"usage\":{\"input_tokens\":37,\"output_tokens\":2,\"input_tokens_details\":{\"cached_tokens\":11}}}}\n\n"
				c, rec, resp, svc, account := meteredOpenAITestContext(PlatformOpenAI, body)
				if brokenRead {
					resp.Body = io.NopCloser(io.MultiReader(strings.NewReader(body), &openAICompatBufferedReadErrorCloser{err: io.ErrUnexpectedEOF}))
				}
				got, err := runMeteredOpenAIReader(svc, c, resp, account, path)
				require.Error(t, err)
				var failover *UpstreamFailoverError
				require.NotErrorAs(t, err, &failover)
				require.NotNil(t, got)
				require.Equal(t, 37, got.InputTokens)
				require.Equal(t, 11, got.CacheReadInputTokens)
				require.NotContains(t, rec.Body.String(), "response.completed")
				wire, ok := extractOpenAIUsageFromJSONBytes(rec.Body.Bytes())
				if strings.Contains(rec.Header().Get("Content-Type"), "text/event-stream") {
					wire = *svc.parseSSEUsageFromBody(rec.Body.String())
					ok = true
					require.Contains(t, rec.Body.String(), "error")
				} else {
					require.GreaterOrEqual(t, rec.Code, 400)
				}
				require.True(t, ok)
				wantInput := 37
				if strings.HasPrefix(path, "messages_") {
					wantInput = 26
				}
				require.Equal(t, wantInput, wire.InputTokens)
				require.Equal(t, 2, wire.OutputTokens)
				require.Equal(t, 11, wire.CacheReadInputTokens)
			})
		}
	}
}

func TestMonitorMeteredHTTPAfterCompactHeartbeat(t *testing.T) {
	c, rec := newCompactBridgeTestContext(t, true)
	stop := StartOpenAICompactSSEKeepalive(c, keepaliveTestInterval)
	defer stop()
	waitForKeepaliveBeats()
	body := `{"error":{"code":"rate_limit_exceeded","message":"upstream rate limited"},"usage":{"input_tokens":37,"output_tokens":2,"input_tokens_details":{"cached_tokens":11}}}`
	_, _, resp, svc, account := meteredOpenAITestContext(PlatformOpenAI, body)
	resp.StatusCode = http.StatusTooManyRequests
	result, err := svc.handleMeteredOpenAIHTTPError(c.Request.Context(), c, account, resp, []byte(body), []byte(`{"model":"model"}`), "model", "model", "model", false, time.Now())
	stop()
	require.Error(t, err)
	var failover *UpstreamFailoverError
	require.NotErrorAs(t, err, &failover)
	require.NotNil(t, result)
	require.Equal(t, 37, result.Usage.InputTokens)
	require.Equal(t, http.StatusOK, rec.Code, "actual heartbeat already committed HTTP 200")
	require.Equal(t, "text/event-stream", rec.Header().Get("Content-Type"))
	events := parseCompactBridgeSSE(t, stripKeepaliveComments(rec.Body.String()))
	require.Len(t, events, 1)
	require.Equal(t, "response.failed", events[0][0])
	require.Equal(t, int64(37), gjson.Get(events[0][1], "response.usage.input_tokens").Int())
	require.Equal(t, int64(11), gjson.Get(events[0][1], "response.usage.cache_read_input_tokens").Int())
}

func TestMonitorMeteredHTTPAfterQueueHeartbeat(t *testing.T) {
	for _, endpoint := range []string{"/v1/responses", "/v1/chat/completions", "/v1/messages"} {
		t.Run(endpoint, func(t *testing.T) {
			body := `{"error":{"code":"rate_limit_exceeded","message":"upstream rate limited"},"usage":{"input_tokens":37,"output_tokens":2,"input_tokens_details":{"cached_tokens":11}}}`
			c, rec, resp, svc, account := meteredOpenAITestContext(PlatformOpenAI, body)
			c.Request.URL.Path = endpoint
			c.Header("Content-Type", "text/event-stream")
			_, err := c.Writer.WriteString(": queued\n\n")
			require.NoError(t, err)
			c.Writer.Flush()
			resp.StatusCode = http.StatusTooManyRequests
			result, err := svc.handleMeteredOpenAIHTTPError(c.Request.Context(), c, account, resp, []byte(body), nil, "model", "model", "model", true, time.Now())
			require.Error(t, err)
			var failover *UpstreamFailoverError
			require.NotErrorAs(t, err, &failover)
			require.NotNil(t, result)
			require.Equal(t, http.StatusOK, rec.Code)
			require.Equal(t, "text/event-stream", rec.Header().Get("Content-Type"))
			require.NotContains(t, rec.Body.String(), "\n\n{", "never append bare JSON after an SSE heartbeat")
			require.Contains(t, rec.Body.String(), "data: ")
			require.Contains(t, rec.Body.String(), `"error"`)
			wire := svc.parseSSEUsageFromBody(rec.Body.String())
			wantInput := 37
			if endpoint == "/v1/messages" {
				wantInput = 26
			}
			require.Equal(t, wantInput, wire.InputTokens)
			require.Equal(t, 11, wire.CacheReadInputTokens)
		})
	}
}
