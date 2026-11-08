package service

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Exercise both client protocols and both response modes against an actual HTTP
// stream. Cache evidence must come from original frames, not decoded zero values.
func TestNativeAnthropicCacheWire(t *testing.T) {
	const initial = `"cache_creation_input_tokens":20,"cache_creation":{"ephemeral_5m_input_tokens":7,"ephemeral_1h_input_tokens":13}`
	for _, tc := range []struct {
		name, start, delta string
		total, five, hour  int
		known              bool
	}{
		{"output_only", initial, `"output_tokens":2`, 20, 7, 13, true},
		{"five_minute", `"cache_creation_input_tokens":20,"cache_creation":{"ephemeral_5m_input_tokens":20,"ephemeral_1h_input_tokens":0}`, `"output_tokens":2`, 20, 20, 0, true},
		{"one_hour", `"cache_creation_input_tokens":20,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":20}`, `"output_tokens":2`, 20, 0, 20, true},
		{"cumulative_replace", initial, `"output_tokens":2,"cache_creation_input_tokens":22,"cache_creation":{"ephemeral_5m_input_tokens":5,"ephemeral_1h_input_tokens":17}`, 22, 5, 17, true},
		{"explicit_zero", initial, `"output_tokens":2,"cache_creation_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0}`, 0, 0, 0, true},
		{"no_split", `"cache_creation_input_tokens":20`, `"output_tokens":2`, 20, 0, 0, false},
		{"no_total", `"cache_creation":{"ephemeral_5m_input_tokens":7,"ephemeral_1h_input_tokens":13}`, `"output_tokens":2`, 0, 0, 0, false},
		{"late_no_split", initial, `"output_tokens":2,"cache_creation_input_tokens":22`, 22, 0, 0, false},
		{"late_no_total", initial, `"output_tokens":2,"cache_creation":{"ephemeral_5m_input_tokens":7,"ephemeral_1h_input_tokens":13}`, 20, 0, 0, false},
		{"contradictory", initial, `"output_tokens":2,"cache_creation_input_tokens":22,"cache_creation":{"ephemeral_5m_input_tokens":5,"ephemeral_1h_input_tokens":18}`, 22, 0, 0, false},
		{"duplicate_bucket", initial, `"output_tokens":2,"cache_creation_input_tokens":20,"cache_creation":{"ephemeral_5m_input_tokens":7,"ephemeral_5m_input_tokens":7,"ephemeral_1h_input_tokens":13}`, 20, 0, 0, false},
		{"invalid_bucket", initial, `"output_tokens":2,"cache_creation_input_tokens":20,"cache_creation":{"ephemeral_5m_input_tokens":null,"ephemeral_1h_input_tokens":13}`, 20, 0, 0, false},
		// The typed decoder rejects the whole event. Raw observation still must
		// invalidate the earlier split; previously observed aggregate usage stays.
		{"invalid_late_frame", initial, `"output_tokens":"invalid","cache_creation_input_tokens":20,"cache_creation":{"ephemeral_5m_input_tokens":null,"ephemeral_1h_input_tokens":13}`, 20, 0, 0, false},
		{"invalid_late_frame_eof", initial, `"output_tokens":"invalid","cache_creation_input_tokens":20,"cache_creation":{"ephemeral_5m_input_tokens":null,"ephemeral_1h_input_tokens":13}`, 20, 0, 0, false},
	} {
		for _, mode := range []string{"chat-buffered", "chat-stream", "responses-buffered", "responses-stream"} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				frames := []string{
					`{"type":"message_start","message":{"id":"synthetic-cache","type":"message","role":"assistant","model":"claude-sonnet-4","content":[],"usage":{"input_tokens":3,` + tc.start + `}}}`,
					`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":"OK"}}`,
					`{"type":"content_block_stop","index":0}`,
					`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{` + tc.delta + `}}`,
					`{"type":"message_stop"}`,
				}
				if strings.HasSuffix(tc.name, "_eof") {
					frames = frames[:len(frames)-1]
				}
				var calls atomic.Int32
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					w.Header().Set("Content-Type", "text/event-stream")
					w.Header().Set("X-Request-Id", "synthetic-cache")
					for _, frame := range frames {
						wire := fmt.Sprintf("event: %s\ndata: %s\n\n", gjson.Get(frame, "type").String(), frame)
						// Split even an individual data line across HTTP chunks.
						half := len(wire) / 2
						_, _ = fmt.Fprint(w, wire[:half])
						w.(http.Flusher).Flush()
						_, _ = fmt.Fprint(w, wire[half:])
						w.(http.Flusher).Flush()
					}
				}))
				defer upstream.Close()
				client := upstream.Client()
				client.Timeout = 5 * time.Second
				resp, err := client.Post(upstream.URL, "application/json", strings.NewReader(`{"model":"synthetic"}`))
				require.NoError(t, err)
				defer resp.Body.Close()
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
				svc := &OpenAIGatewayService{}
				var result *OpenAIForwardResult
				switch mode {
				case "chat-buffered":
					result, err = svc.handleCCBufferedFromNativeAnthropic(resp, c, "claude-sonnet-4", "claude-sonnet-4", "claude-sonnet-4", nil, time.Now())
				case "chat-stream":
					result, err = svc.handleCCStreamingFromNativeAnthropic(resp, c, "claude-sonnet-4", "claude-sonnet-4", "claude-sonnet-4", nil, time.Now())
				case "responses-buffered":
					result, err = svc.handleResponsesBufferedFromNativeAnthropic(resp, c, "claude-sonnet-4", "claude-sonnet-4", "claude-sonnet-4", nil, time.Now(), apicompat.ResponsesClientToolMapping{})
				case "responses-stream":
					result, err = svc.handleResponsesStreamingFromNativeAnthropic(resp, c, "claude-sonnet-4", "claude-sonnet-4", "claude-sonnet-4", nil, time.Now(), apicompat.ResponsesClientToolMapping{})
				}
				if tc.name == "invalid_late_frame_eof" {
					require.ErrorContains(t, err, "missing terminal event")
					require.NotContains(t, rec.Body.String(), "[DONE]")
					require.NotContains(t, rec.Body.String(), `"type":"response.completed"`)
				} else {
					require.NoError(t, err)
				}
				require.NotNil(t, result)
				require.EqualValues(t, 1, calls.Load(), "a failed metering observation must not replay the provider request")
				require.Equal(t, tc.total, result.Usage.CacheCreationInputTokens)
				require.Equal(t, tc.five, result.Usage.CacheCreation5mTokens)
				require.Equal(t, tc.hour, result.Usage.CacheCreation1hTokens)
				require.Equal(t, tc.known, result.Usage.CacheCreationTTLKnown)
				lastUsage := nativeAnthropicCacheFinalUsage(t, rec.Body.String(), strings.HasSuffix(mode, "-stream"), strings.HasPrefix(mode, "chat-"))
				require.Equal(t, tc.known, lastUsage.Get("cache_creation").Exists(), "wire must use the same raw evidence as billing: %s", lastUsage.Raw)
				if tc.known {
					require.EqualValues(t, tc.five, lastUsage.Get("cache_creation.ephemeral_5m_input_tokens").Int())
					require.EqualValues(t, tc.hour, lastUsage.Get("cache_creation.ephemeral_1h_input_tokens").Int())
				}
			})
		}
	}
}

func nativeAnthropicCacheFinalUsage(t *testing.T, wire string, stream, chat bool) gjson.Result {
	t.Helper()
	if !stream {
		return gjson.Get(wire, "usage")
	}
	var usage gjson.Result
	for _, line := range strings.Split(wire, "\n") {
		data, ok := extractOpenAISSEDataLine(line)
		if !ok || data == "[DONE]" {
			continue
		}
		if chat {
			if next := gjson.Get(data, "usage"); next.Exists() {
				usage = next
			}
		} else if next := gjson.Get(data, "response.usage"); next.Exists() {
			usage = next
		}
	}
	require.True(t, usage.Exists(), "terminal wire usage missing")
	return usage
}
