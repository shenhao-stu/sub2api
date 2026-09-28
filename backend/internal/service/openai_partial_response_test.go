//go:build unit

package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAINonStreamingReadFailureRetainsUsage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const jsonBody = `{"id":"resp_known","object":"response","status":"completed","model":"grok-4.6","output":[],"usage":{"input_tokens":123,"output_tokens":7}}`
	const sseBody = "data: {\"type\":\"response.in_progress\",\"response\":{\"id\":\"resp_known\",\"model\":\"grok-4.6\",\"usage\":{\"input_tokens\":123,\"output_tokens\":7}}}\n\n"
	for _, protocol := range []string{"responses", "passthrough", "chat"} {
		for _, test := range []struct {
			name, contentType, body string
			limit                   int64
			wantUsage               bool
		}{
			{"json", "application/json", jsonBody, 0, true},
			{"sse", "text/event-stream", sseBody, 0, true},
			{"sse_cut_tail", "text/event-stream", sseBody + `data: {"usage":{"input_tokens":999`, 0, true},
			{"sse_size_limit", "text/event-stream", sseBody + strings.Repeat("x", 100), int64(len(sseBody) + 10), true},
			{"incomplete_json", "application/json", strings.TrimSuffix(jsonBody, "}"), 0, false},
			{"unterminated_sse", "text/event-stream", strings.TrimSuffix(sseBody, "\n"), 0, false},
			{"zero_usage", "application/json", `{"usage":{"input_tokens":0,"output_tokens":0}}`, 0, false},
		} {
			t.Run(protocol+"/"+test.name, func(t *testing.T) {
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", test.contentType)
					w.Header().Set("Content-Length", fmt.Sprint(len(test.body)+100))
					_, _ = io.WriteString(w, test.body)
				}))
				t.Cleanup(upstream.Close)
				resp, err := upstream.Client().Get(upstream.URL)
				require.NoError(t, err)
				defer resp.Body.Close()
				recorder := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(recorder)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
				account := &Account{ID: 1, Platform: PlatformGrok, Type: AccountTypeAPIKey}
				svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{UpstreamResponseReadMaxBytes: test.limit}}}
				var usage *OpenAIUsage
				switch protocol {
				case "responses":
					result, forwardErr := svc.handleNonStreamingResponse(context.Background(), resp, c, account, "grok-4.6", "grok-4.6")
					err = forwardErr
					if result != nil {
						usage = result.usage
					}
				case "passthrough":
					result, forwardErr := svc.handleNonStreamingResponsePassthrough(context.Background(), resp, c, account, "grok-4.6", "grok-4.6")
					err = forwardErr
					if result != nil {
						usage = result.usage
					}
				case "chat":
					result, forwardErr := svc.bufferRawChatCompletions(c, resp, account, "grok-4.6", "grok-4.6", "grok-4.6", nil, nil, time.Now())
					err = forwardErr
					if result != nil {
						usage = &result.Usage
					}
				}
				require.Error(t, err)
				var failover *UpstreamFailoverError
				require.False(t, errors.As(err, &failover))
				if !test.wantUsage {
					require.Nil(t, usage, "incomplete or zero evidence must not invent billed tokens")
					return
				}
				require.NotNil(t, usage)
				require.Equal(t, 123, usage.InputTokens)
				require.Equal(t, 7, usage.OutputTokens)
				require.Equal(t, http.StatusBadGateway, recorder.Code)
				require.Equal(t, int64(123), gjson.Get(recorder.Body.String(), "usage.input_tokens").Int())
				require.Equal(t, "grok-4.6", observedUpstreamResponseModel(c))
			})
		}
	}
}

func TestPartialOpenAIUsageTerminalAndFrameBoundaries(t *testing.T) {
	for _, test := range []struct {
		name, body    string
		input, output int
	}{
		{"crlf_complete", "data: {\"usage\":{\"input_tokens\":12,\"output_tokens\":3}}\r\n\r\n", 12, 3},
		{"truncated_frame", "data: {\"usage\":{\"input_tokens\":12,\"output_tokens\":3}}\r\n\r", 0, 0},
		{"terminal_authoritative", "data: {\"type\":\"response.in_progress\",\"response\":{\"usage\":{\"input_tokens\":12,\"output_tokens\":3}}}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":10,\"output_tokens\":5}}}\n\n", 10, 5},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			svc := &OpenAIGatewayService{}
			usage := svc.partialOpenAIResponseUsage(c, []byte(test.body))
			require.Equal(t, test.input, usage.InputTokens)
			require.Equal(t, test.output, usage.OutputTokens)
		})
	}
}

func TestGrokForwardPartialReadPreservesResponseModel(t *testing.T) {
	const body = `{"id":"resp_known","object":"response","status":"completed","model":"grok-4.7","output":[],"usage":{"input_tokens":123,"output_tokens":7}}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", fmt.Sprint(len(body)+100))
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(upstream.Close)
	destination, err := url.Parse(upstream.URL)
	require.NoError(t, err)
	svc := openAIClientToolsTestService(nil)
	svc.httpUpstream = &grokRefusalHTTPUpstream{client: upstream.Client(), url: destination}
	account := &Account{ID: 253901, Platform: PlatformGrok, Type: AccountTypeAPIKey,
		Credentials: map[string]any{"api_key": "offline-only", "base_url": "https://api.x.ai/v1"}}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	result, err := svc.Forward(c.Request.Context(), c, account, []byte(`{"model":"grok-4.6","input":"offline fixture","stream":false}`))
	require.Error(t, err)
	require.NotNil(t, result)
	require.Equal(t, 123, result.Usage.InputTokens)
	require.Equal(t, 7, result.Usage.OutputTokens)
	require.Equal(t, "grok-4.6", result.UpstreamModel)
	require.Equal(t, "grok-4.7", result.UpstreamResponseModel)
	require.False(t, result.UpstreamResponseModelConflict)
}
