package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestGatewayForwardMeteredSSEErrorRetainsUsageWithoutReplay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, errorType := range []string{"overloaded_error", "rate_limit_error", "api_error"} {
		t.Run(errorType, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(`{"model":"claude-sonnet-4-5","stream":true,"messages":[{"role":"user","content":"test"}]}`)), PlatformAnthropic)
			require.NoError(t, err)
			body := `event: message_start` + "\n" +
				`data: {"type":"message_start","message":{"id":"msg_metered_error","usage":{"input_tokens":11,"cache_read_input_tokens":7,"cache_creation_input_tokens":3}}}` + "\n\n" +
				`event: message_delta` + "\n" + `data: {"type":"message_delta","usage":{"output_tokens":5}}` + "\n\n" +
				`event: error` + "\n" + `data: {"type":"error","error":{"type":"` + errorType + `","message":"upstream stopped"}}` + "\n\n"
			upstream := &anthropicHTTPUpstreamRecorder{resp: &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "X-Request-Id": []string{"metered-error-request"}},
				Body:       io.NopCloser(strings.NewReader(body)),
			}}
			service := newForwardPartialUsageServiceForTest(upstream)
			result, err := service.Forward(context.Background(), c, newAnthropicOAuthAccountForPartialUsageTest(), parsed)
			require.Error(t, err)
			var failover *UpstreamFailoverError
			require.False(t, errors.As(err, &failover), "metered execution must terminate before a new billable attempt")
			require.NotNil(t, result, "reported usage must reach the existing single-settlement path")
			require.Equal(t, 11, result.Usage.InputTokens)
			require.Equal(t, 7, result.Usage.CacheReadInputTokens)
			require.Equal(t, 3, result.Usage.CacheCreationInputTokens)
			require.Equal(t, 5, result.Usage.OutputTokens)
			require.Equal(t, "metered-error-request", result.RequestID)
			require.Equal(t, 1, strings.Count(recorder.Body.String(), "event: error\n"))
			require.True(t, IsResponseCommitted(c), "the handler must not append a second error frame")
			require.Contains(t, recorder.Body.String(), "upstream stopped")
			require.NotContains(t, recorder.Body.String(), "message_stop")
		})
	}
}
