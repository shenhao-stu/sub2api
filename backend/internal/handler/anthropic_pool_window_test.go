package handler

import (
	"net/http"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestAnthropicPoolWindowExhaustedUsesStablePoolMessage(t *testing.T) {
	const body = `{"error":{"type":"rate_limit_error","code":"upstream_rate_limit","message":"号池账号均被上游限流，稍后再试"}}`
	for _, committed := range []bool{false, true} {
		c, rec := newGinContextForEndpoint(t, "/v1/messages")
		if committed {
			c.Header("Content-Type", "text/event-stream")
			c.Writer.Flush()
		}
		err := &service.UpstreamFailoverError{
			StatusCode: http.StatusTooManyRequests, ResponseBody: []byte(body),
			ResponseHeaders: http.Header{"Retry-After": {"1260"}}, Reason: service.AnthropicPoolWindowCooldownReason,
			ExcludeFromRetry: true,
		}
		(&GatewayHandler{}).handleFailoverExhausted(c, err, service.PlatformAnthropic, committed)
		require.Contains(t, rec.Body.String(), "All available accounts are currently rate-limited. Please retry later.")
		require.NotContains(t, rec.Body.String(), "usage")
		if committed {
			require.Equal(t, 200, rec.Code)
			require.Contains(t, rec.Body.String(), `"status_code":429`)
		} else {
			require.Equal(t, 429, rec.Code)
			require.Equal(t, "1260", rec.Header().Get("Retry-After"))
		}
	}
}
