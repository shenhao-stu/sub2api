package handler

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestAnthropicProviderCapacityError(t *testing.T) {
	const response = `{"type":"error","error":{"type":"upstream_error","message":"No available account in group 'auth' (all disabled, cooling down, or busy), please retry later"}}`
	for _, tc := range []struct {
		name, platform, body, header string
		status, wait                 int
		stream                       bool
	}{
		{"pool", service.PlatformAnthropic, response, "", 429, 30, false},
		{"upstream delay", service.PlatformAnthropic, response, "74", 429, 74, false},
		{"bad delay", service.PlatformAnthropic, response, "wrong", 429, 30, false},
		{"stream committed", service.PlatformAnthropic, response, "", 429, 30, true},
		{"generic throttle", service.PlatformAnthropic, `{"type":"error","error":{"type":"rate_limit_error","message":"Too many requests."}}`, "", 429, 0, false},
		{"non-error payload", service.PlatformAnthropic, `{"type":"message","error":{"type":"upstream_error","message":"No available account in group 'auth' (all disabled, cooling down, or busy), please retry later"}}`, "", 429, 0, false},
		{"different status", service.PlatformAnthropic, response, "", 503, 0, false},
		{"other platform", service.PlatformGemini, response, "", 429, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, w := newGinContextForEndpoint(t, EndpointMessages)
			if tc.stream {
				c.Writer.WriteHeaderNow()
			}
			(&GatewayHandler{}).handleFailoverExhausted(c, &service.UpstreamFailoverError{StatusCode: tc.status, ResponseBody: []byte(tc.body), ResponseHeaders: http.Header{"Retry-After": {tc.header}}}, tc.platform, tc.stream)
			if tc.stream {
				require.Equal(t, http.StatusOK, w.Code)
				require.Empty(t, w.Result().Header.Get("Retry-After"))
				require.Contains(t, w.Body.String(), "upstream_pool_busy")
				return
			}
			require.Equal(t, tc.status, w.Code)
			if tc.wait == 0 {
				require.Empty(t, w.Header().Get("Retry-After"))
				require.NotEqual(t, "upstream_pool_busy", gjson.GetBytes(w.Body.Bytes(), "error.code").String())
				return
			}
			require.Equal(t, strconv.Itoa(tc.wait), w.Header().Get("Retry-After"))
			require.Equal(t, "rate_limit_error", gjson.GetBytes(w.Body.Bytes(), "error.type").String())
			require.Equal(t, "upstream_pool_busy", gjson.GetBytes(w.Body.Bytes(), "error.code").String())
		})
	}
}
