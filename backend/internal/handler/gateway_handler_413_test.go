package handler

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestGatewayFailoverExhaustedHTTP413PreservesStatus(t *testing.T) {
	for _, streamStarted := range []bool{false, true} {
		t.Run(map[bool]string{false: "uncommitted", true: "sse_ping_committed"}[streamStarted], func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			if streamStarted {
				c.Header("Content-Type", "text/event-stream")
				_, _ = c.Writer.WriteString("event: ping\ndata: {}\n\n")
				c.Writer.Flush()
			}
			h := &GatewayHandler{}
			h.handleFailoverExhausted(c, &service.UpstreamFailoverError{
				StatusCode:   http.StatusRequestEntityTooLarge,
				ResponseBody: []byte(`{"error":{"message":"https://private.invalid/ synthetic-secret"}}`),
			}, service.PlatformAnthropic, streamStarted)
			body := rec.Body.String()
			if streamStarted {
				require.Equal(t, http.StatusOK, rec.Code)
				_, body, _ = strings.Cut(body, "event: error\ndata: ")
				require.Equal(t, int64(http.StatusRequestEntityTooLarge), gjson.Get(body, "error.status_code").Int())
			} else {
				require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
			}
			require.Equal(t, "request_too_large", gjson.Get(body, "error.type").String())
			require.Equal(t, "Request body too large", gjson.Get(body, "error.message").String())
			require.NotContains(t, body, "private.invalid")
			require.NotContains(t, body, "synthetic-secret")
		})
	}
}
