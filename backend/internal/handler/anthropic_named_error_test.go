package handler

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestAnthropicErrorAfterCommentHasEventName(t *testing.T) {
	for _, path := range []string{"/v1/messages", "/antigravity/v1/messages"} {
		t.Run(path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, path, nil)
			_, err := c.Writer.WriteString(": keepalive\n\n")
			require.NoError(t, err)
			c.Writer.Flush()
			h := &GatewayHandler{}
			h.handleStreamingAwareErrorWithCode(c, http.StatusTooManyRequests, "rate_limit_error", "queue_full", "Retry later", true)
			require.True(t, strings.HasPrefix(rec.Body.String(), ": keepalive\n\nevent: error\ndata: "))
			require.Contains(t, rec.Body.String(), `"code":"queue_full"`)
			require.NotEmpty(t, service.GetOpsStreamErrors(c))
			require.NotContains(t, rec.Body.String(), "message_stop")
		})
	}
}
