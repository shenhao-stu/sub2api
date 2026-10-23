//go:build unit

package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestGeminiEmptyCompletionOpsAfterHeartbeat(t *testing.T) {
	setupOpsErrorLogTestQueue(t, 2)
	ops := service.NewOpsService(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	r := gin.New()
	r.Use(OpsErrorLoggerMiddleware(ops))
	r.POST("/v1/chat/completions", func(c *gin.Context) {
		setOpsRequestContext(c, "gemini-4-argon", true)
		service.SetOpsUpstreamError(c, 200, "no content; native_finish_reason=stop", "")
		c.Header("Content-Type", "text/event-stream")
		_, _ = c.Writer.WriteString(": ping\n\n")
		c.Writer.Flush()
		_, _ = c.Writer.WriteString(`data: {"error":{"type":"upstream_error","code":"upstream_empty_completion","status_code":502,"message":"no content; native_finish_reason=stop"},"usage":{"input_tokens":100,"output_tokens":0}}` + "\n\n")
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))
	require.Equal(t, 200, w.Code)
	require.Equal(t, int64(1), OpsErrorLogQueueLength())
	job := <-opsErrorLogQueue
	require.Equal(t, 502, job.entry.StatusCode)
	require.Contains(t, job.entry.ErrorMessage, "native_finish_reason=stop")
	require.False(t, job.entry.IsBusinessLimited)
}
