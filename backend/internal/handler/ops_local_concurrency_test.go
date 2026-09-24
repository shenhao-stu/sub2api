package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestOpsResponsesLocalConcurrencyRemains429(t *testing.T) {
	for _, code := range []string{gatewayConcurrencyLimitCode, gatewayQueueFullCode} {
		t.Run(code, func(t *testing.T) {
			setupOpsErrorLogTestQueue(t, 4)
			gin.SetMode(gin.TestMode)
			ops := service.NewOpsService(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
			r := gin.New()
			r.Use(OpsErrorLoggerMiddleware(ops))
			r.POST("/v1/responses", func(c *gin.Context) {
				c.Header("Content-Type", "text/event-stream")
				_, _ = c.Writer.WriteString(": ping\n\n")
				c.Writer.Flush()
				h := &OpenAIGatewayHandler{}
				h.handleStreamingAwareErrorWithCode(c, http.StatusTooManyRequests, "rate_limit_error", code, "Concurrency limit exceeded for account, please retry later", true, false)
			})
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/responses", nil))
			require.Equal(t, http.StatusOK, rec.Code)
			require.Equal(t, int64(1), OpsErrorLogQueueLength())
			job := <-opsErrorLogQueue
			require.Equal(t, http.StatusTooManyRequests, job.entry.StatusCode)
			require.Equal(t, "rate_limit_error", job.entry.ErrorType)
			require.Equal(t, "request", job.entry.ErrorPhase)
			require.True(t, job.entry.IsBusinessLimited)
			require.Nil(t, job.entry.UpstreamStatusCode)
			require.Nil(t, job.entry.UpstreamErrorMessage)
		})
	}
}
