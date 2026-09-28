package handler

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestFailoverExhausted_UpstreamQuotaClassification(t *testing.T) {
	for _, path := range []string{"/v1/messages", "/v1/responses", "/v1/chat/completions"} {
		for _, stream := range []bool{false, true} {
			for _, quota := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/stream=%t/quota=%t", path, stream, quota), func(t *testing.T) {
					setupOpsErrorLogTestQueue(t, 2)
					gin.SetMode(gin.TestMode)
					ops := service.NewOpsService(&ingressRejectOpsRepo{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
					router := gin.New()
					router.Use(OpsErrorLoggerMiddleware(ops))
					router.POST(path, func(c *gin.Context) {
						if stream {
							c.Header("Content-Type", "text/event-stream")
							_, _ = c.Writer.WriteString(": ping\n\n")
							c.Writer.Flush()
						}
						body := []byte(`{"error":{"code":"rate_limit_exceeded","message":"Rate limit exceeded"}}`)
						if quota {
							body = []byte(`{"error":{"code":"insufficient_quota","message":"Insufficient account balance"}}`)
						}
						failure := &service.UpstreamFailoverError{StatusCode: http.StatusTooManyRequests, ResponseBody: body}
						if path == "/v1/messages" {
							(&GatewayHandler{}).handleFailoverExhausted(c, failure, service.PlatformAnthropic, stream)
						} else {
							(&OpenAIGatewayHandler{}).handleFailoverExhausted(c, failure, stream)
						}
					})
					rec := httptest.NewRecorder()
					router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
					if stream {
						require.Equal(t, http.StatusOK, rec.Code)
					} else {
						require.Equal(t, http.StatusTooManyRequests, rec.Code)
					}
					if quota {
						require.Contains(t, rec.Body.String(), `"code":"insufficient_quota"`)
						require.Contains(t, rec.Body.String(), "Upstream account quota is exhausted, please contact administrator")
						require.NotContains(t, rec.Body.String(), "retry later")
					} else {
						require.Contains(t, rec.Body.String(), "Upstream rate limit exceeded, please retry later")
						require.NotContains(t, rec.Body.String(), "insufficient_quota")
					}
					require.Equal(t, int64(1), OpsErrorLogQueueLength())
					entry := (<-opsErrorLogQueue).entry
					require.Equal(t, http.StatusTooManyRequests, entry.StatusCode)
					require.Equal(t, "provider", entry.ErrorOwner)
					require.False(t, entry.IsBusinessLimited)
					require.NotNil(t, entry.UpstreamStatusCode)
					require.Equal(t, http.StatusTooManyRequests, *entry.UpstreamStatusCode)
				})
			}
		}
	}
}
