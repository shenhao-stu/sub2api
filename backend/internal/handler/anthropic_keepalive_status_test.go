package handler

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Synthetic terminal payload only: this invokes the real error writer and Ops
// middleware, never a provider or a recorded customer request.
func TestAnthropicLateHTTPErrorPreservesOpsStatus(t *testing.T) {
	for _, status := range []int{400, 413, 429, 503, 502} {
		for _, conflictingStatus := range []bool{false, true} {
			for _, mode := range []string{"uncommitted", "sse_ping_committed", "json_space_committed"} {
				t.Run(fmt.Sprintf("status=%d/conflict=%t/%s", status, conflictingStatus, mode), func(t *testing.T) {
					setupOpsErrorLogTestQueue(t, 2)
					gin.SetMode(gin.TestMode)
					ops := service.NewOpsService(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
					router := gin.New()
					router.Use(OpsErrorLoggerMiddleware(ops))
					router.POST("/v1/messages", func(c *gin.Context) {
						c.Set(opsAccountIDKey, int64(1))
						c.Set(opsModelKey, "claude-sonnet-4-5")
						c.Set(opsStreamKey, mode == "sse_ping_committed")
						if mode == "sse_ping_committed" {
							c.Header("Content-Type", "text/event-stream")
							_, _ = c.Writer.WriteString("event: ping\ndata: {\"type\":\"ping\"}\n\n")
							c.Writer.Flush()
						} else if mode == "json_space_committed" {
							c.Header("Content-Type", "application/json")
							_, _ = c.Writer.WriteString(" \n")
							c.Writer.Flush()
						}
						service.SetOpsUpstreamError(c, status, "native_request_consumed_no_replay", "")
						errorObject := gin.H{"type": "api_error", "code": "native_consumed", "message": "native_request_consumed_no_replay"}
						payload := gin.H{"type": "error", "error": errorObject}
						if conflictingStatus {
							errorObject["status_code"] = 418
							payload["status_code"] = 418
						}
						service.WriteAnthropicErrorResponse(c, status, payload)
						if mode != "uncommitted" {
							marker, ok := service.GetOpsStreamError(c)
							require.True(t, ok)
							require.Equal(t, status, marker.IntendedStatus)
							require.Equal(t, mode == "json_space_committed", marker.NonStream)
						}
					})
					recorder := httptest.NewRecorder()
					router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/messages", nil))
					wireStatus := http.StatusOK
					if mode == "uncommitted" {
						wireStatus = status
					}
					require.Equal(t, wireStatus, recorder.Code)
					require.Equal(t, int64(1), OpsErrorLogQueueLength())
					job := <-opsErrorLogQueue
					require.Equal(t, mode == "sse_ping_committed", job.entry.Stream, "JSON keepalive errors must not be recorded as streamed requests")
					if mode != "uncommitted" {
						body := recorder.Body.String()
						if mode == "sse_ping_committed" {
							_, body, _ = strings.Cut(body, "event: error\ndata: ")
						}
						require.Equal(t, int64(status), gjson.Get(body, "error.status_code").Int())
						require.Equal(t, "api_error", gjson.Get(body, "error.type").String())
						require.Equal(t, "native_consumed", gjson.Get(body, "error.code").String())
						require.Equal(t, "native_request_consumed_no_replay", gjson.Get(body, "error.message").String())
					}
					require.NotNil(t, job.entry.UpstreamStatusCode)
					require.Equal(t, status, *job.entry.UpstreamStatusCode, "known final upstream HTTP status must not be inferred from generic api_error")
					require.Equal(t, status, job.entry.StatusCode, "visible request failure status should preserve actual upstream HTTP failure")
				})
			}
		}
	}
}
