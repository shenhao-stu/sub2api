package handler

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestOpsErrorLogger_LateJSONFailureAfterKeepalive(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusForbidden, http.StatusTooManyRequests, http.StatusBadGateway} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				setupOpsErrorLogTestQueue(t, 2)
				gin.SetMode(gin.TestMode)
				ops := service.NewOpsService(&ingressRejectOpsRepo{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
				router := gin.New()
				router.Use(OpsErrorLoggerMiddleware(ops))
				router.POST("/v1/responses", func(c *gin.Context) {
					stop := service.StartOpenAIJSONKeepalive(c, time.Second)
					defer stop()
					time.Sleep(1500 * time.Millisecond)
					if status == http.StatusOK {
						service.SetOpsUpstreamError(c, http.StatusTooManyRequests, "earlier rate limit", "")
						c.JSON(status, gin.H{"status": "completed"})
						return
					}
					service.SetOpsUpstreamError(c, status, "final upstream failure", "")
					c.JSON(status, gin.H{"error": gin.H{"type": "upstream_error", "message": "final upstream failure"}})
				})
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/responses", nil))
				require.Equal(t, http.StatusOK, rec.Code)
				require.True(t, strings.HasPrefix(rec.Body.String(), " \n"))
				require.Equal(t, int64(1), OpsErrorLogQueueLength())
				entry := (<-opsErrorLogQueue).entry
				require.Equal(t, status, entry.StatusCode)
				if status == http.StatusOK {
					require.Equal(t, "Recovered upstream error 429: earlier rate limit", entry.ErrorMessage)
				} else {
					require.Equal(t, "final upstream failure", entry.ErrorMessage)
				}
				require.False(t, entry.Stream)
				require.Equal(t, "provider", entry.ErrorOwner)
				require.False(t, entry.IsBusinessLimited)
			})
		})
	}
}

func TestOpsCaptureWriter_LateErrorStateIsBoundedAndReset(t *testing.T) {
	gin.SetMode(gin.TestMode)
	pool := &deterministicOpsCaptureWriterStatePool{}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	w := acquireOpsCaptureWriterFromPool(pool, c.Writer)
	w.WriteHeader(http.StatusOK)
	_, err := w.WriteString(" \n")
	require.NoError(t, err)
	w.WriteHeader(http.StatusTooManyRequests)
	w.WriteHeader(http.StatusBadGateway)
	_, err = w.WriteString(strings.Repeat("x", opsCaptureWriterLimit+1))
	require.NoError(t, err)
	require.Equal(t, http.StatusTooManyRequests, w.capturedErrorStatus(), "first explicit failure wins")
	require.Len(t, w.capturedBytes(), opsCaptureWriterLimit)
	releaseOpsCaptureWriter(w)

	c, _ = gin.CreateTestContext(httptest.NewRecorder())
	reused := acquireOpsCaptureWriterFromPool(pool, c.Writer)
	defer releaseOpsCaptureWriter(reused)
	require.Same(t, w.state, reused.state)
	require.Zero(t, reused.capturedErrorStatus())
	w.WriteHeader(http.StatusInternalServerError)
	require.Zero(t, reused.capturedErrorStatus(), "stale lease cannot change the new request")
	_, err = reused.WriteString(fmt.Sprintf(`{"output":%q}`, strings.Repeat("x", opsCaptureWriterLimit+1)))
	require.NoError(t, err)
	reused.finalizeCapture()
	require.Empty(t, reused.capturedBytes(), "successful JSON must remain unbuffered")
	_, terminal := reused.capturedTerminalError()
	require.False(t, terminal)
}

func TestOpsErrorLogger_ContentPolicySSEStatus(t *testing.T) {
	parsed, ok := parseOpsSSEFailure([]byte("data: {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"code\":\"content_policy_violation\",\"message\":\"Request blocked\"}}\n\n"))
	require.True(t, ok)
	require.Equal(t, http.StatusForbidden, inferStreamFailureStatus(nil, parsed))
}
