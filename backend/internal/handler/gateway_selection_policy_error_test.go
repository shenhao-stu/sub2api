package handler

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestAccountSelectionPolicyError_RejectsClaudeCodeOnly(t *testing.T) {
	for _, path := range []string{"/v1/messages", "/v1/messages/count_tokens"} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%t", path, stream), func(t *testing.T) {
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
					h := &GatewayHandler{}
					require.True(t, h.handleAccountSelectionPolicyError(c, fmt.Errorf("selection: %w", service.ErrClaudeCodeOnly), stream))
				})
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
				if stream {
					require.Equal(t, http.StatusOK, rec.Code)
				} else {
					require.Equal(t, http.StatusForbidden, rec.Code)
				}
				require.Contains(t, rec.Body.String(), `"code":"claude_code_only"`)
				if path == "/v1/messages/count_tokens" {
					require.Zero(t, OpsErrorLogQueueLength(), "default count-token monitoring policy is unchanged")
					return
				}
				require.Equal(t, int64(1), OpsErrorLogQueueLength())
				entry := (<-opsErrorLogQueue).entry
				require.Equal(t, http.StatusForbidden, entry.StatusCode)
				require.Equal(t, "client", entry.ErrorOwner)
				require.True(t, entry.IsBusinessLimited)
				require.Nil(t, entry.UpstreamStatusCode)
			})
		}
	}
}

func TestAccountSelectionPolicyError_OtherResultsRemainUnchanged(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, err := range []error{nil, service.ErrNoAvailableAccounts, errors.New("database unavailable"), errors.New(service.ErrClaudeCodeOnly.Error())} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		h := &GatewayHandler{}
		require.False(t, h.handleAccountSelectionPolicyError(c, err, false))
		require.False(t, c.Writer.Written())
		require.False(t, service.HasOpsClientBusinessLimited(c))
	}
}
