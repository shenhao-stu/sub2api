package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type anthropicNoReplayUpstream struct {
	anthropicHTTPUpstreamRecorder
	calls int
}

func (u *anthropicNoReplayUpstream) DoWithTLS(req *http.Request, proxy string, accountID int64, concurrency int, profile *tlsfingerprint.Profile) (*http.Response, error) {
	u.calls++
	return u.Do(req, proxy, accountID, concurrency)
}

func TestAnthropicPassthroughHTTP400DoesNotReplayAfterKeepalive(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "uncommitted", true: "sse_ping_committed"}[committed], func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			if committed {
				c.Header("Content-Type", "text/event-stream")
				_, _ = c.Writer.WriteString(anthropicPingFrame)
				c.Writer.Flush()
			}
			const response = `{"type":"error","error":{"type":"api_error","message":"native_request_consumed_no_replay"}}`
			up := &anthropicNoReplayUpstream{anthropicHTTPUpstreamRecorder: anthropicHTTPUpstreamRecorder{resp: &http.Response{
				StatusCode: http.StatusBadRequest, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(response)),
			}}}
			svc := &GatewayService{cfg: &config.Config{}, httpUpstream: up, tlsFPProfileService: &TLSFingerprintProfileService{}}
			repo := &gatewayForwardErrorPolicyRepoStub{}
			svc.rateLimitService = NewRateLimitService(repo, nil, svc.cfg, nil, nil)
			account := newAnthropicAPIKeyAccountForTest()
			result, err := svc.forwardAnthropicAPIKeyPassthrough(context.Background(), c, account, []byte(`{"model":"claude-sonnet-4-5","stream":true,"messages":[{"role":"user","content":"synthetic"}]}`), "claude-sonnet-4-5", "claude-sonnet-4-5", true, time.Now())
			require.Error(t, err)
			require.Nil(t, result)
			require.Equal(t, 1, up.calls)
			require.Zero(t, repo.tempCalls)
			require.Zero(t, repo.overloadCalls)
			require.Empty(t, repo.modelRateLimitCalls)
			var failover *UpstreamFailoverError
			require.NotErrorAs(t, err, &failover)
			require.Contains(t, rec.Body.String(), "native_request_consumed_no_replay")
			if committed {
				require.Equal(t, http.StatusOK, rec.Code)
				marker, ok := GetOpsStreamError(c)
				require.True(t, ok)
				require.Equal(t, http.StatusBadRequest, marker.IntendedStatus)
			} else {
				require.Equal(t, http.StatusBadRequest, rec.Code)
			}
		})
	}
}
