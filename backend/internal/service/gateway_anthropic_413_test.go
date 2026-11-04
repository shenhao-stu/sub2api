package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Exercise the actual Forward path with an upstream HTTP response, rather than
// passing the expected status directly to the error writer. Bodies are synthetic.
func TestAnthropicForwardHTTP413PreservesStatusWithoutReplay(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		for _, payload := range []string{
			`{"type":"error","error":{"type":"request_too_large","message":"private upstream https://private.invalid/request?token=synthetic-secret","status_code":502}}`,
			`<html>413 Request Entity Too Large https://private.invalid/ synthetic-secret</html>`,
		} {
			for _, mode := range []string{"uncommitted_json", "uncommitted_stream", "sse_ping_committed", "json_space_committed"} {
				t.Run(fmt.Sprintf("passthrough=%t/html=%t/%s", passthrough, strings.HasPrefix(payload, "<"), mode), func(t *testing.T) {
					gin.SetMode(gin.TestMode)
					rec := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(rec)
					c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
					ready := make(chan struct{})
					close(ready)
					up := &anthropicWaitingUpstream{release: ready, response: &http.Response{
						StatusCode: http.StatusRequestEntityTooLarge, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(payload)),
					}}
					svc := newForwardPartialUsageServiceForTest(&anthropicHTTPUpstreamRecorder{})
					svc.httpUpstream = up
					svc.cfg.Gateway.StreamKeepaliveInterval = 1
					svc.cfg.Gateway.AnthropicNonstreamKeepaliveInterval = 1
					repo := &gatewayForwardErrorPolicyRepoStub{}
					svc.rateLimitService = NewRateLimitService(repo, nil, svc.cfg, nil, nil)
					account := newAnthropicOAuthAccountForPartialUsageTest()
					if passthrough {
						account = newAnthropicAPIKeyAccountForTest()
					}
					stream := mode == "uncommitted_stream" || mode == "sse_ping_committed"
					committed := mode == "sse_ping_committed" || mode == "json_space_committed"
					if committed {
						require.True(t, svc.initAnthropicKeepalive(c, account, stream))
						frame, contentType := " \n", "application/json"
						if stream {
							frame, contentType = anthropicPingFrame, "text/event-stream"
						}
						c.Header("Content-Type", contentType)
						n, err := c.Writer.WriteString(frame)
						require.NoError(t, err)
						recordAnthropicKeepalive(c, n)
						c.Writer.Flush()
					}
					parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(fmt.Sprintf(`{"model":"claude-sonnet-4-5","stream":%t,"messages":[{"role":"user","content":"synthetic"}]}`, stream))), PlatformAnthropic)
					require.NoError(t, err)
					result, err := svc.Forward(context.Background(), c, account, parsed)
					require.Error(t, err)
					require.Nil(t, result, "an HTTP rejection must not invent billable usage")
					require.Equal(t, int32(1), up.calls.Load())
					var failover *UpstreamFailoverError
					require.NotErrorAs(t, err, &failover)
					require.True(t, IsResponseCommitted(c), "handler must not append its generic 502 fallback")
					require.Zero(t, repo.tempCalls)
					require.Zero(t, repo.overloadCalls)
					require.Empty(t, repo.modelRateLimitCalls)
					body := rec.Body.String()
					t.Logf("wire_status=%d body=%s", rec.Code, body)
					if committed {
						require.Equal(t, http.StatusOK, rec.Code)
						if stream {
							var found bool
							_, body, found = strings.Cut(body, "event: error\ndata: ")
							require.True(t, found)
							require.Equal(t, 1, strings.Count(rec.Body.String(), "event: error"))
						}
						require.Equal(t, int64(http.StatusRequestEntityTooLarge), gjson.Get(body, "error.status_code").Int())
						marker, ok := GetOpsStreamError(c)
						require.True(t, ok)
						require.Equal(t, http.StatusRequestEntityTooLarge, marker.IntendedStatus)
					} else {
						require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
					}
					require.True(t, gjson.Valid(body), "exactly one error JSON document")
					require.Equal(t, "error", gjson.Get(body, "type").String())
					require.Equal(t, "request_too_large", gjson.Get(body, "error.type").String())
					require.Equal(t, "Request body too large", gjson.Get(body, "error.message").String())
					require.NotContains(t, body, "private.invalid")
					require.NotContains(t, body, "synthetic-secret")
				})
			}
		}
	}
}
