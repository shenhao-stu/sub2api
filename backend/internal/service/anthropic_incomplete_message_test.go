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

func TestAnthropicPoolIncompleteMessageStopsReplay(t *testing.T) {
	const partial = `{"type":"message","role":"assistant","id":"msg_synthetic","model":"claude-opus-5-5","content":[],"stop_reason":null,"usage":{"input_tokens":2,"output_tokens":5,"cache_read_input_tokens":100}}`
	for _, tc := range []struct {
		name      string
		payload   string
		stream    bool
		committed bool
		noReplay  bool
	}{
		{"json", partial, false, false, true},
		{"stream", partial, true, false, true},
		{"committed stream", partial, true, true, true},
		{"generic gateway error", `{"error":{"message":"temporarily unavailable"}}`, true, false, false},
		{"empty usage", strings.Replace(partial, `"input_tokens":2,"output_tokens":5,"cache_read_input_tokens":100`, `"input_tokens":0,"output_tokens":0`, 1), true, false, false},
		{"missing role", strings.Replace(partial, `"role":"assistant",`, "", 1), true, false, false},
		{"complete response shape", strings.Replace(partial, `"stop_reason":null`, `"stop_reason":"end_turn"`, 1), true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			ready := make(chan struct{})
			close(ready)
			upstream := &anthropicWaitingUpstream{release: ready, response: &http.Response{
				StatusCode: http.StatusBadGateway,
				Header:     http.Header{"Content-Type": {"application/json"}},
				Body:       io.NopCloser(strings.NewReader(tc.payload)),
			}}
			svc := newForwardPartialUsageServiceForTest(&anthropicHTTPUpstreamRecorder{})
			svc.httpUpstream = upstream
			repo := &gatewayForwardErrorPolicyRepoStub{}
			svc.rateLimitService = NewRateLimitService(repo, nil, svc.cfg, nil, nil)
			account := newAnthropicAPIKeyAccountForTest()
			account.Credentials["pool_mode"] = true
			if tc.committed {
				svc.cfg.Gateway.StreamKeepaliveInterval = 1
				require.True(t, svc.initAnthropicKeepalive(c, account, true))
				c.Header("Content-Type", "text/event-stream")
				n, err := c.Writer.WriteString(anthropicPingFrame)
				require.NoError(t, err)
				recordAnthropicKeepalive(c, n)
				c.Writer.Flush()
			}
			request := fmt.Sprintf(`{"model":"claude-opus-5-5","stream":%t,"messages":[{"role":"user","content":"synthetic"}]}`, tc.stream)
			parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(request)), PlatformAnthropic)
			require.NoError(t, err)
			result, err := svc.Forward(context.Background(), c, account, parsed)
			require.Error(t, err)
			require.Nil(t, result, "message-start usage is not a final receipt")
			var failover *UpstreamFailoverError
			if !tc.noReplay {
				require.ErrorAs(t, err, &failover)
				require.Equal(t, tc.payload, string(failover.ResponseBody))
				return
			}
			require.NotErrorAs(t, err, &failover)
			require.Equal(t, int32(1), upstream.calls.Load())
			require.True(t, IsResponseCommitted(c))
			require.Zero(t, repo.tempCalls)
			require.Zero(t, repo.overloadCalls)
			require.Empty(t, repo.modelRateLimitCalls)
			body := rec.Body.String()
			if tc.committed {
				require.Equal(t, http.StatusOK, rec.Code)
				_, payload, ok := strings.Cut(body, "event: error\ndata: ")
				require.True(t, ok)
				require.Equal(t, 1, strings.Count(body, "event: error"))
				body = payload
			} else {
				require.Equal(t, http.StatusBadGateway, rec.Code)
			}
			require.Equal(t, "upstream_malformed_response", gjson.Get(body, "error.code").String())
			require.Equal(t, int64(502), gjson.Get(body, "error.status_code").Int())
			require.NotContains(t, body, "msg_synthetic")
			require.False(t, gjson.Get(body, "usage").Exists())
		})
	}
}
