package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestGatewayForwardSSEErrorClassifiesFailureAndRetainsUsage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		errorType string
		status    int
	}{
		{"invalid_request_error", 400},
		{"authentication_error", 401},
		{"permission_error", 403},
		{"not_found_error", 404},
		{"request_too_large", 413},
		{"rate_limit_error", 429},
		{"api_error", 500},
		{"upstream_error", 502},
		{"unknown_error", 502},
		{"", 502},
		{"overloaded_error", 529},
	} {
		for _, metered := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/metered=%t", tc.errorType, metered), func(t *testing.T) {
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
				parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(`{"model":"claude-sonnet-4-5","stream":true,"messages":[{"role":"user","content":"test"}]}`)), PlatformAnthropic)
				require.NoError(t, err)
				body := ""
				if metered {
					body = "event: message_start\ndata: " +
						`{"type":"message_start","message":{"usage":{"input_tokens":11}}}` + "\n\n"
				}
				body += "event: error\ndata: " +
					fmt.Sprintf(`{"type":"error","error":{"type":%q,"message":"upstream stopped"}}`, tc.errorType) + "\n\n"
				upstream := &anthropicHTTPUpstreamRecorder{resp: &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
					Body:       io.NopCloser(strings.NewReader(body)),
				}}
				svc := newForwardPartialUsageServiceForTest(upstream)
				repo := &gatewayForwardErrorPolicyRepoStub{}
				svc.rateLimitService = NewRateLimitService(repo, nil, svc.cfg, nil, nil)
				result, err := svc.Forward(context.Background(), c, newAnthropicOAuthAccountForPartialUsageTest(), parsed)
				require.Error(t, err)
				events := c.MustGet(OpsUpstreamErrorsKey).([]*OpsUpstreamErrorEvent)
				require.Len(t, events, 1)
				require.Equal(t, tc.status, events[0].UpstreamStatusCode)
				var failover *UpstreamFailoverError
				if metered {
					require.False(t, errors.As(err, &failover), "metered attempts must not be replayed")
					require.NotNil(t, result)
					require.Equal(t, 11, result.Usage.InputTokens)
					require.True(t, IsResponseCommitted(c))
					require.Equal(t, 1, strings.Count(rec.Body.String(), "event: error\n"))
					require.Zero(t, repo.overloadCalls)
				} else {
					require.ErrorAs(t, err, &failover)
					require.Equal(t, tc.status, failover.StatusCode)
					require.Nil(t, result)
					require.Empty(t, rec.Body.String())
				}
			})
		}
	}
}
