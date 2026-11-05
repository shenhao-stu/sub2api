package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const sessionAdmissionTestBody = `{"type":"error","error":{"type":"upstream_error","message":"This group only accepts sessions first seen here.","help":"https://private.invalid/?key=secret"}}`

func TestSessionAdmissionForwardIsTerminal(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		for _, metered := range []bool{false, true} {
			t.Run(fmt.Sprintf("passthrough=%t/metered=%t", passthrough, metered), func(t *testing.T) {
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
				up := &queuedHTTPUpstreamStub{responses: []*http.Response{{StatusCode: 429, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(sessionAdmissionBody(metered)))}}}
				s := newForwardPartialUsageServiceForTest(nil)
				s.httpUpstream = up
				account := newAnthropicAPIKeyAccountForTest()
				account.Credentials["pool_mode"] = true
				account.Extra["anthropic_passthrough"] = passthrough
				parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(`{"model":"claude-opus-5-5","messages":[{"role":"user","content":"synthetic"}]}`)), PlatformAnthropic)
				require.NoError(t, err)
				result, err := s.Forward(context.Background(), c, account, parsed)
				require.Error(t, err)
				var failover *UpstreamFailoverError
				require.False(t, errors.As(err, &failover))
				require.Equal(t, 1, up.callCount)
				require.Equal(t, 429, c.Writer.Status())
				require.Contains(t, rec.Body.String(), `"code":"upstream_session_not_accepted"`)
				require.Contains(t, rec.Body.String(), `"status_code":429`)
				require.NotContains(t, rec.Body.String(), "private.invalid")
				require.NotContains(t, rec.Body.String(), "secret")
				require.True(t, account.Schedulable)
				require.Nil(t, account.TempUnschedulableUntil)
				if metered {
					require.NotNil(t, result)
					require.Equal(t, 12, result.Usage.InputTokens)
					require.Equal(t, 7, result.Usage.OutputTokens)
					require.Equal(t, 5, result.Usage.CacheReadInputTokens)
					require.Equal(t, 2, result.Usage.CacheCreation5mTokens)
					require.Equal(t, 3, result.Usage.CacheCreation1hTokens)
				} else {
					require.Nil(t, result)
				}
			})
		}
	}
}
