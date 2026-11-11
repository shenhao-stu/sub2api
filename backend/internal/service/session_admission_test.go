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
				require.Equal(t, 400, c.Writer.Status())
				require.Contains(t, rec.Body.String(), `"code":"upstream_session_not_accepted"`)
				require.Contains(t, rec.Body.String(), `"status_code":400`)
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

func TestSessionPolicyBlockStopsAiroamRetry(t *testing.T) {
	const body = `{"type":"error","error":{"type":"upstream_error","message":"当前会话因内容审核违规已被拉黑，请使用 /new 开启新会话，或联系管理员解除。","help":"https://private.invalid/"}}`
	for _, tc := range []struct {
		name        string
		passthrough bool
		committed   bool
		metered     bool
	}{
		{name: "http with no usage"},
		{name: "sse with observed usage", passthrough: true, committed: true, metered: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
			if tc.committed {
				c.Header("Content-Type", "text/event-stream")
				_, _ = c.Writer.WriteString(": ping\n\n")
				c.Writer.Flush()
			}
			payload := body
			if tc.metered {
				payload = strings.TrimSuffix(payload, "}") + `,"usage":{"input_tokens":12,"output_tokens":7}}`
			}
			up := &queuedHTTPUpstreamStub{responses: []*http.Response{{StatusCode: 429, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(payload))}}}
			s := newForwardPartialUsageServiceForTest(nil)
			s.httpUpstream = up
			account := newAnthropicAPIKeyAccountForTest()
			account.Credentials["pool_mode"] = true
			account.Extra["anthropic_passthrough"] = tc.passthrough
			parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(fmt.Sprintf(`{"model":"claude-opus-5-5","stream":%t,"messages":[{"role":"user","content":"synthetic"}]}`, tc.committed))), PlatformAnthropic)
			require.NoError(t, err)
			result, err := s.Forward(context.Background(), c, account, parsed)
			require.Error(t, err)
			var failover *UpstreamFailoverError
			require.False(t, errors.As(err, &failover))
			require.Equal(t, 1, up.callCount)
			if tc.metered {
				require.NotNil(t, result)
				require.Equal(t, 12, result.Usage.InputTokens)
				require.Equal(t, 7, result.Usage.OutputTokens)
				require.Equal(t, 1, strings.Count(rec.Body.String(), "event: error"))
			} else {
				require.Nil(t, result)
				require.Equal(t, 403, rec.Code)
			}
			require.Contains(t, rec.Body.String(), `"code":"upstream_session_policy_blocked"`)
			require.Contains(t, rec.Body.String(), `"status_code":403`)
			require.NotContains(t, rec.Body.String(), "private.invalid")
			events, _ := c.Get(OpsUpstreamErrorsKey)
			require.Equal(t, 429, events.([]*OpsUpstreamErrorEvent)[0].UpstreamStatusCode)
		})
	}
}
