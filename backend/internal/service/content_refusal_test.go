package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestContentRefusalAnthropicForwardStopsWithoutLosingUsage(t *testing.T) {
	for _, measured := range []bool{false, true} {
		for _, committed := range []bool{false, true} {
			t.Run(strings.Join([]string{map[bool]string{true: "metered", false: "unmetered"}[measured], map[bool]string{true: "sse", false: "json"}[committed]}, "/"), func(t *testing.T) {
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
				if committed {
					c.Header("Content-Type", "text/event-stream")
					_, _ = c.Writer.WriteString(": ping\n\n")
					c.Writer.Flush()
				}
				body := `{"type":"error","error":{"type":"content_refusal","message":"This conversation is blocked by provider policy. See https://api.derouter.ai/v1/help?token=private or //derouter.ai/help or support.derouter.ai/help","request_id":"private-provider-id"}`
				if measured {
					body += `,"usage":{"input_tokens":12,"output_tokens":7,"cache_read_input_tokens":5,"cache_creation":{"ephemeral_1h_input_tokens":3}}`
				}
				body += "}"
				up := &anthropicHTTPUpstreamRecorder{resp: &http.Response{StatusCode: 403, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}}
				s := &GatewayService{cfg: &config.Config{}, httpUpstream: up}
				s.cfg.Gateway.LogUpstreamErrorBody = true
				s.cfg.Gateway.LogUpstreamErrorBodyMaxBytes = 2048
				a := newAnthropicAPIKeyAccountForTest()
				a.Credentials["pool_mode"] = true
				result, err := s.forwardAnthropicAPIKeyPassthrough(context.Background(), c, a, []byte(`{"model":"claude-sonnet-5-5","messages":[{"role":"user","content":"synthetic"}]}`), "claude-sonnet-5-5", "claude-sonnet-5-5", committed, time.Now())
				require.Error(t, err)
				var failover *UpstreamFailoverError
				require.False(t, errors.As(err, &failover), "policy denial must not switch or retry accounts")
				if measured {
					require.NotNil(t, result)
					require.Equal(t, 12, result.Usage.InputTokens)
					require.Equal(t, 7, result.Usage.OutputTokens)
					require.Equal(t, 5, result.Usage.CacheReadInputTokens)
					require.Equal(t, 3, result.Usage.CacheCreation1hTokens)
					require.Contains(t, rec.Body.String(), `"ephemeral_1h_input_tokens":3`)
				} else {
					require.Nil(t, result)
				}
				want := 403
				if committed {
					want = 200
					require.Equal(t, 1, strings.Count(rec.Body.String(), "event: error"))
				}
				require.Equal(t, want, rec.Code)
				require.Contains(t, rec.Body.String(), `"type":"content_refusal"`)
				require.Contains(t, rec.Body.String(), `"status_code":403`)
				require.NotContains(t, rec.Body.String(), "private-provider-id")
				require.NotContains(t, strings.ToLower(rec.Body.String()), "derouter.ai")
				require.NotContains(t, rec.Body.String(), "token=private")
				privateEvents, ok := c.Get(OpsUpstreamErrorsKey)
				require.True(t, ok)
				events := privateEvents.([]*OpsUpstreamErrorEvent)
				require.Len(t, events, 1)
				require.Equal(t, "private-provider-id", events[0].UpstreamRequestID)
				require.Contains(t, events[0].Detail, "private-provider-id")
			})
		}
	}
}

func TestContentRefusalDoesNotDisableAuthenticationRecovery(t *testing.T) {
	for _, body := range []string{`{"error":{"type":"permission_error"}}`, `{"error":{"message":"content_refusal"}}`, `{"error":{"type":"content_refusal"}`, `{"error":{"type":"content_refusal"}} trailing`} {
		require.False(t, isContentRefusal(403, []byte(body)))
	}
	require.False(t, isContentRefusal(502, []byte(`{"error":{"type":"content_refusal"}}`)))
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
	body := `{"error":{"type":"permission_error","message":"credentials invalid"}}`
	resp := &http.Response{StatusCode: 403, Body: io.NopCloser(strings.NewReader(body))}
	s := &GatewayService{}
	result, err, handled := s.handleContentRefusalResponse(c, newAnthropicAPIKeyAccountForTest(), resp, "model", "model", false, time.Now())
	require.False(t, handled)
	require.NoError(t, err)
	require.Nil(t, result)
	require.False(t, c.Writer.Written())
	remaining, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, body, string(remaining))
}
