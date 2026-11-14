package service

import (
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

const poolWindowFixture = `{"error":{"type":"rate_limit_error","code":"upstream_rate_limit","message":"号池账号均被上游限流，稍后再试"}}`

func TestAnthropicPoolWindowRejectionDisablesSamePoolReplay(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	upstream := &anthropicHTTPUpstreamRecorder{
		resp: &http.Response{StatusCode: http.StatusTooManyRequests,
			Header: http.Header{"Retry-After": {"1260"}, "X-Request-Id": {"e3bf0d88-0104-4e8d-82b5-2b5437cfcd9d"}},
			Body:   io.NopCloser(strings.NewReader(poolWindowFixture))},
	}
	svc := &GatewayService{cfg: &config.Config{}, httpUpstream: upstream}
	account := newAnthropicAPIKeyAccountForTest()
	account.Credentials["pool_mode"] = true
	body := []byte(`{"model":"claude-fable-5-2","max_tokens":64,"messages":[{"role":"user","content":"synthetic"}]}`)
	result, err := svc.forwardAnthropicAPIKeyPassthrough(t.Context(), c, account, body, "claude-fable-5-2", "claude-fable-5-2", false, time.Now())
	var failure *UpstreamFailoverError
	require.ErrorAs(t, err, &failure)
	require.Nil(t, result)
	require.Equal(t, AnthropicPoolWindowCooldownReason, failure.Reason)
	require.True(t, failure.ExcludeFromRetry)
	require.False(t, failure.RetryableOnSameAccount)
	require.True(t, failure.ShouldRetryNextAccount())
	require.Equal(t, 1260, failure.AnthropicPoolRetryAfter())
	require.False(t, c.Writer.Written())
	raw, _ := c.Get(OpsUpstreamErrorsKey)
	events := raw.([]*OpsUpstreamErrorEvent)
	require.Len(t, events, 1)
	require.Equal(t, "e3bf0d88-0104-4e8d-82b5-2b5437cfcd9d", events[0].UpstreamRequestID)
	require.Equal(t, 429, events[0].UpstreamStatusCode)
}

func TestAnthropicPoolWindowEnvelopeIsNarrow(t *testing.T) {
	require.True(t, isAnthropicPoolWindowRejection([]byte(poolWindowFixture)))
	require.True(t, isAnthropicPoolWindowRejection([]byte(`{"type":"error",`+poolWindowFixture[1:])))
	for _, body := range []string{
		strings.Replace(poolWindowFixture, anthropicPoolWindowMessage, "session_busy", 1),
		strings.Replace(poolWindowFixture, "upstream_rate_limit", "different_rate_limit", 1),
		strings.Replace(poolWindowFixture, `"error":`, `"usage":{"input_tokens":2},"error":`, 1),
		strings.Replace(poolWindowFixture, `"message":`, `"status_code":503,"message":`, 1),
		strings.Replace(poolWindowFixture, `"message":`, `"code":"upstream_rate_limit","message":`, 1),
		poolWindowFixture + "{}",
	} {
		require.False(t, isAnthropicPoolWindowRejection([]byte(body)), "%s", body)
	}
}

func TestAnthropicPoolWindowProbePreservesNonmatchingBody(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	body := strings.Replace(poolWindowFixture, anthropicPoolWindowMessage, "session_busy", 1)
	a := newAnthropicAPIKeyAccountForTest()
	for _, pool := range []bool{false, true} {
		a.Credentials["pool_mode"] = pool
		resp := &http.Response{StatusCode: http.StatusTooManyRequests, Body: io.NopCloser(strings.NewReader(body))}
		require.Nil(t, (&GatewayService{}).anthropicPoolWindowRejection(c, a, resp))
		got, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, body, string(got))
		require.NoError(t, resp.Body.Close())
	}
}
