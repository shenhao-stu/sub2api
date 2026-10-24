package service

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestConversationGuardForwardDispatchesOnceAcrossProtocols(t *testing.T) {
	for _, path := range []string{"/v1/responses", "/v1/chat/completions", "/v1/messages"} {
		for _, status := range []int{403, 429, 503} {
			t.Run(fmt.Sprintf("%s/%d", path, status), func(t *testing.T) {
				body := []byte(`{"model":"gpt-5.4","max_tokens":32,"input":"synthetic","messages":[{"role":"user","content":"synthetic"}],"stream":false}`)
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
				code := map[int]string{403: "conversation_account_restricted", 429: "conversation_quota_exhausted", 503: "conversation_account_paused"}[status]
				upstream := &httpUpstreamRecorder{resp: newOpenAIUpstreamErrorResponse(status, fmt.Sprintf(`{"error":{"code":%q,"message":"pinned conversation unavailable"}}`, code))}
				repo := &tempUnschedulableOpenAIAccountRepo{}
				s := &OpenAIGatewayService{cfg: &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{AllowInsecureHTTP: true}}}, httpUpstream: upstream, rateLimitService: NewRateLimitService(repo, nil, &config.Config{}, nil, nil)}
				a := &Account{ID: 465, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Concurrency: 1, Credentials: map[string]any{"api_key": "sk-synthetic", "base_url": "http://upstream.example", "pool_mode": true, "model_provider": "env-openai"}}
				var result *OpenAIForwardResult
				var err error
				switch path {
				case "/v1/responses":
					result, err = s.Forward(context.Background(), c, a, body)
				case "/v1/messages":
					result, err = s.ForwardAsAnthropic(context.Background(), c, a, body, "", "")
				default:
					result, err = s.ForwardAsChatCompletions(context.Background(), c, a, body, "", "")
				}
				var failover *UpstreamFailoverError
				require.ErrorAs(t, err, &failover)
				require.Nil(t, result)
				require.Len(t, upstream.requests, 1)
				require.False(t, failover.ShouldRetryNextAccount())
				require.False(t, failover.RetryableOnSameAccount)
				require.False(t, failover.ShouldReportAccountScheduleFailure())
				require.Zero(t, repo.modelRateLimitAccountID)
				require.False(t, IsResponseCommitted(c))
			})
		}
	}
}

func TestConversationGuardStopsRetriesWithoutPoisoningPool(t *testing.T) {
	for code, status := range map[string]int{
		"conversation_route_unavailable": 503, "conversation_account_paused": 503,
		"conversation_account_unavailable": 403, "conversation_account_restricted": 403,
		"conversation_model_disabled": 403, "conversation_quota_exhausted": 429,
		"conversation_rate_limited": 429, "proxy_unavailable": 503,
	} {
		t.Run(code, func(t *testing.T) {
			body := []byte(fmt.Sprintf(`{"error":{"code":%q,"message":"original conversation is unavailable"}}`, code))
			e := newOpenAIUpstreamFailoverError(status, http.Header{"Retry-After": {"60"}}, body, "", true)
			require.False(t, e.RetryableOnSameAccount)
			require.False(t, e.ShouldRetryNextAccount())
			require.False(t, e.ShouldReportAccountScheduleFailure())
			require.Equal(t, GatewayFailureScopeRequest, e.Scope)
			require.Equal(t, status, e.StatusCode)
			require.Equal(t, body, e.ResponseBody)
			require.Equal(t, "60", e.ResponseHeaders.Get("Retry-After"))
			// No repository or rate limiter is needed for a request-local guard.
			s := &OpenAIGatewayService{}
			require.False(t, s.handleOpenAIAccountUpstreamError(context.Background(), &Account{Platform: PlatformOpenAI}, status, nil, body))
		})
	}
}

func TestConversationGuardDoesNotDisableOrdinaryRecovery(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
	}{
		{429, `{"error":{"type":"concurrency","message":"busy"}}`},
		{503, `{"error":{"code":"overloaded"}}`},
		{429, `{"error":{"code":"proxy_unavailable"}}`},
		{503, `{"error":{"message":"proxy_unavailable"}}`},
		{503, `{"metadata":{"error":{"code":"proxy_unavailable"}}}`},
		{503, `{"error":{"message":"conversation_account_paused"}}`},
		{429, `{"error":{"code":"conversation_account_paused"}}`},
		{503, `{"metadata":{"error":{"code":"conversation_account_paused"}}}`},
		{503, `{"error":{"code":"conversation_account_paused"}`},
	} {
		e := newOpenAIUpstreamFailoverError(tc.status, nil, []byte(tc.body), "", true)
		require.True(t, e.RetryableOnSameAccount, tc.body)
		require.True(t, e.ShouldRetryNextAccount(), tc.body)
	}
}
