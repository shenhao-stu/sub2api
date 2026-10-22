//go:build unit

package service

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestAnthropicPoolUnavailableCarriesNativePolicyExclusion(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		for _, matching := range []bool{false, true} {
			t.Run(fmt.Sprintf("passthrough=%t/matching=%t", passthrough, matching), func(t *testing.T) {
				account := newAnthropicAPIKeyAccountForTest()
				account.Extra["anthropic_passthrough"] = passthrough
				account.Credentials["temp_unschedulable_enabled"] = true
				account.Credentials["temp_unschedulable_rules"] = []any{map[string]any{
					"error_code": float64(503), "keywords": []any{"pool_unavailable"},
					"duration_minutes": float64(10), "scope": "account",
				}}
				body := `{"error":{"type":"overloaded_error","message":"capacity busy"}}`
				if matching {
					body = `{"error":{"type":"overloaded_error","code":"pool_unavailable","message":"号池当前没有可用账号"}}`
				}
				repo := &walletForwardPolicyRepo{upstreamQuotaRepo: upstreamQuotaRepo{account: account}}
				cfg := &config.Config{}
				upstream := &queuedHTTPUpstreamStub{responses: []*http.Response{newOpenAIUpstreamErrorResponse(503, body)}}
				svc := &GatewayService{cfg: cfg, httpUpstream: upstream, rateLimitService: NewRateLimitService(repo, nil, cfg, nil, nil), deferredService: &DeferredService{}, tlsFPProfileService: &TLSFingerprintProfileService{}}
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
				parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(`{"model":"claude-haiku-4-5-20251001","max_tokens":16,"messages":[{"role":"user","content":"hello"}]}`)), PlatformAnthropic)
				require.NoError(t, err)
				result, err := svc.Forward(context.Background(), c, account, parsed)
				var failure *UpstreamFailoverError
				require.ErrorAs(t, err, &failure)
				require.Nil(t, result)
				require.Equal(t, matching, failure.ExcludeFromRetry)
				require.True(t, failure.ShouldRetryNextAccount())
				require.False(t, failure.RetryableOnSameAccount)
				require.Equal(t, 1, upstream.callCount)
				require.Equal(t, map[bool]int{true: 1, false: 0}[matching], repo.tempCalls)
				require.Empty(t, repo.limitedModels)
				require.Zero(t, repo.setErrorCalls)
				require.False(t, c.Writer.Written())
			})
		}
	}
}
