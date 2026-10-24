//go:build unit

package handler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/internal/testutil"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const proxyGuardBody = `{"error":{"type":"server_error","code":"proxy_unavailable","message":"账号出口暂不可用；请稍后重试 https://private.fixture.invalid/path sk-privatefixture"}}`

type proxyGuardLoopUpstream struct {
	service.HTTPUpstream
	calls           atomic.Int32
	reservationSeen atomic.Bool
	cache           *handlerInflightCache
}

func (u *proxyGuardLoopUpstream) Do(*http.Request, string, int64, int) (*http.Response, error) {
	u.calls.Add(1)
	if u.cache.count() > 0 {
		u.reservationSeen.Store(true)
	}
	return &http.Response{StatusCode: 503, Header: http.Header{"Content-Type": {"application/json"}, "Retry-After": {"60"}, "X-Request-Id": {"synthetic-upstream-ref"}}, Body: io.NopCloser(bytes.NewBufferString(proxyGuardBody))}, nil
}

type proxyGuardBillingSpy struct {
	service.UsageBillingRepository
	calls atomic.Int32
}

func (s *proxyGuardBillingSpy) Apply(context.Context, *service.UsageBillingCommand) (*service.UsageBillingApplyResult, error) {
	s.calls.Add(1)
	return &service.UsageBillingApplyResult{}, nil
}

func TestOpenAIProxyGuardHTTPHandlerPreserves503WithoutReplayOrCharge(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, path := range []string{"/v1/responses", "/v1/chat/completions", "/v1/messages"} {
		for _, pool := range []bool{false, true} {
			for _, stream := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/pool=%t/stream=%t", path, pool, stream), func(t *testing.T) {
					groupID := int64(3131)
					accounts := []service.Account{}
					for i := 0; i < 2; i++ {
						accounts = append(accounts, service.Account{ID: int64(465 + i), Name: "synthetic-bridge", Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Concurrency: 1, Priority: i + 1, Credentials: map[string]any{"api_key": "sk-fixture", "base_url": "https://upstream.fixture.invalid", "pool_mode": pool, "pool_mode_retry_count": float64(1), "pool_mode_retry_status_codes": []any{float64(503)}}, Extra: map[string]any{"openai_passthrough": true}})
					}
					repo := &grokCredentialHandlerRepo{accounts: accounts}
					cfg := &config.Config{}
					cfg.Default.RateMultiplier = 1
					cfg.Gateway.MaxAccountSwitches = 1
					cfg.Billing.InflightReservation = config.InflightReservationConfig{Enabled: true, TTLSeconds: 60}
					cache := newHandlerInflightCache(100)
					billingCache := service.NewBillingCacheService(cache, nil, nil, nil, nil, nil, cfg, nil)
					t.Cleanup(billingCache.Stop)
					bills := &proxyGuardBillingSpy{}
					logs := &openAIWSUsageHandlerUsageLogRepoStub{created: make(chan *service.UsageLog, 8)}
					upstream := &proxyGuardLoopUpstream{cache: cache}
					gateway := service.NewOpenAIGatewayService(repo, logs, bills, nil, nil, nil, testutil.StubGatewayCache{}, cfg, nil, nil, service.NewBillingService(cfg, nil), nil, billingCache, upstream, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil)
					h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(nil), billingCache, service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
					apiKey := &service.APIKey{ID: 99, GroupID: &groupID, User: &service.User{ID: 100, Status: service.StatusActive, Balance: 100}, Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI, Status: service.StatusActive, RateMultiplier: 1, AllowMessagesDispatch: true}}
					var ops *gin.Context
					router := gin.New()
					router.Use(func(c *gin.Context) {
						ops = c
						c.Set(string(middleware.ContextKeyAPIKey), apiKey)
						c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 100, Concurrency: 0})
						c.Next()
					})
					router.POST("/v1/responses", h.Responses)
					router.POST("/v1/chat/completions", h.ChatCompletions)
					router.POST("/v1/messages", h.Messages)
					body := fmt.Sprintf(`{"model":"gpt-5.1","input":"synthetic","messages":[{"role":"user","content":"synthetic"}],"max_tokens":32,"max_output_tokens":32,"stream":%t}`, stream)
					req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
					req.Header.Set("Content-Type", "application/json")
					rec := httptest.NewRecorder()
					router.ServeHTTP(rec, req)
					t.Logf("response_status=%d upstream_attempts=%d reservation_seen=%t active_reservations=%d billing_calls=%d", rec.Code, upstream.calls.Load(), upstream.reservationSeen.Load(), cache.count(), bills.calls.Load())
					require.Equal(t, 503, rec.Code, rec.Body.String())
					require.Equal(t, "proxy_unavailable", gjson.GetBytes(rec.Body.Bytes(), "error.code").String())
					require.Equal(t, int64(503), gjson.GetBytes(rec.Body.Bytes(), "error.status_code").Int())
					require.Contains(t, gjson.GetBytes(rec.Body.Bytes(), "error.message").String(), "账号出口暂不可用")
					require.NotContains(t, rec.Body.String(), "private.fixture.invalid")
					require.NotContains(t, rec.Body.String(), "sk-privatefixture")
					require.Equal(t, "60", rec.Header().Get("Retry-After"))
					require.Equal(t, int32(1), upstream.calls.Load(), "a request-local route guard must not replay or select a sibling bridge")
					require.True(t, upstream.reservationSeen.Load(), "exercise an actual in-flight reservation")
					require.Zero(t, cache.count())
					require.Zero(t, bills.calls.Load())
					require.Empty(t, logs.created)
					balance, err := cache.GetUserBalance(context.Background(), 100)
					require.NoError(t, err)
					require.Equal(t, float64(100), balance)
					require.Empty(t, repo.setErrorIDs)
					require.Empty(t, repo.setTempIDs)
					require.Empty(t, repo.rateLimitIDs)
					require.NotNil(t, ops)
				})
			}
		}
	}
}
