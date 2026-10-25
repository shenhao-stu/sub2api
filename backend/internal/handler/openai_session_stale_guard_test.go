//go:build unit

package handler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/internal/testutil"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const sessionStaleBody = `{"error":{"type":"invalid_request_error","code":"conversation_session_stale","message":"private https://private.fixture.invalid/x 10.12.34.56 user@example.invalid Bearer synthetic-secret","param":"private_identifier","status_code":418}}`

type sessionStaleUpstream struct{ proxyGuardLoopUpstream }

func (u *sessionStaleUpstream) Do(*http.Request, string, int64, int) (*http.Response, error) {
	u.calls.Add(1)
	if u.cache.count() > 0 {
		u.reservationSeen.Store(true)
	}
	return &http.Response{StatusCode: 409, Header: http.Header{"Content-Type": {"application/json"}, "Retry-After": {"60"}}, Body: io.NopCloser(bytes.NewBufferString(u.body))}, nil
}
func TestSessionStale409FullHTTPHandler(t *testing.T) {
	for _, retry409 := range []bool{false, true} {
		t.Run(fmt.Sprintf("retry409_%t", retry409), func(t *testing.T) {
			for _, heartbeat := range []bool{false, true} {
				testSessionStale409Handler(t, heartbeat, retry409, true)
			}
		})
	}
}
func testSessionStale409Handler(t *testing.T, heartbeat, retry409, terminalGuard bool) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	for _, path := range []string{"/v1/responses", "/v1/chat/completions", "/v1/messages"} {
		for _, pool := range []bool{false, true} {
			for _, stream := range []bool{false, true} {
				if heartbeat && !stream {
					continue
				}
				t.Run(fmt.Sprintf("%s/pool=%t/stream=%t/heartbeat=%t", path, pool, stream, heartbeat), func(t *testing.T) {
					groupID := int64(3131)
					accounts := []service.Account{}
					for i := 0; i < 2; i++ {
						accounts = append(accounts, service.Account{ID: int64(465 + i), Name: "synthetic-bridge", Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Concurrency: 1, Priority: i + 1, Credentials: map[string]any{"api_key": "sk-fixture", "base_url": "https://upstream.fixture.invalid", "pool_mode": pool, "pool_mode_retry_count": float64(1), "pool_mode_retry_status_codes": []any{float64(503)}}, Extra: map[string]any{"openai_passthrough": true}})
					}
					if retry409 {
						for i := range accounts {
							accounts[i].Credentials["pool_mode_retry_status_codes"] = []any{float64(409), float64(503)}
						}
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
					responseBody := sessionStaleBody
					if !terminalGuard {
						responseBody = `{"error":{"type":"invalid_request_error","code":"ordinary_conflict","message":"ordinary conflict"}}`
					}
					upstream := &sessionStaleUpstream{proxyGuardLoopUpstream: proxyGuardLoopUpstream{cache: cache, body: responseBody}}
					gateway := service.NewOpenAIGatewayService(repo, logs, bills, nil, nil, nil, testutil.StubGatewayCache{}, cfg, nil, nil, service.NewBillingService(cfg, nil), nil, billingCache, upstream, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil)
					h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(nil), billingCache, service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
					apiKey := &service.APIKey{ID: 99, GroupID: &groupID, User: &service.User{ID: 100, Status: service.StatusActive, Balance: 100}, Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI, Status: service.StatusActive, RateMultiplier: 1, AllowMessagesDispatch: true}}
					var ops *gin.Context
					router := gin.New()
					router.Use(func(c *gin.Context) {
						ops = c
						c.Set(string(middleware.ContextKeyAPIKey), apiKey)
						c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 100, Concurrency: 0})
						if heartbeat {
							c.Header("Content-Type", "text/event-stream")
							_, err := c.Writer.WriteString(": ping\n\n")
							require.NoError(t, err)
							c.Writer.Flush()
						}
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
					response, prefix := rec.Body.String(), "error."
					if heartbeat && terminalGuard {
						require.Equal(t, 200, rec.Code)
						streamErr, ok := service.GetOpsStreamError(ops)
						require.True(t, ok)
						require.Equal(t, 409, streamErr.IntendedStatus)
						if path == "/v1/responses" {
							require.Equal(t, 1, strings.Count(response, "event: response.failed\n"))
							response = strings.Split(response, "event: response.failed\ndata: ")[1]
							prefix = "response.error."
						} else {
							response = strings.Split(response, "data: ")[1]
						}
					} else {
						want := 409
						if retry409 && pool && path == "/v1/responses" && !terminalGuard {
							want = 502
						}
						if heartbeat {
							want = 200
						}
						require.Equal(t, want, rec.Code, response)
					}
					if terminalGuard {
						require.Equal(t, "conversation_session_stale", gjson.Get(response, prefix+"code").String(), response)
						if heartbeat {
							require.Equal(t, int64(409), gjson.Get(response, prefix+"status_code").Int(), response)
						}

						require.NotContains(t, rec.Body.String(), "private.fixture.invalid")
						require.NotContains(t, rec.Body.String(), "sk-privatefixture")
						for _, private := range []string{"10.12.34.56", "user@example.invalid", "synthetic-secret", "private_identifier", "418"} {
							require.NotContains(t, rec.Body.String(), private)
						}
						require.Equal(t, "原会话与账号当前授权或出口已不一致，请新建会话；本次未切换账号。", gjson.Get(response, prefix+"message").String())
						if !heartbeat {
							require.Equal(t, "60", rec.Header().Get("Retry-After"))
						}

					}
					wantAttempts := int32(1)
					if !terminalGuard && retry409 && pool && path == "/v1/responses" {
						wantAttempts = 4
					}
					require.Equal(t, wantAttempts, upstream.calls.Load())

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

func TestSessionStaleGuardDoesNotChangeOrdinary409HTTPHandler(t *testing.T) {
	for _, retry409 := range []bool{false, true} {
		t.Run(fmt.Sprintf("retry409_%t", retry409), func(t *testing.T) {
			for _, heartbeat := range []bool{false, true} {
				testSessionStale409Handler(t, heartbeat, retry409, false)
			}
		})
	}
}
