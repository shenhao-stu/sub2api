//go:build unit

package handler

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	middleware "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestGeminiPaymentHTTP402MustNotBecome429(t *testing.T) {
	for _, path := range []string{"/v1/chat/completions", "/v1/responses", "/v1/messages"} {
		t.Run(path, func(t *testing.T) {
			group := &service.Group{ID: 54, Platform: service.PlatformGemini, Hydrated: true, Status: service.StatusActive, RateMultiplier: 1}
			account := &service.Account{ID: 811, Platform: service.PlatformGemini, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Concurrency: 4, GroupIDs: []int64{54}, AccountGroups: []service.AccountGroup{{AccountID: 811, GroupID: 54}}, Credentials: map[string]any{"api_key": "fixture"}}
			h, cleanup := newTestGatewayHandler(t, group, []*service.Account{account})
			defer cleanup()
			upstream := &grokMediaSlotUpstream{call: func(req *http.Request, _ int64) (*http.Response, error) {
				return &http.Response{StatusCode: 402, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"error":{"code":402,"status":"RESOURCE_EXHAUSTED","message":"Insufficient credits"}}`))}, nil
			}}
			h.geminiCompatService = service.NewGeminiMessagesCompatService(nil, nil, nil, nil, nil, nil, upstream, nil, &config.Config{})
			body := `{"model":"gemini-4-argon","messages":[{"role":"user","content":"fixture"}],"max_tokens":16}`
			if path == "/v1/responses" {
				body = `{"model":"gemini-4-argon","input":"fixture"}`
			}
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)).WithContext(context.WithValue(context.Background(), ctxkey.Group, group))
			key := &service.APIKey{ID: 20, UserID: 10, GroupID: &group.ID, Group: group, User: &service.User{ID: 10, Balance: 100}}
			c.Set(string(middleware.ContextKeyAPIKey), key)
			c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 10, Concurrency: 10})
			switch path {
			case "/v1/chat/completions":
				h.ChatCompletions(c)
			case "/v1/responses":
				h.Responses(c)
			default:
				h.Messages(c)
			}
			require.Equal(t, 402, rec.Code, rec.Body.String())
			require.Equal(t, 1, upstream.calls, "a payment rejection is not a same-account retry")
			require.NotContains(t, rec.Body.String(), "rate_limit_error")
		})
	}
}

func TestGeminiPaymentHTTPFailoverBillsOnlySuccessfulAccount(t *testing.T) {
	for _, path := range []string{"/v1/chat/completions", "/v1/responses", "/v1/messages"} {
		for _, stream := range []bool{false, true} {
			for _, scenario := range []string{"none", "vertex", "canceled"} {
				fallback := scenario != "none"
				t.Run(fmt.Sprintf("%s/stream=%t/scenario=%s", path, stream, scenario), func(t *testing.T) {
					requestCtx, cancel := context.WithCancel(context.Background())
					defer cancel()
					group := &service.Group{ID: 54, Platform: service.PlatformGemini, Hydrated: true, Status: service.StatusActive, RateMultiplier: 1}
					ai := &service.Account{ID: 811, Platform: service.PlatformGemini, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Priority: 10, Concurrency: 4, GroupIDs: []int64{54}, AccountGroups: []service.AccountGroup{{AccountID: 811, GroupID: 54}}, Credentials: map[string]any{"api_key": "fixture", "pool_mode": true, "pool_mode_retry_count": 4}}
					vertex := &service.Account{ID: 812, Platform: service.PlatformGemini, Type: service.AccountTypeServiceAccount, Status: service.StatusActive, Schedulable: true, Priority: 20, Concurrency: 4, GroupIDs: []int64{54}, AccountGroups: []service.AccountGroup{{AccountID: 812, GroupID: 54}}, Credentials: map[string]any{"auth_mode": "apikey", "api_key": "fixture-vertex", "location": "global", "tier_id": "vertex"}}
					accounts := []*service.Account{ai}
					if fallback {
						accounts = append(accounts, vertex)
					}
					h, cleanup := newTestGatewayHandler(t, group, accounts)
					defer cleanup()
					cfg := &config.Config{}
					cfg.Default.RateMultiplier = 1
					logs := &cachedBillingUsageRepo{}
					billing := &mediaCompletionBillingRepo{apply: func(ctx context.Context) error { require.NoError(t, ctx.Err()); return nil }}
					snapshot := service.NewSchedulerSnapshotService(&fakeSchedulerCache{accounts: accounts}, nil, nil, nil, nil)
					h.gatewayService = service.NewGatewayService(nil, &fakeGroupRepo{group: group}, logs, billing, nil, nil, nil, nil, cfg, snapshot, nil, service.NewBillingService(cfg, nil), nil, nil, nil, nil, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
					var hits []int64
					upstream := &grokMediaSlotUpstream{call: func(req *http.Request, id int64) (*http.Response, error) {
						hits = append(hits, id)
						requestBody, err := io.ReadAll(req.Body)
						require.NoError(t, err)
						require.Contains(t, string(requestBody), "payment-fixture")
						if id == 811 {
							if scenario == "canceled" {
								cancel()
							}
							return &http.Response{StatusCode: 402, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"error":{"code":402,"status":"RESOURCE_EXHAUSTED","message":"Your prepayment credits are depleted. Please go to AI Studio at https://provider.example/billing"}}`))}, nil
						}
						require.Equal(t, int64(812), id)
						body := `{"candidates":[{"content":{"role":"model","parts":[{"text":"payment-fixture-ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":5,"totalTokenCount":15}}`
						if req.URL.Query().Get("alt") == "sse" {
							body = "data: " + body + "\n\n"
						}
						return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, nil
					}}
					h.geminiCompatService = service.NewGeminiMessagesCompatService(nil, nil, nil, nil, nil, nil, upstream, nil, cfg)
					body := fmt.Sprintf(`{"model":"gemini-4-argon","stream":%t,"max_tokens":16,"messages":[{"role":"user","content":"payment-fixture"}]}`, stream)
					if path == "/v1/responses" {
						body = fmt.Sprintf(`{"model":"gemini-4-argon","stream":%t,"input":"payment-fixture"}`, stream)
					}
					rec := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(rec)
					c.Request = httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)).WithContext(context.WithValue(requestCtx, ctxkey.Group, group))
					key := &service.APIKey{ID: 20, UserID: 10, GroupID: &group.ID, Group: group, User: &service.User{ID: 10, Balance: 100}}
					c.Set(string(middleware.ContextKeyAPIKey), key)
					c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 10, Concurrency: 10})
					switch path {
					case "/v1/chat/completions":
						h.ChatCompletions(c)
					case "/v1/responses":
						h.Responses(c)
					default:
						h.Messages(c)
					}
					if scenario == "canceled" {
						require.Equal(t, []int64{811}, hits)
						require.Empty(t, billing.commands)
					} else if fallback {
						require.Equal(t, []int64{811, 812}, hits, "exactly one rejected attempt before the eligible credential")
						require.Equal(t, 200, rec.Code, rec.Body.String())
						require.Contains(t, rec.Body.String(), "payment-fixture-ok")
						require.Len(t, billing.commands, 1)
						require.Equal(t, 10, billing.commands[0].InputTokens)
						require.Equal(t, 5, billing.commands[0].OutputTokens)
						require.Equal(t, int64(812), logs.first().AccountID)
					} else {
						require.Equal(t, []int64{811}, hits, "pool policy cannot retry an explicitly depleted wallet")
						require.Equal(t, 402, rec.Code, rec.Body.String())
						require.Contains(t, rec.Body.String(), "prepaid credits are depleted")
						require.NotContains(t, rec.Body.String(), "provider.example")
						require.Empty(t, billing.commands)
					}
				})
			}
		}
	}
}
