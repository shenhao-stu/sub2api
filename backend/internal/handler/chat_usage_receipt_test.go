//go:build unit

package handler

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type chatHandlerReceiptRepo struct {
	openAIWSUsageHandlerUsageLogRepoStub
	mu           sync.Mutex
	claim        *service.ChatUsageReceiptClaim
	receipt      *service.UsageReceipt
	billingID    string
	prepareError error
}

func (r *chatHandlerReceiptRepo) PrepareChatUsageReceipt(context.Context, *service.ChatUsageReceiptClaim) error {
	return r.prepareError
}
func (r *chatHandlerReceiptRepo) ReleaseChatUsageReceipt(context.Context, *service.ChatUsageReceiptClaim) error {
	return nil
}

func (r *chatHandlerReceiptRepo) ClaimChatUsageReceipt(_ context.Context, c *service.ChatUsageReceiptClaim) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.claim != nil {
		return false, nil
	}
	copy := *c
	r.claim = &copy
	return true, nil
}
func (r *chatHandlerReceiptRepo) CompleteChatUsageReceipt(_ context.Context, c *service.ChatUsageReceiptClaim, a int64, b string, v *service.UsageReceipt) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	copy := *v
	r.receipt = &copy
	r.billingID = b
	return nil
}
func (r *chatHandlerReceiptRepo) GetUsageReceipt(_ context.Context, key int64, id string) (*service.UsageReceipt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.receipt == nil || r.claim.APIKeyID != key || r.claim.RequestID != id {
		return nil, service.ErrUsageLogNotFound
	}
	copy := *r.receipt
	return &copy, nil
}

func TestChatReceiptActualHandlerNegotiation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, mode := range []string{"normal", "queue_headers_committed", "no_account", "invalid_id", "kimi_unsupported", "journal_capacity"} {
		t.Run(mode, func(t *testing.T) {
			accounts := []service.Account{{ID: 19930, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Credentials: map[string]any{"api_key": "synthetic"}, Extra: map[string]any{"openai_responses_supported": true}}}
			if mode == "kimi_unsupported" {
				accounts[0].Platform = service.PlatformKimi
				accounts[0].Credentials["api_protocol"] = "chat_completions"
			}
			if mode == "no_account" {
				accounts = nil
			}
			accountRepo := &grokPartialUsageAccountRepo{grokStreamFailoverAccountRepo: grokStreamFailoverAccountRepo{openAIWSFailoverHandlerAccountRepoStub: openAIWSFailoverHandlerAccountRepoStub{accounts: accounts}}}
			repo := &chatHandlerReceiptRepo{openAIWSUsageHandlerUsageLogRepoStub: openAIWSUsageHandlerUsageLogRepoStub{created: make(chan *service.UsageLog, 3)}}
			if mode == "journal_capacity" {
				repo.prepareError = errors.New("synthetic full durable journal")
			}
			billing := &monitorCyberBillingRepo{commands: make(chan *service.UsageBillingCommand, 3)}
			users := &monitorCyberUserRepo{}
			cfg := &config.Config{}
			cfg.Default.RateMultiplier = 1
			cfg.Gateway.MaxAccountSwitches = 0
			cache := service.NewBillingCacheService(nil, users, nil, nil, nil, nil, cfg, nil)
			t.Cleanup(cache.Stop)
			upstream := &grokMediaSlotUpstream{call: func(req *http.Request, _ int64) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("data: {\"type\":\"response.output_text.delta\",\"delta\":\"synthetic\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":100,\"output_tokens\":7,\"input_tokens_details\":{\"cached_tokens\":90}}}}\n\n"))}, nil
			}}
			gateway := service.NewOpenAIGatewayService(accountRepo, repo, billing, users, nil, nil, nil, cfg, nil, nil, service.NewBillingService(cfg, nil), nil, cache, upstream, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil)
			h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(nil), cache, service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
			read := &GatewayHandler{usageService: service.NewUsageService(repo, nil, nil, nil)}
			router := gin.New()
			router.Use(middleware.ClientRequestID())
			router.Use(func(c *gin.Context) {
				groupID := int64(14230)
				c.Set(string(middleware.ContextKeyAPIKey), &service.APIKey{ID: 11830, UserID: 11730, GroupID: &groupID, User: &service.User{ID: 11730, Status: service.StatusActive, Balance: 100}, Group: &service.Group{ID: groupID, Hydrated: true, Platform: service.PlatformOpenAI, Status: service.StatusActive, RateMultiplier: 1}})
				c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 11730})
				if mode == "kimi_unsupported" {
					key, _ := c.Get(string(middleware.ContextKeyAPIKey))
					key.(*service.APIKey).Group.Platform = service.PlatformKimi
				}
				c.Next()
			})
			router.POST(service.ChatReceiptEndpoint, func(c *gin.Context) {
				if mode == "queue_headers_committed" {
					// Real queues run only after the request body has already been read.
					body, _ := io.ReadAll(c.Request.Body)
					c.Request.Body = io.NopCloser(strings.NewReader(string(body)))
					c.Header("Content-Type", "text/event-stream")
					c.Writer.WriteString(": queued\n\n")
					c.Writer.Flush()
				}
				h.ChatCompletions(c)
			})
			router.GET("/v1/sub2api/usage/:request_id", read.KeyUsageReceipt)
			server := httptest.NewServer(router)
			defer server.Close()
			id := uuid.NewString()
			if mode == "invalid_id" {
				id = "client-spoof"
			}
			var lastBody string
			send := func() *http.Response {
				body := `{"model":"gemini-4-argon","messages":[{"role":"user","content":"synthetic"}],"stream":true}`
				if mode == "kimi_unsupported" {
					body = strings.Replace(body, "gemini-4-argon", "kimi-k3", 1)
				}
				req, err := http.NewRequest("POST", server.URL+service.ChatReceiptEndpoint, strings.NewReader(body))
				require.NoError(t, err)
				req.Header.Set(service.ChatReceiptVersionHeader, "v2")
				req.Header.Set(service.ChatReceiptIDHeader, id)
				resp, err := server.Client().Do(req)
				require.NoError(t, err)
				data, _ := io.ReadAll(resp.Body)
				lastBody = string(data)
				resp.Body.Close()
				return resp
			}
			resp := send()
			if mode == "journal_capacity" {
				require.Equal(t, 503, resp.StatusCode)
				require.Empty(t, resp.Header.Get(service.ChatReceiptVersionHeader))
				require.Empty(t, resp.Header.Get(service.ChatReceiptIDHeader))
				require.Nil(t, repo.claim)
				require.Nil(t, repo.receipt)
				require.Zero(t, upstream.calls)
				require.Zero(t, len(billing.commands))
				require.Zero(t, len(repo.created))
				return
			}
			if mode == "kimi_unsupported" {
				require.Equal(t, 400, resp.StatusCode)
				require.Contains(t, lastBody, "unsupported_metering_protocol")
				require.Nil(t, repo.claim)
				require.Nil(t, repo.receipt)
				require.Zero(t, upstream.calls)
				require.Zero(t, len(billing.commands))
				require.Zero(t, len(repo.created))
				require.Empty(t, resp.Header.Get(service.ChatReceiptVersionHeader))
				return
			}

			if mode == "no_account" || mode == "invalid_id" {
				require.GreaterOrEqual(t, resp.StatusCode, 400)
				require.Empty(t, resp.Header.Get(service.ChatReceiptVersionHeader))
				require.Nil(t, repo.claim)
				require.Zero(t, upstream.calls)
				return
			}
			require.Equal(t, 200, resp.StatusCode)
			if mode == "normal" {
				require.Equal(t, "v2", resp.Header.Get(service.ChatReceiptVersionHeader))
				require.Equal(t, id, resp.Header.Get(service.ChatReceiptIDHeader))
			} else {
				require.Empty(t, resp.Header.Get(service.ChatReceiptVersionHeader))
			}
			require.NotNil(t, repo.claim, "upstream calls=%d", upstream.calls)
			require.NotNil(t, repo.receipt, "upstream calls=%d billing=%s", upstream.calls, repo.billingID)
			fetched, err := server.Client().Get(server.URL + "/v1/sub2api/usage/" + id)
			require.NoError(t, err)
			data, _ := io.ReadAll(fetched.Body)
			fetched.Body.Close()
			require.Equal(t, 200, fetched.StatusCode)
			require.Contains(t, string(data), `"output_tokens":7`)
			require.NotContains(t, string(data), "billing_request_id")
			require.NotContains(t, string(data), "account_id")
			require.NotContains(t, string(data), `"claim"`)
			require.Equal(t, 1, upstream.calls)
			if mode == "normal" {
				again := send()
				require.Equal(t, 409, again.StatusCode)
				require.Empty(t, again.Header.Get(service.ChatReceiptVersionHeader))
				require.Equal(t, 1, upstream.calls)
			}
		})
	}
}
