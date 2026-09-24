//go:build unit

package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type monitorCyberUserRepo struct{ service.UserRepository }

func (*monitorCyberUserRepo) GetByID(_ context.Context, id int64) (*service.User, error) {
	return &service.User{ID: id, Status: service.StatusActive, Balance: 100}, nil
}

type monitorCyberBillingRepo struct {
	service.UsageBillingRepository
	commands chan *service.UsageBillingCommand
}

func (r *monitorCyberBillingRepo) Apply(_ context.Context, cmd *service.UsageBillingCommand) (*service.UsageBillingApplyResult, error) {
	r.commands <- cmd
	return &service.UsageBillingApplyResult{Applied: true}, nil
}

// Exercise real handlers in standard billing mode. The cyber audit barrier
// makes a duplicate fallback bill observable without sleeping or guessing.
func TestMonitorCompatCyberBillsOnceWithCacheSplit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, endpoint := range []string{"/v1/chat/completions", "/v1/messages"} {
		t.Run(endpoint, func(t *testing.T) {
			accounts := []service.Account{{
				ID: 19930, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
				Status: service.StatusActive, Schedulable: true,
				Credentials: map[string]any{"api_key": "test-only-token"},
				Extra:       map[string]any{"openai_responses_supported": true},
			}}
			accountRepo := &grokPartialUsageAccountRepo{grokStreamFailoverAccountRepo: grokStreamFailoverAccountRepo{openAIWSFailoverHandlerAccountRepoStub: openAIWSFailoverHandlerAccountRepoStub{accounts: accounts}}}
			usageRepo := &openAIWSUsageHandlerUsageLogRepoStub{created: make(chan *service.UsageLog, 3)}
			billingRepo := &monitorCyberBillingRepo{commands: make(chan *service.UsageBillingCommand, 3)}
			userRepo := &monitorCyberUserRepo{}
			cfg := &config.Config{}
			cfg.Default.RateMultiplier = 1
			billingCache := service.NewBillingCacheService(nil, userRepo, nil, nil, nil, nil, cfg, nil)
			t.Cleanup(billingCache.Stop)
			upstream := &grokPartialUsageUpstream{cyberFailure: true}
			gateway := service.NewOpenAIGatewayService(accountRepo, usageRepo, billingRepo, userRepo, nil, nil, nil, cfg, nil, nil,
				service.NewBillingService(cfg, nil), nil, billingCache, upstream, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil)
			h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(nil), billingCache,
				service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
			opsRepo := &grokPartialUsageOpsRepo{recorded: make(chan struct{}, 1)}
			h.opsService = service.NewOpsService(opsRepo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(`{"model":"gpt-5.1","messages":[{"role":"user","content":"test"}],"max_tokens":32,"stream":true}`))
			groupID := int64(14230)
			c.Set(string(middleware.ContextKeyAPIKey), &service.APIKey{ID: 11830, GroupID: &groupID,
				User:  &service.User{ID: 11730, Status: service.StatusActive, Balance: 100},
				Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI, Status: service.StatusActive, RateMultiplier: 1, AllowMessagesDispatch: true}})
			c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 11730})
			if endpoint == "/v1/messages" {
				h.Messages(c)
			} else {
				h.ChatCompletions(c)
			}
			require.NotEmpty(t, upstream.accountIDs, "fixture did not reach forwarding: status=%d body=%s", rec.Code, rec.Body.String())
			select {
			case <-opsRepo.recorded:
			case <-time.After(3 * time.Second):
				t.Fatal("cyber audit did not reach its completion barrier")
			}
			require.Equal(t, []int64{19930}, upstream.accountIDs)
			require.Len(t, billingRepo.commands, 1, "partial result owns one real billing callback")
			cmd := <-billingRepo.commands
			require.Equal(t, 60, cmd.InputTokens)
			require.Equal(t, 40, cmd.CacheReadTokens)
			require.Equal(t, 7, cmd.OutputTokens)
			require.Greater(t, cmd.BalanceCost, 0.0)
			require.Len(t, usageRepo.created, 1, "cyber fallback must not add another usage row")
			usage := <-usageRepo.created
			require.Equal(t, service.RequestTypeCyberBlocked, usage.RequestType)
			require.InDelta(t, cmd.BalanceCost, usage.ActualCost, 1e-12)
		})
	}
}
