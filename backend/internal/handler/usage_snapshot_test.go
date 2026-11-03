package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type usageSnapshotParent struct {
	context.Context
	sealed bool
}

func (p *usageSnapshotParent) Value(key any) any {
	if p.sealed {
		panic("request context accessed after enqueue")
	}
	return p.Context.Value(key)
}

func TestUsageTaskSnapshotsContextBeforeEnqueue(t *testing.T) {
	parent := &usageSnapshotParent{Context: context.WithValue(context.Background(), ctxkey.RequestID, "original-request")}
	parent.Context = context.WithValue(parent.Context, ctxkey.ClientRequestID, "original-client")
	task, abandon := wrapUsageRecordTaskContext(parent, func(ctx context.Context) {
		require.Equal(t, "original-request", ctx.Value(ctxkey.RequestID))
		require.Equal(t, "original-client", ctx.Value(ctxkey.ClientRequestID))
		require.ErrorIs(t, ctx.Err(), context.Canceled, "worker cancellation must still apply")
	})
	defer abandon()
	parent.sealed = true
	worker, cancel := context.WithCancel(context.Background())
	cancel()
	require.NotPanics(t, func() { task(worker) })
}

func blockUsageSnapshotPool(t *testing.T) (*service.UsageRecordWorkerPool, func()) {
	t.Helper()
	pool := newUsageRecordTestPool(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	pool.Submit(func(context.Context) { close(entered); <-release })
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("worker not blocked")
	}
	return pool, unblock
}

func TestQueuedUsageDoesNotReadReusedGinContext(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, endpoint := range []string{"/v1/responses", "/v1/messages", "/v1/chat/completions"} {
		t.Run(endpoint, func(t *testing.T) {
			accounts := []service.Account{{ID: 9930, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
				Status: service.StatusActive, Schedulable: true, Credentials: map[string]any{"api_key": "fixture"},
				Extra: map[string]any{"openai_responses_supported": true}}}
			cfg := &config.Config{RunMode: config.RunModeSimple}
			cfg.Default.RateMultiplier = 1
			accountRepo := &grokPartialUsageAccountRepo{grokStreamFailoverAccountRepo: grokStreamFailoverAccountRepo{openAIWSFailoverHandlerAccountRepoStub: openAIWSFailoverHandlerAccountRepoStub{accounts: accounts}}}
			usageRepo := &openAIWSUsageHandlerUsageLogRepoStub{created: make(chan *service.UsageLog, 3)}
			upstream := &grokPartialUsageUpstream{}
			cache := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
			t.Cleanup(cache.Stop)
			gateway := service.NewOpenAIGatewayService(accountRepo, usageRepo, nil, nil, nil, nil, nil, cfg, nil, nil,
				service.NewBillingService(cfg, nil), nil, cache, upstream, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil)
			h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(nil), cache,
				service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
			pool, unblock := blockUsageSnapshotPool(t)
			h.usageRecordWorkerPool = pool
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(`{"model":"gpt-5.1","input":"test","messages":[{"role":"user","content":"test"}],"max_tokens":32,"stream":true}`))
			c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), ctxkey.RequestedPublicModel, "original-public-model"))
			groupID := int64(4230)
			c.Set(string(middleware.ContextKeyAPIKey), &service.APIKey{ID: 1830, GroupID: &groupID,
				User:  &service.User{ID: 1730, Status: service.StatusActive},
				Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI, Status: service.StatusActive, AllowMessagesDispatch: true}})
			c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 1730})
			switch endpoint {
			case "/v1/responses":
				h.Responses(c)
			case "/v1/messages":
				h.Messages(c)
			default:
				h.ChatCompletions(c)
			}
			require.Equal(t, []int64{9930}, upstream.accountIDs, "one synthetic upstream dispatch")
			c.Request = httptest.NewRequest(http.MethodPost, "/next-client", nil).WithContext(
				context.WithValue(context.Background(), ctxkey.RequestedPublicModel, "another-clients-model"))
			unblock()
			pool.Stop()
			require.Len(t, usageRepo.created, 1)
			usage := <-usageRepo.created
			require.Equal(t, "original-public-model", usage.RequestedModel)
			require.Equal(t, int64(1830), usage.APIKeyID)
			require.Equal(t, int64(1730), usage.UserID)
			require.Equal(t, 60, usage.InputTokens)
			require.Equal(t, 40, usage.CacheReadTokens)
			require.Equal(t, 7, usage.OutputTokens)
		})
	}
}
