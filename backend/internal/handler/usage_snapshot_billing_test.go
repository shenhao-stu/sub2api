//go:build unit

package handler

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestQueuedSystemOneUsageSnapshotsResultWithoutResponseBody(t *testing.T) {
	c, _ := newSystemOneHandlerContext(validSystemOneHandlerBody)
	c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), ctxkey.RequestedPublicModel, "original-public-model"))
	usage := &openAIWSUsageHandlerUsageLogRepoStub{created: make(chan *service.UsageLog, 1)}
	cfg := &config.Config{RunMode: config.RunModeSimple}
	billing := service.NewBillingService(cfg, nil)
	pool, unblock := blockUsageSnapshotPool(t)
	h := &GatewayHandler{usageRecordWorkerPool: pool, gatewayService: service.NewGatewayService(
		nil, nil, usage, nil, nil, nil, nil, nil, cfg, nil, nil, billing, nil, nil,
		nil, nil, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil,
		service.NewModelPricingResolver(nil, billing), nil, nil, nil)}
	result := &service.SystemOneForwardResult{ForwardResult: service.ForwardResult{
		RequestID: "original-response", Model: "claude-sonnet-4-6", Usage: service.ClaudeUsage{InputTokens: 12, OutputTokens: 3}},
		Body: make([]byte, 4<<20)}
	key := &service.APIKey{ID: 2, User: &service.User{ID: 1}}
	account := &service.Account{ID: 3, Platform: service.PlatformTypeSafe, Type: service.AccountTypeAPIKey}
	h.recordSystemOneUsage(c, key, account, nil, service.ChannelMappingResult{}, result.Model, []byte(validSystemOneHandlerBody), result, 1, time.Time{})
	// The HTTP response owner is free to release/reuse its result after enqueue.
	result.ForwardResult = service.ForwardResult{Model: "unrelated", Usage: service.ClaudeUsage{InputTokens: 999}}
	result.Body = nil
	c.Request = httptest.NewRequest("POST", "/another-client", nil)
	unblock()
	pool.Stop()
	require.Len(t, usage.created, 1)
	got := <-usage.created
	require.Equal(t, "original-public-model", got.RequestedModel)
	require.Equal(t, "original-response", got.RequestID)
	require.Equal(t, 12, got.InputTokens)
	require.Equal(t, 3, got.OutputTokens)
}

func TestQueuedGrokMediaUsageSnapshotsPayloadBeforeEnqueue(t *testing.T) {
	h, _, _, billing := mediaCompletionBillingFixture(t)
	pool, unblock := blockUsageSnapshotPool(t)
	h.usageRecordWorkerPool = pool
	c, _ := grokMediaSlotContext(context.Background(), false)
	key, _ := middleware.GetAPIKeyFromContext(c)
	key.Group.RateMultiplier = 1
	body := []byte(`{"model":"grok-imagine-image","prompt":"original"}`)
	expectedHash := service.HashUsageRequestPayload(body)
	recordGrokMediaUsage(c, h, zap.NewNop(), key, middleware.AuthSubject{UserID: 10}, nil,
		&service.Account{ID: 1, Platform: service.PlatformGrok, Type: service.AccountTypeAPIKey},
		&service.OpenAIForwardResult{RequestID: "image-fixture", Model: "grok-imagine-image", ImageCount: 1},
		"grok-imagine-image", body, "image-fixture")
	clear(body)
	unblock()
	pool.Stop()
	require.Len(t, billing.commands, 1)
	require.Equal(t, expectedHash, billing.commands[0].RequestPayloadHash)
}
