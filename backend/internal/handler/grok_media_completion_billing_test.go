//go:build unit

package handler

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type mediaCompletionBillingRepo struct {
	service.UsageBillingRepository
	commands []*service.UsageBillingCommand
	apply    func(context.Context) error
}

func (r *mediaCompletionBillingRepo) Apply(ctx context.Context, cmd *service.UsageBillingCommand) (*service.UsageBillingApplyResult, error) {
	r.commands = append(r.commands, cmd)
	if r.apply != nil {
		return nil, r.apply(ctx)
	}
	return &service.UsageBillingApplyResult{}, nil
}

func mediaCompletionBillingFixture(t *testing.T) (*OpenAIGatewayHandler, *grokMediaSlotBindings, *grokMediaSlotUpstream, *mediaCompletionBillingRepo) {
	h, slots, bindings, upstream := newGrokMediaSlotHandler(t, false, false)
	bindings.checkContext = true
	account := service.Account{ID: 1, Platform: service.PlatformGrok, Type: service.AccountTypeAPIKey,
		Status: service.StatusActive, Schedulable: true, Concurrency: 50, GroupIDs: []int64{24}, Credentials: map[string]any{"api_key": "fixture"}}
	repo := grokMediaSlotRepo{openAIImagesFailoverAccountRepo: openAIImagesFailoverAccountRepo{accounts: []service.Account{account}}}
	cfg := &config.Config{}
	billing := &mediaCompletionBillingRepo{}
	h.gatewayService = service.NewOpenAIGatewayService(repo, nil, billing, nil, nil, nil, bindings, cfg,
		nil, service.NewConcurrencyService(slots), service.NewBillingService(cfg, nil), nil, nil, upstream,
		service.NewDeferredService(repo, nil, time.Minute), nil, nil, nil, nil, nil, nil, nil)
	return h, bindings, upstream, billing
}

func TestGrokVideoCompletedDownloadBillsDespiteDeliveryFailure(t *testing.T) {
	for _, cancelClient := range []bool{false, true} {
		t.Run(map[bool]string{false: "connected", true: "cancelled"}[cancelClient], func(t *testing.T) {
			h, bindings, upstream, billing := mediaCompletionBillingFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			upstream.call = func(req *http.Request, _ int64) (*http.Response, error) {
				calls++
				if !strings.HasSuffix(req.URL.Path, "/content") && req.URL.Host != "vidgen.x.ai" {
					return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(
						`{"status":"done","model":"grok-imagine-video","video":{"url":"https://vidgen.x.ai/test.mp4","duration":6}}`))}, nil
				}
				if cancelClient {
					cancel()
				}
				return nil, errors.New("fixture delivery failure")
			}
			c, _ := grokMediaSlotContext(ctx, false)
			key, _ := middleware2.GetAPIKeyFromContext(c)
			key.Group.RateMultiplier = 1
			h.GrokVideoContent(c)
			require.Len(t, billing.commands, 1)
			require.Equal(t, "grok-video:task", billing.commands[0].RequestID)
			require.Positive(t, billing.commands[0].BalanceCost)
			require.Equal(t, 2, calls, "download failure must not regenerate or retry another account")
			require.Len(t, bindings.billed, 1)
			c, _ = grokMediaSlotContext(context.Background(), false)
			h.GrokVideoStatus(c)
			require.Len(t, billing.commands, 1, "later done poll must not bill again")
		})
	}
}

func TestGrokVideoAcceptedCreationPersistsAfterClientCancellation(t *testing.T) {
	h, _, bindings, upstream := newGrokMediaSlotHandler(t, false, false)
	bindings.checkContext = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	upstream.call = func(*http.Request, int64) (*http.Response, error) {
		cancel()
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"request_id":"accepted"}`))}, nil
	}
	c, _ := grokMediaSlotContext(ctx, true)
	h.GrokVideoGeneration(c)
	require.Equal(t, 1, bindings.writes)
	require.Contains(t, bindings.pending, "10:20:accepted")
}

func TestGrokVideoFailedBillingReleasesClaimAfterTaskCancellation(t *testing.T) {
	h, bindings, _, billing := mediaCompletionBillingFixture(t)
	c, _ := grokMediaSlotContext(context.Background(), false)
	key, _ := middleware2.GetAPIKeyFromContext(c)
	key.Group.RateMultiplier = 1
	subject := middleware2.AuthSubject{UserID: 10}
	result := &service.OpenAIForwardResult{ResponseID: "task", Model: "grok-imagine-video", VideoCount: 1, VideoDurationSeconds: 6}
	result = prepareGrokVideoCompletionBilling(c.Request.Context(), h, zap.NewNop(), key, subject, "task", result)
	require.NotNil(t, result)
	// The billing repository has its own detached context. Simulate its failure
	// after the worker's shorter timeout has expired, without waiting for real timeouts.
	h.usageRecordWorkerPool = service.NewUsageRecordWorkerPoolWithOptions(service.UsageRecordWorkerPoolOptions{WorkerCount: 1, QueueSize: 1, TaskTimeout: time.Nanosecond})
	t.Cleanup(h.usageRecordWorkerPool.Stop)
	billing.apply = func(context.Context) error { return context.DeadlineExceeded }
	recordGrokMediaUsage(c, h, zap.NewNop(), key, subject, nil,
		&service.Account{ID: 1, Platform: service.PlatformGrok, Type: service.AccountTypeAPIKey}, result, result.Model, nil, "task")
	h.usageRecordWorkerPool.Stop()
	require.Equal(t, 1, bindings.releases)
	require.Empty(t, bindings.billed)
	next := prepareGrokVideoCompletionBilling(context.Background(), h, zap.NewNop(), key, subject, "task", result)
	require.NotNil(t, next, "billing failure must permit a later durable retry")
}

func TestGrokMediaBillingContextPreservesValuesWithinBound(t *testing.T) {
	type key struct{}
	parent, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "trace"))
	cancel()
	ctx, release := grokMediaBillingContext(parent)
	defer release()
	require.NoError(t, ctx.Err())
	require.Equal(t, "trace", ctx.Value(key{}))
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.Positive(t, time.Until(deadline))
	require.LessOrEqual(t, time.Until(deadline), 5*time.Second)
}
