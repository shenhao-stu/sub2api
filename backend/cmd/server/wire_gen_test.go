package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestProvideServiceBuildInfo(t *testing.T) {
	in := handler.BuildInfo{
		Version:   "v-test",
		BuildType: "release",
	}
	out := provideServiceBuildInfo(in)
	require.Equal(t, in.Version, out.Version)
	require.Equal(t, in.BuildType, out.BuildType)
}

func newCleanupTest(t *testing.T, billingCacheSvc *service.BillingCacheService, usagePool *service.UsageRecordWorkerPool) func() {
	t.Helper()
	cfg := &config.Config{}

	oauthSvc := service.NewOAuthService(nil, nil)
	openAIOAuthSvc := service.NewOpenAIOAuthService(nil, nil)
	geminiOAuthSvc := service.NewGeminiOAuthService(nil, nil, nil, nil, cfg)
	antigravityOAuthSvc := service.NewAntigravityOAuthService(nil)

	tokenRefreshSvc := service.NewTokenRefreshService(
		nil,
		oauthSvc,
		openAIOAuthSvc,
		geminiOAuthSvc,
		antigravityOAuthSvc,
		nil,
		nil,
		cfg,
		nil,
	)
	accountExpirySvc := service.NewAccountExpiryService(nil, time.Second)
	codexVersionSyncSvc := service.NewOpenAICodexVersionSyncService(nil, nil, nil, time.Second)
	claudeCodeVersionSyncSvc := service.NewClaudeCodeVersionSyncService(nil, nil, nil, time.Second)
	proxyExpirySvc := service.NewProxyExpiryService(nil, time.Second)
	subscriptionExpirySvc := service.NewSubscriptionExpiryService(nil, time.Second)
	pricingSvc := service.NewPricingService(cfg, nil)
	emailQueueSvc := service.NewEmailQueueService(nil, 1)
	if billingCacheSvc == nil {
		billingCacheSvc = service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	}
	idempotencyCleanupSvc := service.NewIdempotencyCleanupService(nil, cfg)
	schedulerSnapshotSvc := service.NewSchedulerSnapshotService(nil, nil, nil, nil, cfg)
	opsSystemLogSinkSvc := service.NewOpsSystemLogSink(nil)

	cleanup := provideCleanup(
		nil, // entClient
		nil, // redis
		&service.OpsMetricsCollector{},
		&service.OpsAggregationService{},
		&service.OpsAlertEvaluatorService{},
		&service.OpsCleanupService{},
		&service.OpsScheduledReportService{},
		opsSystemLogSinkSvc,
		nil, // opsService
		nil, // opsIngressRejectAggregator
		nil, // apiKeyService
		nil, // authCacheInvalidationWorker
		schedulerSnapshotSvc,
		tokenRefreshSvc,
		accountExpirySvc,
		nil, // cnProviderBalanceCheck
		codexVersionSyncSvc,
		claudeCodeVersionSyncSvc,
		proxyExpirySvc,
		subscriptionExpirySvc,
		&service.UsageCleanupService{},
		idempotencyCleanupSvc,
		&service.BatchImageCleanupService{},
		nil, // batchImageWorker
		pricingSvc,
		emailQueueSvc,
		billingCacheSvc,
		usagePool,
		&service.SubscriptionService{},
		oauthSvc,
		openAIOAuthSvc,
		geminiOAuthSvc,
		antigravityOAuthSvc,
		nil, // grokOAuth
		nil, // openAIGateway
		nil, // scheduledTestRunner
		nil, // backupSvc
		nil, // paymentOrderExpiry
		nil, // channelMonitorRunner
		nil, // channelMonitorV2Aggregator
		nil, // quotaFlusher
		nil, // upstreamBillingProbe
		nil, // ollamaCloudUsage
		nil, // opencodeGoUsage
		nil, // auditLog
		nil, // openAIAutoReset
		nil, // promptAudit
		nil, // pluginManager
	)

	return cleanup
}

func TestProvideCleanup_WithMinimalDependencies_NoPanic(t *testing.T) {
	require.NotPanics(t, newCleanupTest(t, nil, &service.UsageRecordWorkerPool{}))
}

type cleanupBillingCache struct {
	service.BillingCache
	recorded chan float64
}

func (c *cleanupBillingCache) UpdateAPIKeyRateLimitUsage(_ context.Context, _ int64, cost float64) error {
	c.recorded <- cost
	return nil
}

func TestProvideCleanupDrainsUsageBeforeClosingBillingCache(t *testing.T) {
	cache := &cleanupBillingCache{recorded: make(chan float64, 1)}
	billingCache := service.NewBillingCacheService(cache, nil, nil, nil, nil, nil, &config.Config{}, nil)
	pool := service.NewUsageRecordWorkerPoolWithOptions(service.UsageRecordWorkerPoolOptions{
		WorkerCount: 1, QueueSize: 2, TaskTimeout: 5 * time.Second,
	})
	started, release := make(chan struct{}), make(chan struct{})
	defer pool.Stop()
	defer billingCache.Stop()
	var releaseOnce sync.Once
	finish := func() { releaseOnce.Do(func() { close(release) }) }
	defer finish()
	require.Equal(t, service.UsageRecordSubmitModeEnqueued, pool.Submit(func(context.Context) {
		close(started)
		<-release
		billingCache.QueueUpdateAPIKeyRateLimitUsage(7, 0.25)
	}))
	<-started
	cleanup := newCleanupTest(t, billingCache, pool)
	done := make(chan struct{})
	go func() { cleanup(); close(done) }()
	select {
	case <-done:
		t.Fatal("cleanup returned while usage was still settling")
	case <-time.After(50 * time.Millisecond):
	}
	finish()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup did not drain usage and billing cache")
	}
	select {
	case cost := <-cache.recorded:
		require.Equal(t, 0.25, cost)
	default:
		t.Fatal("last usage task lost its billing cache write during shutdown")
	}
}
