package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	"github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type cleanupOpsRepo struct {
	service.OpsRepository
	entered chan struct{}
	release chan struct{}
	mu      sync.Mutex
	seen    map[string]int
}

func (r *cleanupOpsRepo) BatchInsertErrorLogs(_ context.Context, entries []*service.OpsInsertErrorLogInput) (int64, error) {
	select {
	case r.entered <- struct{}{}:
	default:
	}
	// A blocked persistence dependency intentionally ignores context in the timeout
	// case, so the public Stop budget, rather than a stub deadline, is exercised.
	<-r.release
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, entry := range entries {
		r.seen[entry.RequestID]++
	}
	return int64(len(entries)), nil
}

func (r *cleanupOpsRepo) InsertErrorLog(ctx context.Context, entry *service.OpsInsertErrorLogInput) (int64, error) {
	return r.BatchInsertErrorLogs(ctx, []*service.OpsInsertErrorLogInput{entry})
}

type cleanupCloseDriver struct {
	dialect.Driver
	closed chan struct{}
	once   sync.Once
}

func (d *cleanupCloseDriver) Close() error {
	d.once.Do(func() { close(d.closed) })
	return nil
}

// Shutdown is process-global and intentionally irreversible. Exercise its
// public surface in a fresh test process instead of exporting a reset hook or
// allowing one cleanup test to leave another test's Ops workers stopped.
func TestProvideCleanupDrainsOpsAfterBillingBeforeInfra(t *testing.T) {
	const childEnv = "SUB2API_OPS_CLEANUP_TEST_CHILD"
	if mode := os.Getenv(childEnv); mode != "" {
		testProvideCleanupOpsLifecycle(t, mode == "timeout")
		return
	}
	for _, mode := range []string{"drain", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			exe, err := os.Executable()
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, exe, "-test.run=^TestProvideCleanupDrainsOpsAfterBillingBeforeInfra$", "-test.v")
			// Bound the worker count so the fixture always retains queued work.
			cmd.Env = append(os.Environ(), childEnv+"="+mode, "GOMAXPROCS=2")
			output, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", output)
		})
	}
}

func testProvideCleanupOpsLifecycle(t *testing.T, timeout bool) {
	gin.SetMode(gin.TestMode)
	var logs bytes.Buffer
	oldOutput := log.Writer()
	defer log.SetOutput(oldOutput)
	repo := &cleanupOpsRepo{entered: make(chan struct{}, 32), release: make(chan struct{}), seen: make(map[string]int)}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(repo.release) }) }
	defer release()
	ops := service.NewOpsService(repo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	router := gin.New()
	router.Use(handler.OpsErrorLoggerMiddleware(ops))
	router.POST("/v1/chat/completions", func(c *gin.Context) {
		c.Header("X-Request-Id", c.GetHeader("X-Synthetic-Request-Id"))
		c.JSON(http.StatusBadGateway, gin.H{"error": gin.H{"type": "upstream_error", "message": "synthetic cleanup error"}})
	})
	enqueue := func(i int) {
		request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		request.Header.Set("X-Synthetic-Request-Id", fmt.Sprintf("cleanup-%d", i))
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, request)
		require.Equal(t, http.StatusBadGateway, recorder.Code)
	}
	const total = 256
	enqueue(0)
	select {
	case <-repo.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("Ops worker did not enter persistence")
	}
	for i := 1; i < total-1; i++ {
		enqueue(i)
	}
	require.Equal(t, int64(total-1), handler.OpsErrorLogEnqueuedTotal())
	require.Positive(t, handler.OpsErrorLogQueueLength())

	cache := &cleanupBillingCache{recorded: make(chan float64, 1)}
	billingCache := service.NewBillingCacheService(cache, nil, nil, nil, nil, nil, &config.Config{}, nil)
	pool := service.NewUsageRecordWorkerPoolWithOptions(service.UsageRecordWorkerPoolOptions{WorkerCount: 1, QueueSize: 2, TaskTimeout: 5 * time.Second})
	defer pool.Stop()
	defer billingCache.Stop()
	usageStarted, usageRelease := make(chan struct{}), make(chan struct{})
	var usageOnce sync.Once
	releaseUsage := func() { usageOnce.Do(func() { close(usageRelease) }) }
	defer releaseUsage()
	require.Equal(t, service.UsageRecordSubmitModeEnqueued, pool.Submit(func(context.Context) {
		close(usageStarted)
		<-usageRelease
		billingCache.QueueUpdateAPIKeyRateLimitUsage(7, 0.25)
	}))
	<-usageStarted
	driver := &cleanupCloseDriver{closed: make(chan struct{})}
	cleanup := newCleanupTest(t, billingCache, pool, ent.NewClient(ent.Driver(driver)))
	log.SetOutput(&logs)
	done := make(chan struct{})
	go func() { cleanup(); close(done) }()
	select {
	case <-done:
		t.Fatal("cleanup returned before usage settled")
	case <-time.After(50 * time.Millisecond):
	}
	require.Positive(t, handler.OpsErrorLogQueueCapacity(), "Ops shutdown must wait for billing")
	enqueue(total - 1)
	require.Equal(t, int64(total), handler.OpsErrorLogEnqueuedTotal(), "Ops must remain open while billing is settling")
	releaseUsage()
	select {
	case cost := <-cache.recorded:
		require.Equal(t, 0.25, cost)
	case <-time.After(2 * time.Second):
		t.Fatal("billing cache did not settle")
	}
	require.Eventually(t, func() bool { return handler.OpsErrorLogQueueCapacity() == 0 }, 2*time.Second, time.Millisecond)
	select {
	case <-done:
		t.Fatal("cleanup returned while Ops persistence was blocked")
	default:
	}
	select {
	case <-driver.closed:
		t.Fatal("Ent closed while Ops was draining")
	default:
	}
	enqueue(total)
	require.Equal(t, int64(total), handler.OpsErrorLogEnqueuedTotal(), "stop must reject new work")
	if !timeout {
		release()
	}
	select {
	case <-done:
	case <-time.After(12 * time.Second):
		t.Fatal("Ops stop exceeded its bounded cleanup window")
	}
	select {
	case <-driver.closed:
	default:
		t.Fatal("infra must close after bounded Ops stop")
	}
	if timeout {
		require.Contains(t, logs.String(), "[Cleanup] OpsErrorLogWorkers failed:")
		require.NotContains(t, logs.String(), "[Cleanup] OpsErrorLogWorkers succeeded")
		require.NotContains(t, logs.String(), "[Cleanup] All cleanup steps completed")
		require.False(t, handler.StopOpsErrorLogWorkers(), "repeat stop must preserve failed drain result")
		release()
		require.Eventually(t, func() bool { return handler.OpsErrorLogProcessedTotal() == total }, 2*time.Second, time.Millisecond)
	} else {
		require.Contains(t, logs.String(), "[Cleanup] OpsErrorLogWorkers succeeded")
		require.True(t, handler.StopOpsErrorLogWorkers())
	}
	enqueue(total + 1)
	require.Equal(t, int64(total), handler.OpsErrorLogEnqueuedTotal(), "completed Stop must not reopen the queue")
	require.Zero(t, handler.OpsErrorLogQueueCapacity())
	require.Zero(t, handler.OpsErrorLogQueueLength())
	require.Zero(t, handler.OpsErrorLogQueueBytes())
	require.Zero(t, handler.OpsErrorLogDroppedTotal())
	require.Equal(t, int64(total), handler.OpsErrorLogProcessedTotal())
	repo.mu.Lock()
	defer repo.mu.Unlock()
	require.Len(t, repo.seen, total)
	for _, count := range repo.seen {
		require.Equal(t, 1, count)
	}
}
