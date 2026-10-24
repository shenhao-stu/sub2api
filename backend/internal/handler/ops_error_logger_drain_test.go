package handler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

type opsDrainBarrierRepo struct {
	service.OpsRepository
	entered chan struct{}
	release chan struct{}
	mu      sync.Mutex
	seen    map[string]int
}

func (r *opsDrainBarrierRepo) BatchInsertErrorLogs(ctx context.Context, entries []*service.OpsInsertErrorLogInput) (int64, error) {
	r.entered <- struct{}{}
	select {
	case <-r.release:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, entry := range entries {
		r.seen[entry.RequestID]++
	}
	return int64(len(entries)), nil
}

func (r *opsDrainBarrierRepo) InsertErrorLog(ctx context.Context, entry *service.OpsInsertErrorLogInput) (int64, error) {
	return r.BatchInsertErrorLogs(ctx, []*service.OpsInsertErrorLogInput{entry})
}

// Stop while every real worker is inside persistence and queued work remains.
// A worker must drain its closed startup queue, even after the global admission
// pointer is cleared. Re-reading the global nil channel deadlocks this case.
func TestOpsErrorLogWorkersDrainStartupQueueDuringStop(t *testing.T) {
	resetOpsErrorLoggerStateForTest(t)
	workers, _ := opsErrorLogConfig()
	total := workers*opsErrorLogBatchSize + 4
	repo := &opsDrainBarrierRepo{entered: make(chan struct{}, total), release: make(chan struct{}), seen: make(map[string]int)}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(repo.release) }) }
	t.Cleanup(func() {
		release()
		StopOpsErrorLogWorkers()
		resetOpsErrorLoggerStateForTest(t)
	})
	ops := service.NewOpsService(repo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	enqueue := func(i int) {
		enqueueOpsErrorLog(ops, &service.OpsInsertErrorLogInput{RequestID: fmt.Sprintf("synthetic-drain-%d", i), StatusCode: 502, ErrorMessage: "synthetic"})
	}
	waitForWorker := func() {
		t.Helper()
		select {
		case <-repo.entered:
		case <-time.After(3 * time.Second):
			t.Fatal("worker did not reach persistence barrier")
		}
	}
	enqueue(0)
	waitForWorker()
	for i := 1; i < total; i++ {
		enqueue(i)
	}
	for i := 1; i < workers; i++ {
		waitForWorker()
	}
	require.Equal(t, int64(total), opsErrorLogEnqueued.Load())
	require.Positive(t, OpsErrorLogQueueLength(), "fixture must retain a queued tail")
	require.Positive(t, opsErrorLogQueueBytes.Load())
	stopped := make(chan bool, 1)
	go func() { stopped <- StopOpsErrorLogWorkers() }()
	require.Eventually(t, func() bool {
		opsErrorLogMu.RLock()
		defer opsErrorLogMu.RUnlock()
		return opsErrorLogStopping && opsErrorLogQueue == nil
	}, time.Second, time.Millisecond)
	enqueue(total)
	require.Equal(t, int64(total), opsErrorLogEnqueued.Load(), "stopping must reject new work")
	release()
	select {
	case drained := <-stopped:
		require.True(t, drained)
	case <-time.After(3 * time.Second):
		t.Fatal("workers did not drain after persistence resumed")
	}
	require.True(t, StopOpsErrorLogWorkers(), "repeated stop must preserve the drained result")
	require.Zero(t, OpsErrorLogQueueLength())
	require.Zero(t, opsErrorLogQueueBytes.Load())
	require.Zero(t, opsErrorLogDropped.Load())
	require.Equal(t, int64(total), opsErrorLogProcessed.Load())
	repo.mu.Lock()
	defer repo.mu.Unlock()
	require.Len(t, repo.seen, total)
	for _, count := range repo.seen {
		require.Equal(t, 1, count, "each accepted event must persist once")
	}
}
