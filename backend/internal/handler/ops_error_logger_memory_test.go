package handler

import (
	"runtime"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

// Hold 500 real queue entries without a database or upstream. Each synthetic
// plaintext error is larger than the queue's body limit; its original backing
// allocation must become collectible after enqueue returns.
func TestOpsErrorQueue500BodiesRespectRetainedByteBudget(t *testing.T) {
	const count, inputBytes = 500, 256 << 10
	setupOpsErrorLogTestQueue(t, count)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	ops := &service.OpsService{}
	for i := 0; i < count; i++ {
		enqueueOpsErrorLog(ops, &service.OpsInsertErrorLogInput{
			StatusCode: 502,
			ErrorBody:  strings.Repeat("synthetic upstream unavailable; ", inputBytes/31),
		})
	}
	if len(opsErrorLogQueue) != count {
		t.Fatalf("queued %d entries, want %d", len(opsErrorLogQueue), count)
	}
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)
	retained := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	accounted := opsErrorLogQueueBytes.Load()
	t.Logf("entries=%d retained_heap=%d queue_accounted_bytes=%d source_bytes_each~=%d", count, retained, accounted, inputBytes)
	// Allow allocator/object overhead, but not the original 125 MiB bodies.
	if retained > accounted+8<<20 {
		t.Fatalf("queue retains %d bytes beyond its counted %d-byte payload", retained, accounted)
	}
	runtime.KeepAlive(opsErrorLogQueue)
}
