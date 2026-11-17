package service

import (
	"context"
	"errors"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/sync/semaphore"
)

// Match the native relay's Nginx body class. The edge still enforces the
// global limit when two Sub2API workers overlap during a rolling release.
const nativeLargeBodyMin = 1_000_000
const nativeLargeBodyMax = 16 << 20
const nativeLargeWait = 12 * time.Second

var errNativeIngressBusy = errors.New("native relay large request queue full")

type nativeIngressLane struct {
	slots   *semaphore.Weighted
	waiters chan struct{}
}

func newNativeIngressLane() *nativeIngressLane {
	return &nativeIngressLane{slots: semaphore.NewWeighted(2), waiters: make(chan struct{}, 2)}
}

func (lane *nativeIngressLane) acquire(ctx context.Context, timeout time.Duration) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	release := func() { lane.slots.Release(1) }
	if lane.slots.TryAcquire(1) {
		return release, nil
	}
	select {
	case lane.waiters <- struct{}{}:
		defer func() { <-lane.waiters }()
	default:
		return nil, errNativeIngressBusy
	}
	wait, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := lane.slots.Acquire(wait, 1); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errNativeIngressBusy
	}
	return release, nil
}

// Only explicitly configured passthrough accounts enter the queue. Each lane
// retains two semaphores, never the requests, credentials or session histories.
func (s *GatewayService) acquireNativeIngress(c *gin.Context, account *Account, size int) (func(), error) {
	if size < nativeLargeBodyMin || size > nativeLargeBodyMax || !account.getExtraBool("native_relay_large_body_queue") {
		return nil, nil
	}
	lane, ok := s.nativeIngressLanes.Load(account.ID)
	if !ok {
		lane, _ = s.nativeIngressLanes.LoadOrStore(account.ID, newNativeIngressLane())
	}
	ctx := c.Request.Context()
	release, err := lane.(*nativeIngressLane).acquire(ctx, nativeLargeWait)
	if err == nil && ctx.Err() != nil {
		release()
		return nil, ctx.Err()
	}
	if errors.Is(err, errNativeIngressBusy) {
		if !c.Writer.Written() {
			c.Header("Retry-After", "1")
		}
		MarkResponseCommitted(c)
		WriteAnthropicErrorResponse(c, 429, gin.H{"type": "error", "error": gin.H{
			"type": "overloaded_error", "code": "native_ingress_busy", "message": "Relay capacity is busy; retry later.",
		}})
	}
	return release, err
}
