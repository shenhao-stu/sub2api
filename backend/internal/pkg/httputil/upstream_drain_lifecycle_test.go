package httputil

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/requestlifecycle"
	"github.com/stretchr/testify/require"
)

func TestUpstreamDrainPreservesForceShutdownAndRequestIsolation(t *testing.T) {
	forceA, cancelForceA := context.WithCancel(context.Background())
	defer cancelForceA()
	forceB, cancelForceB := context.WithCancel(context.Background())
	defer cancelForceB()
	client, cancelClient := context.WithCancel(context.Background())
	defer cancelClient()
	a, releaseA := NewUpstreamAttemptContext(DetachUpstreamContext(DetachUpstreamContext(requestlifecycle.WithForceCancellation(client, forceA))), time.Minute)
	defer releaseA()
	b, releaseB := NewUpstreamAttemptContext(DetachUpstreamContext(requestlifecycle.WithForceCancellation(context.Background(), forceB)), time.Minute)
	defer releaseB()
	cancelClient()
	require.NoError(t, a.Err(), "ordinary disconnect must preserve late usage reads")
	cancelForceA()
	select {
	case <-a.Done():
	case <-time.After(time.Second):
		t.Fatal("forced shutdown was lost by detached billing context")
	}
	require.NoError(t, b.Err(), "forced shutdown must not cancel a different server lifetime")
}
