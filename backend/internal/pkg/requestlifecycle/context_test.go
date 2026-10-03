package requestlifecycle

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWithoutClientCancelRetainsOnlyForceCancellation(t *testing.T) {
	type valueKey struct{}
	client, cancelClient := context.WithTimeout(context.WithValue(context.Background(), valueKey{}, "request-value"), time.Hour)
	defer cancelClient()
	force, cancelForce := context.WithCancel(context.Background())
	defer cancelForce()
	upstream := WithoutClientCancel(WithForceCancellation(client, force))
	upstream = WithoutClientCancel(upstream)
	require.Equal(t, "request-value", upstream.Value(valueKey{}))
	_, hasDeadline := upstream.Deadline()
	require.False(t, hasDeadline)
	cancelClient()
	require.NoError(t, upstream.Err())
	require.NoError(t, context.Cause(upstream))
	child, cancelChild := context.WithCancel(upstream)
	defer cancelChild()
	cancelForce()
	select {
	case <-child.Done():
	case <-time.After(time.Second):
		t.Fatal("forced shutdown did not cancel the upstream child")
	}
	require.ErrorIs(t, upstream.Err(), context.Canceled)
	require.ErrorIs(t, context.Cause(upstream), context.Canceled)
}

func TestWithoutClientCancelWithoutLifecycleKeepsLegacyBehavior(t *testing.T) {
	client, cancel := context.WithCancel(context.Background())
	cancel()
	upstream := WithoutClientCancel(client)
	require.Nil(t, upstream.Done())
	require.NoError(t, upstream.Err())
}

func TestForceCancellationDoesNotCrossServerLifecycles(t *testing.T) {
	forceA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	forceB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	upstreamA := WithoutClientCancel(WithForceCancellation(context.Background(), forceA))
	upstreamB := WithoutClientCancel(WithForceCancellation(context.Background(), forceB))
	cancelA()
	require.ErrorIs(t, upstreamA.Err(), context.Canceled)
	require.NoError(t, upstreamB.Err())
	cancelB()
	require.ErrorIs(t, upstreamB.Err(), context.Canceled)
}
