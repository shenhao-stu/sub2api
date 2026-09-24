package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCleanupWithinWaitsForCompletion(t *testing.T) {
	finished := false
	require.True(t, cleanupWithin(func() { finished = true }, time.Second))
	require.True(t, finished)
}

func TestCleanupWithinBoundsBlockedCleanup(t *testing.T) {
	release, finished := make(chan struct{}), make(chan struct{})
	require.False(t, cleanupWithin(func() { <-release; close(finished) }, 10*time.Millisecond))
	close(release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("cleanup worker did not exit")
	}
}
