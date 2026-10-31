//go:build unit

package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type accountWaitCacheStub struct {
	stubConcurrencyCacheForTest
	ticket             string
	polls              []string
	enqueueErr         error
	acquireErr         error
	cancelCalls        int
	cancelContextError error
	acquired           bool
}

func (c *accountWaitCacheStub) EnqueueAccountSlotWait(_ context.Context, _ int64, ticket string, _ time.Duration) error {
	c.ticket = ticket
	return c.enqueueErr
}
func (c *accountWaitCacheStub) AcquireWaitingAccountSlot(_ context.Context, _ int64, _ int, ticket string) (bool, error) {
	c.polls = append(c.polls, ticket)
	return c.acquired, c.acquireErr
}
func (c *accountWaitCacheStub) CancelAccountSlotWait(ctx context.Context, _ int64, ticket string) error {
	c.cancelCalls++
	c.cancelContextError = ctx.Err()
	if ticket != c.ticket {
		return errors.New("wrong ticket")
	}
	return nil
}

func TestAccountWaitServiceStableTicketAndOwnership(t *testing.T) {
	cache := &accountWaitCacheStub{}
	s := NewConcurrencyService(cache)
	acquire, cancel, err := s.NewAccountSlotWait(context.Background(), 10, 1, time.Minute)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		result, err := acquire()
		require.NoError(t, err)
		require.False(t, result.Acquired)
	}
	cache.acquired = true
	result, err := acquire()
	require.NoError(t, err)
	require.True(t, result.Acquired)
	cancel()
	require.Zero(t, cache.cancelCalls, "the slot now belongs to the caller until forwarding finishes")
	result.ReleaseFunc()
	result.ReleaseFunc()
	require.Equal(t, []string{cache.ticket}, cache.releasedRequestIDs)
	for _, ticket := range cache.polls {
		require.Equal(t, cache.ticket, ticket)
	}
}

func TestAccountWaitServiceCleansAmbiguousRedisErrors(t *testing.T) {
	for _, enqueueFailure := range []bool{false, true} {
		name := "acquire-ack-lost"
		if enqueueFailure {
			name = "enqueue-ack-lost"
		}
		t.Run(name, func(t *testing.T) {
			cache := &accountWaitCacheStub{}
			failure := errors.New("redis acknowledgement lost")
			if enqueueFailure {
				cache.enqueueErr = failure
			} else {
				cache.acquireErr = failure
			}
			ctx, stop := context.WithCancel(context.Background())
			acquire, cancel, err := NewConcurrencyService(cache).NewAccountSlotWait(ctx, 10, 1, time.Minute)
			if enqueueFailure {
				require.ErrorIs(t, err, failure)
				require.Nil(t, acquire)
				stop()
			} else {
				require.NoError(t, err)
				_, err = acquire()
				require.ErrorIs(t, err, failure)
				stop()
				cancel()
				cancel()
			}
			require.Equal(t, 1, cache.cancelCalls)
			require.NoError(t, cache.cancelContextError, "cleanup must survive request cancellation")
		})
	}
}
