package handler

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestAccountWaitHelperCancellationReleasesTicket(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "timeout", true: "client-cancelled"}[cancelled], func(t *testing.T) {
			server := miniredis.RunT(t)
			client := redis.NewClient(&redis.Options{Addr: server.Addr()})
			t.Cleanup(func() { _ = client.Close() })
			cache := repository.NewConcurrencyCache(client, 15, 900)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ok, err := cache.AcquireAccountSlot(ctx, 10, 1, "owner")
			require.NoError(t, err)
			require.True(t, ok)
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest("POST", "/v1/messages", nil).WithContext(ctx)
			helper := NewConcurrencyHelper(service.NewConcurrencyService(cache), SSEPingFormatClaude, 5*time.Millisecond)
			if cancelled {
				time.AfterFunc(20*time.Millisecond, cancel)
			}
			started := false
			release, err := helper.AcquireAccountSlotWithWaitTimeout(c, 10, 1, 50*time.Millisecond, true, &started)
			require.Error(t, err)
			require.Nil(t, release)
			if cancelled {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				var concurrencyErr *ConcurrencyError
				require.ErrorAs(t, err, &concurrencyErr)
				require.True(t, concurrencyErr.IsTimeout)
			}
			require.True(t, started)
			require.Contains(t, rec.Body.String(), "event: ping")
			require.NoError(t, cache.ReleaseAccountSlot(context.Background(), 10, "owner"))
			ok, err = cache.AcquireAccountSlot(context.Background(), 10, 1, "new-request")
			require.NoError(t, err)
			require.True(t, ok, "timed-out/cancelled waiters must not block subsequent clients")
		})
	}
}

func TestAccountWaitHelperTransfersAcquiredSlot(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	cache := repository.NewConcurrencyCache(client, 15, 900)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
	helper := NewConcurrencyHelper(service.NewConcurrencyService(cache), SSEPingFormatClaude, time.Millisecond)
	started := false
	release, err := helper.AcquireAccountSlotWithWaitTimeout(c, 10, 1, time.Second, false, &started)
	require.NoError(t, err)
	require.NotNil(t, release)
	count, err := cache.GetAccountConcurrency(context.Background(), 10)
	require.NoError(t, err)
	require.Equal(t, 1, count, "wait cleanup must not release the forwarded request's slot")
	release()
	release()
	count, err = cache.GetAccountConcurrency(context.Background(), 10)
	require.NoError(t, err)
	require.Zero(t, count)
}
