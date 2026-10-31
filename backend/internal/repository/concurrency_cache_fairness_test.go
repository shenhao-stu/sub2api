package repository

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestAccountSlotDoesNotOvertakeRegisteredWaiter(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	cache := NewConcurrencyCache(client, 15, 900)
	ctx := context.Background()
	ok, err := cache.AcquireAccountSlot(ctx, 10, 1, "running")
	require.NoError(t, err)
	require.True(t, ok)
	now, err := client.Time(ctx).Result()
	require.NoError(t, err)
	require.NoError(t, client.ZAdd(ctx, "concurrency:account_wait:10", redis.Z{Score: 1, Member: "older"}).Err())
	require.NoError(t, client.ZAdd(ctx, "concurrency:account_wait_expiry:10", redis.Z{Score: float64(now.Add(time.Minute).UnixMilli()), Member: "older"}).Err())
	require.NoError(t, cache.ReleaseAccountSlot(ctx, 10, "running"))
	ok, err = cache.AcquireAccountSlot(ctx, 10, 1, "newcomer")
	require.NoError(t, err)
	require.False(t, ok, "new requests must not steal a released slot from a registered waiter")
}

func TestAccountWaitFIFO(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	testAccountWaitFIFO(t, client)
}

func TestAccountWaitRespectsLiveSlotsAndHandoff(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	cache := NewConcurrencyCache(client, 15, 900).(*concurrencyCache)
	live := service.LiveConcurrencyCache(cache)
	ctx := context.Background()
	ok, err := cache.AcquireAccountSlot(ctx, 10, 1, "owner")
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, cache.EnqueueAccountSlotWait(ctx, 10, "waiting", time.Minute))
	// The owner may hand an already acquired slot to a WebSocket; it is not a newcomer.
	ok, err = live.AcquireLiveLease(ctx, 10, 1, 20, 2, 30, "live-owner", true)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, cache.ReleaseAccountSlot(ctx, 10, "owner"))
	ok, err = cache.AcquireWaitingAccountSlot(ctx, 10, 1, "waiting")
	require.NoError(t, err)
	require.False(t, ok, "live leases still count toward the hard account limit")
	require.NoError(t, live.ReleaseLiveLease(ctx, 10, 20, 30, "live-owner"))
	ok, err = live.AcquireLiveLease(ctx, 10, 1, 20, 2, 30, "new-live", false)
	require.NoError(t, err)
	require.False(t, ok, "new live leases must not bypass older HTTP waiters")
	ok, err = cache.AcquireWaitingAccountSlot(ctx, 10, 1, "waiting")
	require.NoError(t, err)
	require.True(t, ok)
}

type accountWaitLostAckCache struct {
	service.ConcurrencyCache
	service.AccountSlotWaitCache
	enqueueFailure bool
}

func (c accountWaitLostAckCache) EnqueueAccountSlotWait(ctx context.Context, id int64, ticket string, ttl time.Duration) error {
	if err := c.AccountSlotWaitCache.EnqueueAccountSlotWait(ctx, id, ticket, ttl); err != nil {
		return err
	}
	if c.enqueueFailure {
		return errors.New("simulated lost enqueue acknowledgement after Redis applied it")
	}
	return nil
}

func (c accountWaitLostAckCache) AcquireWaitingAccountSlot(ctx context.Context, id int64, limit int, ticket string) (bool, error) {
	ok, err := c.AccountSlotWaitCache.AcquireWaitingAccountSlot(ctx, id, limit, ticket)
	if err != nil {
		return ok, err
	}
	if ok {
		return false, errors.New("simulated lost acquire acknowledgement after Redis applied it")
	}
	return ok, nil
}

func testAccountWaitLostAcknowledgement(t *testing.T, cache *concurrencyCache, accountID int64) {
	t.Helper()
	for _, enqueueFailure := range []bool{false, true} {
		wrapped := accountWaitLostAckCache{cache, cache, enqueueFailure}
		ctx, stop := context.WithCancel(context.Background())
		acquire, cancel, err := service.NewConcurrencyService(wrapped).NewAccountSlotWait(ctx, accountID, 1, time.Minute)
		if enqueueFailure {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			_, err = acquire()
			require.Error(t, err)
			stop()
			cancel()
		}
		stop()
		queue, expiry, _ := accountWaitKeys(accountID)
		for _, key := range []string{queue, expiry, accountSlotKey(accountID)} {
			n, err := cache.rdb.ZCard(context.Background(), key).Result()
			require.NoError(t, err)
			require.Zero(t, n, "lost acknowledgement must not retain queue/slot: %s", key)
		}
	}
}

func testAccountWaitMixedVersion(t *testing.T, cache *concurrencyCache, accountID int64) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, cache.EnqueueAccountSlotWait(ctx, accountID, "new-waiter", time.Minute))
	var wg sync.WaitGroup
	results := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i == 0 {
				_, err := cache.AcquireWaitingAccountSlot(ctx, accountID, 1, "new-waiter")
				results <- err
				return
			}
			// The old gateway supplies only regular/live keys, so it ignores FIFO
			// during a rollout but must still share the same hard limit.
			_, _, err := runScriptInt64Pair(ctx, cache.rdb, acquireScript,
				[]string{accountSlotKey(accountID), liveAccountSlotKey(accountID)}, 1, cache.slotTTLSeconds, fmt.Sprintf("old-%d", i))
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
	n, err := cache.GetAccountConcurrency(ctx, accountID)
	require.NoError(t, err)
	require.Equal(t, 1, n, "old/new overlap must never exceed the hard account limit")
	for i := 1; i < 16; i++ {
		require.NoError(t, cache.ReleaseAccountSlot(ctx, accountID, fmt.Sprintf("old-%d", i)))
	}
	require.NoError(t, cache.CancelAccountSlotWait(ctx, accountID, "new-waiter"))
	require.NoError(t, cache.EnqueueAccountSlotWait(ctx, accountID, "after-old-drain", time.Minute))
	ok, err := cache.AcquireAccountSlot(ctx, accountID, 1, "newcomer")
	require.NoError(t, err)
	require.False(t, ok, "FIFO applies after the legacy gateway drains")
	ok, err = cache.AcquireWaitingAccountSlot(ctx, accountID, 1, "after-old-drain")
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, cache.ReleaseAccountSlot(ctx, accountID, "after-old-drain"))
}

func TestAccountWaitFillsAvailableCapacityInOrder(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	cache := NewConcurrencyCache(client, 15, 900).(*concurrencyCache)
	ctx := context.Background()
	for _, ticket := range []string{"first", "second", "third"} {
		require.NoError(t, cache.EnqueueAccountSlotWait(ctx, 10, ticket, time.Minute))
	}
	ok, err := cache.AcquireWaitingAccountSlot(ctx, 10, 2, "third")
	require.NoError(t, err)
	require.False(t, ok)
	for _, ticket := range []string{"second", "first"} {
		ok, err = cache.AcquireWaitingAccountSlot(ctx, 10, 2, ticket)
		require.NoError(t, err)
		require.True(t, ok, "the first two queue positions may fill two available slots")
	}
	ok, err = cache.AcquireWaitingAccountSlot(ctx, 10, 2, "third")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestAccountWaitRealRedis(t *testing.T) {
	addr := os.Getenv("ACCOUNT_WAIT_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("ACCOUNT_WAIT_TEST_REDIS_ADDR must point to an isolated test Redis")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	testAccountWaitFIFO(t, client)
}

func testAccountWaitFIFO(t *testing.T, client *redis.Client) {
	t.Helper()
	cache := NewConcurrencyCache(client, 15, 900).(*concurrencyCache)
	ctx := context.Background()
	accountID := time.Now().UnixNano()
	queue, expiry, sequence := accountWaitKeys(accountID)
	t.Cleanup(func() {
		_ = client.Del(ctx, queue, expiry, sequence, accountSlotKey(accountID), liveAccountSlotKey(accountID)).Err()
		_ = client.ZRem(ctx, accountActiveIndexKey, accountID).Err()
	})
	ok, err := cache.AcquireAccountSlot(ctx, accountID, 1, "owner")
	require.NoError(t, err)
	require.True(t, ok)
	for _, id := range []string{"first", "second", "third"} {
		require.NoError(t, cache.EnqueueAccountSlotWait(ctx, accountID, id, time.Minute))
	}
	// Polling/re-registering must not move the oldest request behind newer ones.
	require.NoError(t, cache.EnqueueAccountSlotWait(ctx, accountID, "first", time.Minute))
	require.NoError(t, cache.ReleaseAccountSlot(ctx, accountID, "owner"))
	for _, id := range []string{"third", "second"} {
		ok, err = cache.AcquireWaitingAccountSlot(ctx, accountID, 1, id)
		require.NoError(t, err)
		require.False(t, ok)
	}
	ok, err = cache.AcquireWaitingAccountSlot(ctx, accountID, 1, "first")
	require.NoError(t, err)
	require.True(t, ok)
	// Lost acquire acknowledgements may retry the same ticket, never add a slot.
	ok, err = cache.AcquireWaitingAccountSlot(ctx, accountID, 1, "first")
	require.NoError(t, err)
	require.True(t, ok)
	count, err := cache.GetAccountConcurrency(ctx, accountID)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.NoError(t, cache.ReleaseAccountSlot(ctx, accountID, "first"))
	require.NoError(t, cache.CancelAccountSlotWait(ctx, accountID, "second"))
	require.NoError(t, cache.CancelAccountSlotWait(ctx, accountID, "second"))
	ok, err = cache.AcquireWaitingAccountSlot(ctx, accountID, 1, "third")
	require.NoError(t, err)
	require.True(t, ok, "cancelled head must not strand the next waiter")
	require.NoError(t, cache.CancelAccountSlotWait(ctx, accountID, "third"))
	count, err = cache.GetAccountConcurrency(ctx, accountID)
	require.NoError(t, err)
	require.Zero(t, count, "ambiguous acquisition cleanup must also release its own slot")

	// A crashed head expires without requiring a live application to clean it.
	require.NoError(t, cache.EnqueueAccountSlotWait(ctx, accountID, "crashed", time.Minute))
	require.NoError(t, cache.EnqueueAccountSlotWait(ctx, accountID, "survivor", 2*time.Minute))
	now, err := client.Time(ctx).Result()
	require.NoError(t, err)
	require.NoError(t, client.ZAdd(ctx, expiry, redis.Z{Score: float64(now.Add(-time.Second).UnixMilli()), Member: "crashed"}).Err())
	ok, err = cache.AcquireWaitingAccountSlot(ctx, accountID, 1, "survivor")
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, cache.ReleaseAccountSlot(ctx, accountID, "survivor"))

	// A shorter new wait must not shorten an older ticket's Redis key TTL.
	require.NoError(t, cache.EnqueueAccountSlotWait(ctx, accountID, "long", 2*time.Minute))
	require.NoError(t, cache.EnqueueAccountSlotWait(ctx, accountID, "short", time.Second))
	ttl, err := client.PTTL(ctx, queue).Result()
	require.NoError(t, err)
	require.Greater(t, ttl, time.Minute)
	require.NoError(t, cache.CancelAccountSlotWait(ctx, accountID, "long"))
	require.NoError(t, cache.CancelAccountSlotWait(ctx, accountID, "short"))

	// Many replicas compete for a single released slot; only the queue head wins.
	const contenders = 24
	for i := 0; i < contenders; i++ {
		require.NoError(t, cache.EnqueueAccountSlotWait(ctx, accountID, fmt.Sprint(i), time.Minute))
	}
	type result struct {
		id       int
		acquired bool
		err      error
	}
	results := make(chan result, contenders)
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			won, err := cache.AcquireWaitingAccountSlot(ctx, accountID, 1, fmt.Sprint(id))
			results <- result{id, won, err}
		}(i)
	}
	wg.Wait()
	close(results)
	winners := 0
	for r := range results {
		require.NoError(t, r.err)
		if r.acquired {
			winners++
			require.Zero(t, r.id)
		}
	}
	require.Equal(t, 1, winners)
	for i := 0; i < contenders; i++ {
		require.NoError(t, cache.CancelAccountSlotWait(ctx, accountID, fmt.Sprint(i)))
	}
	// Fully cancelled queues leave the ordinary fast path available again.
	ok, err = cache.AcquireAccountSlot(ctx, accountID, 1, "after-cancel")
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, cache.ReleaseAccountSlot(ctx, accountID, "after-cancel"))
	testAccountWaitLostAcknowledgement(t, cache, accountID)
	testAccountWaitMixedVersion(t, cache, accountID)
}
