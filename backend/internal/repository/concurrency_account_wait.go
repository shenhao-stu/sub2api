package repository

import (
	"context"
	"strconv"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

var _ service.AccountSlotWaitCache = (*concurrencyCache)(nil)

// Queue position and expiry are separate: polling never resets a ticket's age.
const reapAccountWaitersLua = `
	local function reapWaiters(queue, expiry, nowMillis)
		local expired = redis.call('ZRANGEBYSCORE', expiry, '-inf', nowMillis)
		for _, member in ipairs(expired) do
			redis.call('ZREM', queue, member)
			redis.call('ZREM', expiry, member)
		end
	end
`

var enqueueAccountWaitScript = redis.NewScript(reapAccountWaitersLua + `
	redis.replicate_commands()
	local queue, expiry, sequence = KEYS[1], KEYS[2], KEYS[3]
	local ticket, ttl = ARGV[1], tonumber(ARGV[2])
	local clock = redis.call('TIME')
	local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
	reapWaiters(queue, expiry, now)
	if redis.call('ZSCORE', queue, ticket) == false then
		redis.call('ZADD', queue, redis.call('INCR', sequence), ticket)
		redis.call('ZADD', expiry, now + ttl, ticket)
	end
	for _, key in ipairs(KEYS) do
		if redis.call('PTTL', key) < ttl then redis.call('PEXPIRE', key, ttl) end
	end
	return 1
`)

var cancelAccountWaitScript = redis.NewScript(`
	for _, key in ipairs(KEYS) do redis.call('ZREM', key, ARGV[1]) end
	return 1
`)

func accountWaitKeys(accountID int64) (string, string, string) {
	id := strconv.FormatInt(accountID, 10)
	return "concurrency:account_wait:" + id, "concurrency:account_wait_expiry:" + id, "concurrency:account_wait_sequence:" + id
}

func (c *concurrencyCache) EnqueueAccountSlotWait(ctx context.Context, accountID int64, ticket string, timeout time.Duration) error {
	queue, expiry, sequence := accountWaitKeys(accountID)
	_, err := enqueueAccountWaitScript.Run(ctx, c.rdb, []string{queue, expiry, sequence}, ticket, max(timeout.Milliseconds(), 1)).Result()
	return err
}

func (c *concurrencyCache) AcquireWaitingAccountSlot(ctx context.Context, accountID int64, maxConcurrency int, ticket string) (bool, error) {
	return c.acquireAccountSlot(ctx, accountID, maxConcurrency, ticket, true)
}

func (c *concurrencyCache) CancelAccountSlotWait(ctx context.Context, accountID int64, ticket string) error {
	queue, expiry, _ := accountWaitKeys(accountID)
	// Also reclaim an acquisition whose Redis acknowledgement was lost.
	_, err := cancelAccountWaitScript.Run(ctx, c.rdb, []string{queue, expiry, accountSlotKey(accountID)}, ticket).Result()
	if err == nil {
		c.refreshAccountActiveIndex(ctx, accountID)
	}
	return err
}
