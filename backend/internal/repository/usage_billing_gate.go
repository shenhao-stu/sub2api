package repository

import (
	"context"
	"database/sql"
	"sync"

	"golang.org/x/sync/semaphore"
)

// A balance row is serial inside PostgreSQL already. Wait before borrowing a
// connection so one busy user cannot fill the pool with blocked transactions.
// This is admission control only: the database still owns atomicity and dedup,
// including across processes during a rolling release.
var billingUserGate = struct {
	sync.Mutex
	entries map[billingUserGateKey]*billingUserGateEntry
}{entries: make(map[billingUserGateKey]*billingUserGateEntry)}

type billingUserGateKey struct {
	db     *sql.DB
	userID int64
}

type billingUserGateEntry struct {
	permit *semaphore.Weighted
	refs   int
}

func acquireBillingUser(ctx context.Context, db *sql.DB, userID int64) (func(), error) {
	key := billingUserGateKey{db: db, userID: userID}
	billingUserGate.Lock()
	entry := billingUserGate.entries[key]
	if entry == nil {
		entry = &billingUserGateEntry{permit: semaphore.NewWeighted(1)}
		billingUserGate.entries[key] = entry
	}
	entry.refs++
	billingUserGate.Unlock()
	dropRef := func() {
		billingUserGate.Lock()
		defer billingUserGate.Unlock()
		entry.refs--
		if entry.refs == 0 {
			delete(billingUserGate.entries, key)
		}
	}
	if err := entry.permit.Acquire(ctx, 1); err != nil {
		dropRef()
		return nil, err
	}
	return func() {
		entry.permit.Release(1)
		dropRef()
	}, nil
}
