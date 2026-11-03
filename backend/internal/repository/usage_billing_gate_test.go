package repository

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBillingGateCancellationAndIsolation(t *testing.T) {
	db := &sql.DB{}
	release, err := acquireBillingUser(t.Context(), db, 1)
	require.NoError(t, err)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = acquireBillingUser(canceled, db, 1)
	require.ErrorIs(t, err, context.Canceled)
	other, err := acquireBillingUser(t.Context(), db, 2)
	require.NoError(t, err)
	other()
	release()
	billingUserGate.Lock()
	_, retained := billingUserGate.entries[billingUserGateKey{db: db, userID: 1}]
	billingUserGate.Unlock()
	require.False(t, retained, "canceled waiters must not leak gate entries")
}

func TestBillingGate500ConcurrentOwners(t *testing.T) {
	db := &sql.DB{}
	var active, peak atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 500 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			release, err := acquireBillingUser(t.Context(), db, 1)
			if err != nil {
				t.Error(err)
				return
			}
			n := active.Add(1)
			for old := peak.Load(); n > old; old = peak.Load() {
				if peak.CompareAndSwap(old, n) {
					break
				}
			}
			active.Add(-1)
			release()
		}()
	}
	close(start)
	wg.Wait()
	require.Equal(t, int32(1), peak.Load())
	billingUserGate.Lock()
	_, retained := billingUserGate.entries[billingUserGateKey{db: db, userID: 1}]
	billingUserGate.Unlock()
	require.False(t, retained)
}
