package service

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
)

// AccountSlotWaitCache keeps the same ticket across polls and gateway replicas.
type AccountSlotWaitCache interface {
	EnqueueAccountSlotWait(context.Context, int64, string, time.Duration) error
	AcquireWaitingAccountSlot(context.Context, int64, int, string) (bool, error)
	CancelAccountSlotWait(context.Context, int64, string) error
}

// NewAccountSlotWait registers one bounded wait, without changing account selection.
// Cancel must run on every exit; a successful acquisition transfers slot ownership
// to the returned ReleaseFunc instead.
func (s *ConcurrencyService) NewAccountSlotWait(ctx context.Context, accountID int64, maxConcurrency int, timeout time.Duration) (func() (*AcquireResult, error), func(), error) {
	cache, ok := s.cache.(AccountSlotWaitCache)
	if !ok || maxConcurrency <= 0 {
		return func() (*AcquireResult, error) {
			return s.AcquireAccountSlot(ctx, accountID, maxConcurrency)
		}, func() {}, nil
	}
	ticket := generateRequestID()
	var acquired atomic.Bool
	var cancelOnce sync.Once
	cancel := func() {
		cancelOnce.Do(func() {
			if acquired.Load() {
				return
			}
			cleanupCtx, done := context.WithTimeout(context.Background(), 5*time.Second)
			defer done()
			if err := cache.CancelAccountSlotWait(cleanupCtx, accountID, ticket); err != nil {
				logger.LegacyPrintf("service.concurrency", "Warning: failed to cancel account wait for %d: %v", accountID, err)
			}
		})
	}
	if err := cache.EnqueueAccountSlotWait(ctx, accountID, ticket, timeout); err != nil {
		cancel()
		return nil, nil, err
	}
	acquire := func() (*AcquireResult, error) {
		ok, err := cache.AcquireWaitingAccountSlot(ctx, accountID, maxConcurrency, ticket)
		if err != nil {
			return nil, err
		}
		if !ok {
			return &AcquireResult{}, nil
		}
		acquired.Store(true)
		var releaseOnce sync.Once
		return &AcquireResult{Acquired: true, ReleaseFunc: func() {
			releaseOnce.Do(func() {
				releaseCtx, done := context.WithTimeout(context.Background(), 5*time.Second)
				defer done()
				if err := s.cache.ReleaseAccountSlot(releaseCtx, accountID, ticket); err != nil {
					logger.LegacyPrintf("service.concurrency", "Warning: failed to release account slot for %d: %v", accountID, err)
				}
			})
		}}, nil
	}
	return acquire, cancel, nil
}
