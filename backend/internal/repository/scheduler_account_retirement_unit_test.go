//go:build unit

package repository

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestSchedulerCacheRetirementFencesLateAccountAndSnapshotWrites(t *testing.T) {
	ctx := context.Background()
	cache, mr := newSchedulerCacheUnitWithRedis(t)
	deleted := service.Account{ID: 23686, Platform: service.PlatformTypeSafe, Type: service.AccountTypeAPIKey}
	live := service.Account{ID: 7315, Platform: service.PlatformTypeSafe, Type: service.AccountTypeAPIKey}
	require.NoError(t, cache.SetAccount(ctx, &deleted))
	require.NoError(t, cache.UpdateLastUsed(ctx, map[int64]time.Time{deleted.ID: time.Now()}))
	require.NoError(t, cache.RetireAccount(ctx, deleted.ID))
	require.NoError(t, cache.RetireAccount(ctx, deleted.ID))
	retired, err := mr.SIsMember(schedulerRetiredAccountsKey, strconv.FormatInt(deleted.ID, 10))
	require.NoError(t, err)
	require.True(t, retired)
	ttl, err := cache.rdb.TTL(ctx, schedulerRetiredAccountsKey).Result()
	require.NoError(t, err)
	require.Equal(t, time.Duration(-1), ttl, "retirement must not expire while delayed work may still hold the account")
	mr.FastForward(30 * 24 * time.Hour)

	// A DB read started before the deletion can finish at any later time.
	require.NoError(t, cache.SetAccount(ctx, &deleted))
	require.NoError(t, cache.UpdateLastUsed(ctx, map[int64]time.Time{deleted.ID: time.Now()}))
	bucket := service.SchedulerBucket{GroupID: 46, Platform: service.PlatformTypeSafe, Mode: service.SchedulerModeSingle}
	token, err := cache.CaptureBucketWriteToken(ctx, bucket)
	require.NoError(t, err)
	ids, err := cache.SetSnapshotAndReturnAccountIDs(ctx, bucket, token, []service.Account{deleted, live, deleted})
	require.NoError(t, err)
	require.Equal(t, []int64{live.ID}, ids)
	accounts, hit, err := cache.GetSnapshot(ctx, bucket)
	require.NoError(t, err)
	require.True(t, hit)
	require.Len(t, accounts, 1)
	require.Equal(t, live.ID, accounts[0].ID)
	for _, key := range []string{schedulerAccountKey("23686"), schedulerAccountMetaKey("23686"), schedulerLastUsedKey("23686")} {
		require.False(t, mr.Exists(key), key)
	}
}

func TestSchedulerCacheRetirementBetweenPayloadAndBucketPublication(t *testing.T) {
	ctx := context.Background()
	cache := newSchedulerCacheUnit(t)
	stale := service.Account{ID: 23686, Platform: service.PlatformTypeSafe, Type: service.AccountTypeAPIKey}
	live := service.Account{ID: 7323, Platform: service.PlatformTypeSafe, Type: service.AccountTypeAPIKey}
	ids, err := cache.writeAccountIDs(ctx, []service.Account{stale, live})
	require.NoError(t, err)
	require.NoError(t, cache.RetireAccount(ctx, stale.ID))
	// Both a full rebuild's pending member list and another mode's ID-only
	// reuse can publish IDs captured before RetireAccount. Neither may make
	// the healthy members of that bucket fall back forever.
	for _, mode := range []string{service.SchedulerModeSingle, service.SchedulerModeForced} {
		bucket := service.SchedulerBucket{GroupID: 46, Platform: service.PlatformTypeSafe, Mode: mode}
		token, err := cache.CaptureBucketWriteToken(ctx, bucket)
		require.NoError(t, err)
		require.NoError(t, cache.SetSnapshotByAccountIDs(ctx, bucket, token, ids))
		accounts, hit, err := cache.GetSnapshot(ctx, bucket)
		require.NoError(t, err)
		require.True(t, hit)
		require.Len(t, accounts, 1)
		require.Equal(t, live.ID, accounts[0].ID)
	}
	// An ordinary cache miss must still trigger rehydration rather than
	// silently dropping an account that has not been deleted.
	require.NoError(t, cache.DeleteAccount(ctx, live.ID))
	_, hit, err := cache.GetSnapshot(ctx, service.SchedulerBucket{GroupID: 46, Platform: service.PlatformTypeSafe, Mode: service.SchedulerModeSingle})
	require.NoError(t, err)
	require.False(t, hit)
}

func TestSchedulerCacheOrdinaryClearAndMarshalFailureRemainReloadable(t *testing.T) {
	ctx := context.Background()
	cache, mr := newSchedulerCacheUnitWithRedis(t)
	account := service.Account{ID: 7315, Platform: service.PlatformTypeSafe, Type: service.AccountTypeAPIKey}
	require.NoError(t, cache.SetAccount(ctx, &account))
	require.NoError(t, cache.DeleteAccount(ctx, account.ID))
	require.NoError(t, cache.SetAccount(ctx, &account))
	cached, err := cache.GetAccount(ctx, account.ID)
	require.NoError(t, err)
	require.NotNil(t, cached)
	badTime := time.Date(10000, time.January, 1, 0, 0, 0, 0, time.UTC)
	account.ExpiresAt = &badTime
	require.NoError(t, cache.SetAccount(ctx, &account))
	cached, err = cache.GetAccount(ctx, account.ID)
	require.NoError(t, err)
	require.Nil(t, cached)
	account.ExpiresAt = nil
	require.NoError(t, cache.SetAccount(ctx, &account))
	cached, err = cache.GetAccount(ctx, account.ID)
	require.NoError(t, err)
	require.NotNil(t, cached)
	require.False(t, mr.Exists(schedulerRetiredAccountsKey))
}

func TestSchedulerCacheConcurrentRetirementCannotBeOverwritten(t *testing.T) {
	ctx := context.Background()
	cache, mr := newSchedulerCacheUnitWithRedis(t)
	account := service.Account{ID: 23686, Platform: service.PlatformTypeSafe, Type: service.AccountTypeAPIKey}
	var wg sync.WaitGroup
	errs := make(chan error, 65)
	start := make(chan struct{})
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- cache.SetAccount(ctx, &account)
			errs <- cache.UpdateLastUsed(ctx, map[int64]time.Time{account.ID: time.Now()})
		}()
	}
	wg.Add(1)
	go func() { defer wg.Done(); <-start; errs <- cache.RetireAccount(ctx, account.ID) }()
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.False(t, mr.Exists(schedulerAccountKey("23686")))
	require.False(t, mr.Exists(schedulerAccountMetaKey("23686")))
	require.False(t, mr.Exists(schedulerLastUsedKey("23686")))
}

func TestSchedulerCacheRetirementRejectsLegacyUnfencedWrites(t *testing.T) {
	ctx := context.Background()
	cache, mr := newSchedulerCacheUnitWithRedis(t)
	stale := service.Account{ID: 23686, Platform: service.PlatformTypeSafe, Type: service.AccountTypeAPIKey}
	live := service.Account{ID: 7315, Platform: service.PlatformTypeSafe, Type: service.AccountTypeAPIKey}
	bucket := service.SchedulerBucket{GroupID: 46, Platform: service.PlatformTypeSafe, Mode: service.SchedulerModeSingle}
	token, err := cache.CaptureBucketWriteToken(ctx, bucket)
	require.NoError(t, err)
	require.NoError(t, cache.SetSnapshot(ctx, bucket, token, []service.Account{stale, live}))
	require.NoError(t, cache.RetireAccount(ctx, stale.ID))
	// The previous release can still run a snapshot rebuild while draining
	// long requests. Simulate its unconditional SET, bypassing the new writer.
	payload, err := json.Marshal(stale)
	require.NoError(t, err)
	mr.Set(schedulerAccountKey("23686"), string(payload))
	mr.Set(schedulerAccountMetaKey("23686"), string(payload))
	cached, err := cache.GetAccount(ctx, stale.ID)
	require.NoError(t, err)
	require.Nil(t, cached)
	accounts, hit, err := cache.GetSnapshot(ctx, bucket)
	require.NoError(t, err)
	require.True(t, hit)
	require.Len(t, accounts, 1)
	require.Equal(t, live.ID, accounts[0].ID)
	// Retired legacy data is ignored before decoding, even if corrupt.
	mr.Set(schedulerAccountKey("23686"), "invalid legacy payload")
	mr.Set(schedulerAccountMetaKey("23686"), "invalid legacy metadata")
	cached, err = cache.GetAccount(ctx, stale.ID)
	require.NoError(t, err)
	require.Nil(t, cached)
	accounts, hit, err = cache.GetSnapshot(ctx, bucket)
	require.NoError(t, err)
	require.True(t, hit)
	require.Len(t, accounts, 1)
	require.NoError(t, cache.RetireAccount(ctx, stale.ID))
	require.False(t, mr.Exists(schedulerAccountKey("23686")))
	require.False(t, mr.Exists(schedulerAccountMetaKey("23686")))
}
