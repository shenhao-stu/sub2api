//go:build unit

package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func dedicatedFallbackChain() *affinityScheduleFixture {
	f := newAffinityScheduleFixture(nil)
	base := f.repo.accounts[1]
	f.repo.accounts = nil
	for i, priority := range []int{0, 1, 10, 20, -1} {
		a := base
		a.ID, a.Priority = int64(i+1), priority
		a.Extra = map[string]any{"anthropic_passthrough": true}
		if i < 2 || i == 4 {
			tag := "getoken:1"
			if i == 4 {
				tag = "getoken:other"
			}
			a.Extra[AffinityTagsExtraKey] = []string{tag}
			a.Extra[AffinityWaitBeforeSharedExtraKey] = true
		}
		f.repo.accounts = append(f.repo.accounts, a)
	}
	f.repo.accountsByID = map[int64]*Account{}
	for i := range f.repo.accounts {
		f.repo.accountsByID[f.repo.accounts[i].ID] = &f.repo.accounts[i]
	}
	return f
}

func TestDedicatedFallbackEntireChain(t *testing.T) {
	for _, cause := range []string{"predispatch_exclusion", "quota_cooldown"} {
		t.Run(cause, func(t *testing.T) {
			f := dedicatedFallbackChain()
			ctx := WithAccountAffinityTags(context.Background(), []string{"getoken:1"})
			excluded := map[int64]struct{}{}
			for _, want := range []int64{1, 2, 3, 4} {
				got, err := f.svc.SelectAccountWithLoadAwareness(ctx, nil, "chain", affinityTestModel, excluded, "", 0)
				require.NoError(t, err)
				require.Equal(t, want, got.Account.ID)
				require.True(t, got.Acquired)
				got.ReleaseFunc()
				if cause == "predispatch_exclusion" {
					excluded[want] = struct{}{}
				} else {
					reset := time.Now().Add(time.Hour)
					f.repo.accountsByID[want].RateLimitResetAt = &reset
				}
			}
			got, err := f.svc.SelectAccountWithLoadAwareness(ctx, nil, "chain", affinityTestModel, excluded, "", 0)
			require.Error(t, err)
			require.Nil(t, got, "another user's healthy dedicated account is never a fallback")
		})
	}
}

func TestDedicatedFallbackBusyQueuesAndSharedStickyDoesNotBounce(t *testing.T) {
	f := dedicatedFallbackChain()
	ctx := WithAccountAffinityTags(context.Background(), []string{"getoken:1"})
	f.concurrencyCache.acquireResults = map[int64]bool{1: false, 2: false}
	f.concurrencyCache.loadMap = map[int64]*AccountLoadInfo{
		1: {AccountID: 1, LoadRate: 100}, 2: {AccountID: 2, LoadRate: 100},
	}
	got, err := f.svc.SelectAccountWithLoadAwareness(ctx, nil, "", affinityTestModel, nil, "", 0)
	require.NoError(t, err)
	require.NotNil(t, got.WaitPlan)
	require.Contains(t, []int64{1, 2}, got.Account.ID)
	require.False(t, got.Acquired)
	f.cache.sessionBindings["shared-session"] = 3
	got, err = f.svc.SelectAccountWithLoadAwareness(ctx, nil, "shared-session", affinityTestModel, nil, "", 0)
	require.NoError(t, err)
	require.Equal(t, int64(3), got.Account.ID)
	require.True(t, got.Acquired)
	got.ReleaseFunc()
}
