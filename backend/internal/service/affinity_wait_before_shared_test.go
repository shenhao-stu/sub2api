//go:build unit

package service

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/stretchr/testify/require"
)

func TestAffinityWaitBeforeShared(t *testing.T) {
	for _, mode := range []string{"normal", "routed", "legacy", "load-error"} {
		for _, busy := range []string{"full", "race"} {
			t.Run(mode+"/"+busy, func(t *testing.T) {
				f := newAffinityScheduleFixture(nil)
				f.repo.accounts[0].Extra["affinity_wait_before_shared"] = true
				f.concurrencyCache.acquireResults = map[int64]bool{1: false}
				if busy == "full" {
					f.concurrencyCache.loadMap = map[int64]*AccountLoadInfo{1: {AccountID: 1, CurrentConcurrency: 1, LoadRate: 100}}
				}
				ctx := WithAccountAffinityTags(context.Background(), []string{"getoken:1"})
				var gid *int64
				switch mode {
				case "routed":
					g := &Group{ID: 10, Platform: PlatformAnthropic, Status: StatusActive, Hydrated: true, ModelRoutingEnabled: true, ModelRouting: map[string][]int64{affinityTestModel: {1, 2}}}
					gid = &g.ID
					f.svc.groupRepo = &mockGroupRepoForGateway{groups: map[int64]*Group{10: g}}
					ctx = context.WithValue(ctx, ctxkey.Group, g)
				case "legacy":
					f.svc.cfg.Gateway.Scheduling.LoadBatchEnabled = false
				case "load-error":
					f.concurrencyCache.loadBatchErr = errAffinityLoadBatch
				}
				got, err := f.svc.SelectAccountWithLoadAwareness(ctx, gid, "", affinityTestModel, nil, "", 0)
				require.NoError(t, err)
				require.Equal(t, int64(1), got.Account.ID)
				require.False(t, got.Acquired)
				require.NotNil(t, got.WaitPlan)
				require.Equal(t, int64(1), got.WaitPlan.AccountID)
			})
		}
	}
}

func TestAffinityWaitBeforeSharedPreservesEligibilityAndSticky(t *testing.T) {
	for _, tc := range []struct {
		name         string
		adjust       func(*affinityScheduleFixture)
		user         string
		sticky, want int64
	}{
		{"quota cooldown", func(f *affinityScheduleFixture) {
			reset := time.Now().Add(time.Hour)
			f.repo.accounts[0].RateLimitResetAt = &reset
		}, "1", 0, 2},
		{"manual pause", func(f *affinityScheduleFixture) { f.repo.accounts[0].Schedulable = false }, "1", 0, 2},
		{"wrong user", func(f *affinityScheduleFixture) {}, "2", 0, 2},
		{"anonymous", func(f *affinityScheduleFixture) {}, "", 0, 2},
		{"existing shared sticky", func(f *affinityScheduleFixture) {}, "1", 2, 2},
		{"default policy still spills on busy", func(f *affinityScheduleFixture) {
			f.repo.accounts[0].Extra["affinity_wait_before_shared"] = false
			f.concurrencyCache.acquireResults = map[int64]bool{1: false}
		}, "1", 0, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAffinityScheduleFixture(map[string]int64{"s": tc.sticky})
			f.repo.accounts[0].Extra["affinity_wait_before_shared"] = true
			tc.adjust(f)
			ctx := context.Background()
			if tc.user != "" {
				ctx = WithAccountAffinityTags(ctx, []string{"getoken:" + tc.user})
			}
			got, err := f.svc.SelectAccountWithLoadAwareness(ctx, nil, "s", affinityTestModel, nil, "", 0)
			require.NoError(t, err)
			require.Equal(t, tc.want, got.Account.ID)
			require.True(t, got.Acquired)
		})
	}
}

func TestAffinityWaitRejectedDedicatedFallsBackToShared(t *testing.T) {
	f := newAffinityScheduleFixture(map[string]int64{"s": 1})
	f.repo.accounts[0].Extra[AffinityWaitBeforeSharedExtraKey] = true
	ctx := WithAccountAffinityTags(context.Background(), []string{"getoken:1"})
	got, err := f.svc.SelectAccountWithLoadAwareness(ctx, nil, "s", affinityTestModel, map[int64]struct{}{1: {}}, "", 0)
	require.NoError(t, err)
	require.Equal(t, int64(2), got.Account.ID)
	require.True(t, got.Acquired)
}
