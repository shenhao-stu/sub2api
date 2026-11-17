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

func TestSharedStickyRelayAtCapacity(t *testing.T) {
	for _, routed := range []bool{false, true} {
		for _, tc := range []struct {
			name                                         string
			tag                                          string
			configured, relayFree, peerFree, excludePeer bool
			want                                         int64
			wait                                         bool
		}{
			{"free peer", "getoken:1", true, false, true, false, 4, false},
			{"all busy", "getoken:1", true, false, false, false, 3, true},
			{"peer excluded", "getoken:1", true, false, true, true, 3, true},
			{"healthy binding", "getoken:1", true, true, true, false, 3, false},
			{"default queue", "getoken:1", false, false, true, false, 3, true},
			{"other tenant", "getoken:2", true, false, true, false, 3, true},
		} {
			route := "plain/"
			if routed {
				route = "model-route/"
			}
			t.Run(route+tc.name, func(t *testing.T) {
				f := dedicatedFallbackChain()
				f.svc.cfg.Gateway.Scheduling.StickySessionMaxWaiting = 3
				f.svc.cfg.Gateway.Scheduling.StickySessionWaitTimeout = 2 * time.Minute
				ctx := WithAccountAffinityTags(context.Background(), []string{tc.tag})
				excluded := map[int64]struct{}{1: {}, 2: {}}
				if tc.excludePeer {
					excluded[4] = struct{}{}
				}
				relay := f.repo.accountsByID[3]
				relay.Concurrency = 10
				if tc.configured {
					relay.Extra["busy_sticky_fallback_tags"] = []any{"getoken:1"}
				}
				var gid *int64
				if routed {
					id := int64(25)
					gid = &id
					f.svc.groupRepo = &mockGroupRepoForGateway{groups: map[int64]*Group{
						id: {ID: id, Platform: PlatformAnthropic, Status: StatusActive, Hydrated: true, ModelRoutingEnabled: true,
							ModelRouting: map[string][]int64{affinityTestModel: {3, 4}}},
					}}
				}
				f.cache.sessionBindings["turn"] = 3
				f.concurrencyCache.acquireResults = map[int64]bool{3: tc.relayFree, 4: tc.peerFree}
				f.concurrencyCache.loadMap = map[int64]*AccountLoadInfo{3: {AccountID: 3, LoadRate: 100}, 4: {AccountID: 4}}
				got, err := f.svc.SelectAccountWithLoadAwareness(ctx, gid, "turn", affinityTestModel, excluded, "", 0)
				require.NoError(t, err)
				require.Equal(t, tc.want, got.Account.ID)
				if tc.wait {
					require.False(t, got.Acquired)
					require.NotNil(t, got.WaitPlan)
					require.Equal(t, 2*time.Minute, got.WaitPlan.Timeout)
					require.Equal(t, 3, got.WaitPlan.MaxWaiting)
				} else {
					require.True(t, got.Acquired)
					got.ReleaseFunc()
				}
			})
		}
	}
}
