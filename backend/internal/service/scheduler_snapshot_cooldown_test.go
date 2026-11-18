//go:build unit

package service

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

type cooldownSnapshotCache struct {
	SchedulerCache
	members []*Account
}

func (c *cooldownSnapshotCache) GetSnapshot(context.Context, SchedulerBucket) ([]*Account, bool, error) {
	return c.members, c.members != nil, nil
}

func (*cooldownSnapshotCache) CaptureBucketWriteToken(_ context.Context, bucket SchedulerBucket) (SchedulerBucketWriteToken, error) {
	return SchedulerBucketWriteToken{Bucket: bucket, Epoch: 1}, nil
}

func (c *cooldownSnapshotCache) SetSnapshot(_ context.Context, _ SchedulerBucket, _ SchedulerBucketWriteToken, accounts []Account) error {
	c.members = make([]*Account, len(accounts))
	for i := range accounts {
		c.members[i] = &accounts[i]
	}
	return nil
}

type cooldownMembershipRepo struct {
	AccountRepository
	account Account
	reads   int
}

func (r *cooldownMembershipRepo) ListModelAvailabilityCandidates(_ context.Context, _ *int64, _ []string, _ bool) ([]Account, error) {
	r.reads++
	return []Account{r.account}, nil
}

func (*cooldownMembershipRepo) ListSchedulableByGroupIDAndPlatform(context.Context, int64, string) ([]Account, error) {
	return nil, nil // The database excludes accounts still in cooldown.
}

func (*cooldownMembershipRepo) ListSchedulableByGroupIDAndPlatforms(context.Context, int64, []string) ([]Account, error) {
	return nil, nil
}

func TestGroupedAnthropicSnapshotRecoversRelayWithoutRefresh(t *testing.T) {
	for _, tc := range []struct {
		name   string
		forced bool
	}{{"mixed", false}, {"forced", true}} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				groupID := int64(25)
				reset := time.Now().Add(time.Hour)
				cache := &cooldownSnapshotCache{}
				repo := &cooldownMembershipRepo{account: Account{
					ID: 33871, Platform: PlatformAnthropic, Status: StatusActive,
					Schedulable: true, TempUnschedulableUntil: &reset,
				}}
				svc := NewSchedulerSnapshotService(cache, nil, repo, nil, nil)
				check := func(want int) {
					t.Helper()
					selected, mixed, err := svc.ListSchedulableAccounts(ctx, &groupID, PlatformAnthropic, tc.forced)
					require.NoError(t, err)
					require.Equal(t, !tc.forced, mixed)
					require.Len(t, selected, want)
					if want == 1 {
						require.Equal(t, repo.account.ID, selected[0].ID)
					}
				}
				check(0)
				require.Len(t, cache.members, 1, "keep the relay in the group across cooldown")
				check(0)
				time.Sleep(time.Until(reset))
				check(1)
				require.Equal(t, 1, repo.reads)
				cache.members[0].Schedulable = false
				check(0)
			})
		})
	}
}

func TestGroupedOpenAISnapshotRetainsCoolingMember(t *testing.T) {
	ctx := context.Background()
	groupID := int64(11)
	reset := time.Now().Add(time.Hour)
	cache := &cooldownSnapshotCache{}
	repo := &cooldownMembershipRepo{account: Account{
		ID: 34034, Platform: PlatformOpenAI, Status: StatusActive,
		Schedulable: true, RateLimitResetAt: &reset,
	}}
	svc := NewSchedulerSnapshotService(cache, nil, repo, nil, nil)
	selected, _, err := svc.ListSchedulableAccounts(ctx, &groupID, PlatformOpenAI, false)
	require.NoError(t, err)
	require.Empty(t, selected, "a freshly loaded account must respect cooldown")
	require.Len(t, cache.members, 1, "cooldown must not erase group membership")
	require.Equal(t, int64(34034), cache.members[0].ID)

	selected, _, err = svc.ListSchedulableAccounts(ctx, &groupID, PlatformOpenAI, false)
	require.NoError(t, err)
	require.Empty(t, selected, "a cached account must respect cooldown")
	require.Equal(t, 1, repo.reads, "cache hit must not consult the database")

	eligible := derefAccounts(cache.members, reset)
	require.Len(t, eligible, 1, "the same immutable snapshot is eligible at the reset boundary")
	require.Equal(t, int64(34034), eligible[0].ID)
	require.Equal(t, reset, *cache.members[0].RateLimitResetAt)
}
