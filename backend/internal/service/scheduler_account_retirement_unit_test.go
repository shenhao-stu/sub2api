//go:build unit

package service

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type retirementEventAccountRepo struct {
	*bulkEventAccountRepo
	account *Account
	err     error
}

func (r *retirementEventAccountRepo) GetByID(context.Context, int64) (*Account, error) {
	return r.account, r.err
}

func TestSchedulerAccountEventRetiresOnlyConfirmedMissing(t *testing.T) {
	for _, tc := range []struct {
		name    string
		account *Account
		err     error
		retire  bool
	}{
		{name: "deleted", err: ErrAccountNotFound, retire: true},
		{name: "transient database failure", err: errors.New("database temporarily unavailable")},
		{name: "existing account ignores deletion hint", account: &Account{ID: 42, Platform: PlatformOpenAI}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := newBulkEventSnapshotCache()
			repo := &retirementEventAccountRepo{bulkEventAccountRepo: newBulkEventAccountRepo(), account: tc.account, err: tc.err}
			svc := newBulkEventTestService(cache, repo)
			id := int64(42)
			err := svc.handleAccountEvent(context.Background(), &id, map[string]any{"deleted": true}, make(map[batchSeenKey]struct{}))
			if tc.err != nil && !tc.retire {
				require.ErrorIs(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
			set, retired := cache.accountWrites()
			if tc.retire {
				require.Equal(t, []int64{id}, retired)
			} else {
				require.Empty(t, retired)
			}
			if tc.account != nil {
				require.Equal(t, []int64{id}, set)
			} else {
				require.Empty(t, set)
			}
			require.Empty(t, cache.deleteAccountIDs)
		})
	}
}
