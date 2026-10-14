//go:build integration

package repository

import (
	"context"
	"fmt"
	"testing"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

type retirementCommitRecorder struct {
	schedulerCacheRecorder
	checkCommitted func(context.Context, int64)
}

func (r *retirementCommitRecorder) RetireAccount(ctx context.Context, id int64) error {
	if r.checkCommitted != nil {
		r.checkCommitted(ctx, id)
	}
	return r.schedulerCacheRecorder.RetireAccount(ctx, id)
}

func retirementFixture(t *testing.T) (*service.Account, *service.Group) {
	t.Helper()
	client := testEntClient(t)
	account := mustCreateAccount(t, client, &service.Account{Name: "retirement-" + t.Name()})
	group := mustCreateGroup(t, client, &service.Group{Name: "retirement-" + t.Name()})
	mustBindAccountToGroup(t, client, account.ID, group.ID, 1)
	t.Cleanup(func() {
		for _, statement := range []string{
			"DELETE FROM scheduler_outbox WHERE account_id = $1",
			"DELETE FROM account_groups WHERE account_id = $1",
			"DELETE FROM accounts WHERE id = $1",
		} {
			_, err := integrationDB.ExecContext(context.Background(), statement, account.ID)
			require.NoError(t, err)
		}
		_, err := integrationDB.ExecContext(context.Background(), "DELETE FROM groups WHERE id = $1", group.ID)
		require.NoError(t, err)
	})
	return account, group
}

func assertRetirementRows(t *testing.T, executor sqlExecutor, id int64, active bool, bindings, deletedEvents int) {
	t.Helper()
	var observedActive bool
	var observedBindings, observedEvents int
	query := `SELECT deleted_at IS NULL,
		(SELECT count(*) FROM account_groups WHERE account_id = $1),
		(SELECT count(*) FROM scheduler_outbox WHERE account_id = $1 AND payload->>'deleted' = 'true')
		FROM accounts WHERE id = $1`
	require.NoError(t, scanSingleRow(context.Background(), executor, query, []any{id},
		&observedActive, &observedBindings, &observedEvents))
	require.Equal(t, active, observedActive)
	require.Equal(t, bindings, observedBindings)
	require.Equal(t, deletedEvents, observedEvents)
}

func TestAccountRetirementOwnedCommitAndDistinctDeletionEvent(t *testing.T) {
	ctx := context.Background()
	account, group := retirementFixture(t)
	require.NoError(t, enqueueSchedulerOutbox(ctx, integrationDB,
		service.SchedulerOutboxEventAccountChanged, &account.ID, nil,
		map[string]any{"group_ids": []int64{group.ID}}))
	cache := &retirementCommitRecorder{checkCommitted: func(ctx context.Context, id int64) {
		require.NoError(t, ctx.Err())
		_, bounded := ctx.Deadline()
		require.True(t, bounded)
		assertRetirementRows(t, integrationDB, id, false, 0, 1)
	}}
	repo := newAccountRepositoryWithSQL(testEntClient(t), integrationDB, cache)
	require.NoError(t, repo.Delete(ctx, account.ID))
	require.Equal(t, []int64{account.ID}, cache.retireIDs)
	require.Empty(t, cache.deleteIDs)
	var allEvents int
	require.NoError(t, integrationDB.QueryRowContext(ctx,
		"SELECT count(*) FROM scheduler_outbox WHERE account_id = $1", account.ID).Scan(&allEvents))
	require.Equal(t, 2, allEvents, "deleted event must survive dedup against the live update")
}

func TestAccountRetirementExternalTransactions(t *testing.T) {
	for _, tc := range []struct {
		name      string
		contextTx bool
		commit    bool
		abort     bool
	}{
		{name: "context_rollback", contextTx: true},
		{name: "context_commit", contextTx: true, commit: true},
		{name: "context_failed_commit", contextTx: true, commit: true, abort: true},
		{name: "bare_client_rollback"},
		{name: "bare_client_commit", commit: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			account, _ := retirementFixture(t)
			tx := testEntTx(t)
			cache := &retirementCommitRecorder{checkCommitted: func(ctx context.Context, id int64) {
				require.NoError(t, ctx.Err())
				assertRetirementRows(t, integrationDB, id, false, 0, 1)
			}}
			repo := newAccountRepositoryWithSQL(tx.Client(), tx, cache)
			if tc.contextTx {
				repo = newAccountRepositoryWithSQL(testEntClient(t), integrationDB, cache)
				ctx = dbent.NewTxContext(ctx, tx)
			}
			require.NoError(t, repo.Delete(ctx, account.ID))
			require.Empty(t, cache.retireIDs, "pending transaction cannot create a permanent fence")
			assertRetirementRows(t, tx, account.ID, false, 0, 1)
			assertRetirementRows(t, integrationDB, account.ID, true, 1, 0)
			if tc.abort {
				_, err := tx.ExecContext(ctx, "SELECT 1 / 0")
				require.Error(t, err)
				require.Error(t, tx.Commit())
			} else if tc.commit {
				require.NoError(t, tx.Commit())
			} else {
				require.NoError(t, tx.Rollback())
			}
			if tc.commit && !tc.abort {
				assertRetirementRows(t, integrationDB, account.ID, false, 0, 1)
				if tc.contextTx {
					require.Equal(t, []int64{account.ID}, cache.retireIDs)
				} else {
					require.Empty(t, cache.retireIDs, "bare transaction clients rely on committed outbox")
				}
			} else {
				assertRetirementRows(t, integrationDB, account.ID, true, 1, 0)
				require.Empty(t, cache.retireIDs)
			}
			require.Empty(t, cache.deleteIDs)
		})
	}
}

func TestAccountRetirementOutboxFailureRollsBackDeletion(t *testing.T) {
	ctx := context.Background()
	account, _ := retirementFixture(t)
	constraint := fmt.Sprintf("retirement_test_reject_%d", account.ID)
	_, err := integrationDB.ExecContext(ctx, fmt.Sprintf(
		"ALTER TABLE scheduler_outbox ADD CONSTRAINT %s CHECK (account_id IS DISTINCT FROM %d)", constraint, account.ID))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := integrationDB.ExecContext(context.Background(), "ALTER TABLE scheduler_outbox DROP CONSTRAINT "+constraint)
		require.NoError(t, err)
	})
	cache := &schedulerCacheRecorder{}
	repo := newAccountRepositoryWithSQL(testEntClient(t), integrationDB, cache)
	require.Error(t, repo.Delete(ctx, account.ID), "real database must reject the event insert")
	assertRetirementRows(t, integrationDB, account.ID, true, 1, 0)
	require.Empty(t, cache.retireIDs)
	require.Empty(t, cache.deleteIDs)
}

func TestAccountRetirementMissingAccountDoesNotFenceOrEnqueue(t *testing.T) {
	ctx := context.Background()
	var absentID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, "SELECT coalesce(max(id), 0) + 1000 FROM accounts").Scan(&absentID))
	cache := &schedulerCacheRecorder{}
	repo := newAccountRepositoryWithSQL(testEntClient(t), integrationDB, cache)
	require.ErrorIs(t, repo.Delete(ctx, absentID), service.ErrAccountNotFound)
	var events int
	require.NoError(t, integrationDB.QueryRowContext(ctx,
		"SELECT count(*) FROM scheduler_outbox WHERE account_id = $1", absentID).Scan(&events))
	require.Zero(t, events)
	require.Empty(t, cache.retireIDs)
	require.Empty(t, cache.deleteIDs)
}
