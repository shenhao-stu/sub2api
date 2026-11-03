package repository

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func contentionBillingPostgres(t *testing.T) *sql.DB {
	t.Helper()
	db := receiptJournalPostgres(t)
	db.SetMaxOpenConns(16)
	_, err := db.Exec(`
	CREATE TABLE users(id bigint PRIMARY KEY,balance numeric NOT NULL,updated_at timestamptz,deleted_at timestamptz);
	CREATE TABLE api_keys(id bigint PRIMARY KEY,user_id bigint);
	CREATE TABLE usage_billing_dedup(id bigserial PRIMARY KEY,request_id text,api_key_id bigint,request_fingerprint text,UNIQUE(request_id,api_key_id));
	CREATE TABLE usage_billing_dedup_archive(request_id text,api_key_id bigint,request_fingerprint text);
	INSERT INTO users VALUES(1,1000,NULL,NULL),(2,1000,NULL,NULL);
	INSERT INTO api_keys VALUES(11,1),(22,2);`)
	require.NoError(t, err)
	return db
}

func contentionCommand(userID int64, id string) *service.UsageBillingCommand {
	return &service.UsageBillingCommand{RequestID: id, APIKeyID: userID * 11, UserID: userID, BalanceCost: 1}
}

func TestUsageBillingPostgresHotUserDoesNotExhaustConnections(t *testing.T) {
	db := contentionBillingPostgres(t)
	repo := NewUsageBillingRepository(nil, db)
	blocker, err := db.Begin()
	require.NoError(t, err)
	defer blocker.Rollback()
	_, err = blocker.Exec(`UPDATE users SET balance=balance WHERE id=1`)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var wg sync.WaitGroup
	for n := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = repo.Apply(ctx, contentionCommand(1, fmt.Sprintf("blocked-%d", n)))
		}()
	}
	defer wg.Wait()
	defer cancel()
	// Wait until at least the first write has reached PostgreSQL. All other
	// requests for this user must wait without borrowing a database connection.
	require.Eventually(t, func() bool { return db.Stats().InUse >= 2 }, time.Second*5, time.Millisecond*10)
	time.Sleep(100 * time.Millisecond)
	otherCtx, otherCancel := context.WithTimeout(t.Context(), time.Second)
	defer otherCancel()
	result, err := repo.Apply(otherCtx, contentionCommand(2, "other-user"))
	require.NoError(t, err, "one user's blocked balance must not starve another user")
	require.True(t, result.Applied)
	require.LessOrEqual(t, db.Stats().InUse, 3)
	cancel()
	wg.Wait()
	require.NoError(t, blocker.Rollback())
	var canceledCharges int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM usage_billing_dedup WHERE api_key_id=11`).Scan(&canceledCharges))
	require.Zero(t, canceledCharges, "canceled waiters and the canceled transaction must leave no billing claim")
	var balance float64
	require.NoError(t, db.QueryRow(`SELECT balance FROM users WHERE id=1`).Scan(&balance))
	require.Equal(t, float64(1000), balance)
}

func TestUsageBillingPostgres500ConcurrentRequestsSettleOnce(t *testing.T) {
	db := contentionBillingPostgres(t)
	repo := NewUsageBillingRepository(nil, db)
	start := make(chan struct{})
	errCh := make(chan error, 500)
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	begin := time.Now()
	for n := range 500 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// Two deliveries of each exact billing identity remain one charge.
			_, err := repo.Apply(ctx, contentionCommand(1, fmt.Sprintf("request-%d", n/2)))
			errCh <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
	var balance float64
	var dedup int
	require.NoError(t, db.QueryRow(`SELECT balance FROM users WHERE id=1`).Scan(&balance))
	require.Equal(t, float64(750), balance)
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM usage_billing_dedup`).Scan(&dedup))
	require.Equal(t, 250, dedup)
	_, err := repo.Apply(ctx, &service.UsageBillingCommand{RequestID: "request-0", APIKeyID: 11, UserID: 1, BalanceCost: 2})
	require.ErrorIs(t, err, service.ErrUsageBillingRequestConflict)
	_, err = repo.Apply(ctx, &service.UsageBillingCommand{RequestID: "foreign-key", APIKeyID: 22, UserID: 1, BalanceCost: 1})
	require.ErrorIs(t, err, service.ErrAPIKeyNotFound)
	require.NoError(t, db.QueryRow(`SELECT balance FROM users WHERE id=1`).Scan(&balance))
	require.Equal(t, float64(750), balance)
	t.Logf("500 concurrent submissions / 250 immutable billings in %s", time.Since(begin))
}
