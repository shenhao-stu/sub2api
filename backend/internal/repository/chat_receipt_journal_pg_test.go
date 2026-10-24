package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/migrations"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type receiptJournalFailOnceSQL struct {
	sqlExecutor
	fail        atomic.Bool
	afterCommit bool
}

func (s *receiptJournalFailOnceSQL) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	if strings.HasPrefix(q, "UPDATE provider_usage_receipts") && s.fail.CompareAndSwap(true, false) {
		if s.afterCommit {
			if _, err := s.sqlExecutor.ExecContext(ctx, q, args...); err != nil {
				return nil, err
			}
		}
		return nil, errors.New("synthetic unknown transaction outcome")
	}
	return s.sqlExecutor.ExecContext(ctx, q, args...)
}

func receiptJournalPostgres(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("PG_TEST_DSN")
	if dsn == "" {
		t.Skip("owned isolated PG_TEST_DSN required")
	}
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	require.Equal(t, "/plan_evidence_test", u.Path, "never run journal integration against a production database")
	require.Contains(t, []string{"127.0.0.1", "localhost"}, u.Hostname(), "test PG must be local to the isolated test environment")
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	schema := "receipt_journal_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = db.Exec("CREATE SCHEMA " + schema)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.Exec("DROP SCHEMA " + schema + " CASCADE"); _ = db.Close() })
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	scoped, err := sql.Open("postgres", u.String())
	require.NoError(t, err)
	scoped.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = scoped.Close() })
	migration, err := migrations.FS.ReadFile("246_provider_usage_receipts.sql")
	require.NoError(t, err)
	for range 2 {
		_, err = scoped.Exec(string(migration))
		require.NoError(t, err)
	}
	return scoped
}

func TestChatReceiptJournalPostgresRecoveryAndDeletion(t *testing.T) {
	for _, mode := range []string{"write_failure", "commit_ack_lost", "claim_deleted", "claim_replaced", "conflicting_final"} {
		t.Run(mode, func(t *testing.T) {
			db := receiptJournalPostgres(t)
			j, e := journalFixture(t)
			fault := &receiptJournalFailOnceSQL{sqlExecutor: db, afterCommit: mode == "commit_ack_lost"}
			fault.fail.Store(true)
			r := newUsageLogRepositoryWithSQL(nil, fault)
			r.chatJournal = j
			won, err := r.ClaimChatUsageReceipt(context.Background(), e.Claim)
			require.NoError(t, err)
			require.True(t, won)
			err = r.CompleteChatUsageReceipt(context.Background(), e.Claim, e.AccountID, e.BillingRequestID, e.Receipt)
			require.Error(t, err)
			frozen, err := j.load(e.Claim.APIKeyID, e.Claim.RequestID)
			require.NoError(t, err)
			require.NotNil(t, frozen)
			require.Equal(t, e, *frozen)
			switch mode {
			case "claim_deleted":
				_, err = db.Exec("DELETE FROM provider_usage_receipts")
				require.NoError(t, err)
			case "claim_replaced":
				changed := *e.Claim
				changed.Token = uuid.NewString()
				p, _ := json.Marshal(chatReceiptEnvelope{State: "pending", Claim: &changed})
				_, err = db.Exec("UPDATE provider_usage_receipts SET payload=$1", string(p))
				require.NoError(t, err)
			case "conflicting_final":
				changed := e
				changed.AccountID++
				p, _ := json.Marshal(changed)
				_, err = db.Exec("UPDATE provider_usage_receipts SET payload=$1", string(p))
				require.NoError(t, err)
			}
			// Every worker constructs a fresh repository; no captured process state
			// or current credentials can replace the original immutable binding.
			var successes atomic.Int32
			var wg sync.WaitGroup
			for range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					fresh := newUsageLogRepositoryWithSQL(nil, db)
					fresh.chatJournal = j
					got, err := fresh.GetUsageReceipt(context.Background(), e.Claim.APIKeyID, e.Claim.RequestID)
					if mode == "write_failure" || mode == "commit_ack_lost" {
						if err != nil || got == nil || got.OutputTokens != 7 || !got.RecordedAt.Equal(e.Receipt.RecordedAt) {
							t.Errorf("recovery did not preserve exact final: %v", err)
							return
						}
						successes.Add(1)
					} else if !errors.Is(err, service.ErrChatReceiptConflict) {
						t.Errorf("conflicting claim must not be recovered: %v", err)
					}
				}()
			}
			wg.Wait()
			var count int
			require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM provider_usage_receipts").Scan(&count))
			if mode == "write_failure" || mode == "commit_ack_lost" {
				require.Equal(t, int32(8), successes.Load())
				require.Equal(t, 1, count)
				remaining, err := j.load(e.Claim.APIKeyID, e.Claim.RequestID)
				require.NoError(t, err)
				require.Nil(t, remaining)
			} else {
				want := 1
				if mode == "claim_deleted" {
					want = 0
				}
				require.Equal(t, want, count)
				remaining, err := j.load(e.Claim.APIKeyID, e.Claim.RequestID)
				require.NoError(t, err)
				require.NotNil(t, remaining)
			}
		})
	}
}
