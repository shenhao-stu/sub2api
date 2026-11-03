package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

type anthropicReceiptFailOnceSQL struct {
	sqlExecutor
	fail atomic.Bool
	mode string
}

func (s *anthropicReceiptFailOnceSQL) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	if s.mode != "readback_failed" && strings.HasPrefix(q, "INSERT INTO provider_usage_receipts") && s.fail.CompareAndSwap(true, false) {
		if s.mode == "commit_ack_lost" {
			if _, err := s.sqlExecutor.ExecContext(ctx, q, args...); err != nil {
				return nil, err
			}
		}
		return nil, errors.New("synthetic unavailable transaction outcome")
	}
	return s.sqlExecutor.ExecContext(ctx, q, args...)
}

func (s *anthropicReceiptFailOnceSQL) QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	if s.mode == "readback_failed" && strings.HasPrefix(q, "SELECT (payload-") && s.fail.CompareAndSwap(true, false) {
		return nil, errors.New("synthetic readback failure")
	}
	return s.sqlExecutor.QueryContext(ctx, q, args...)
}

func TestAnthropicReceiptPostgresDurableRecovery(t *testing.T) {
	for _, mode := range []string{"write_failed", "commit_ack_lost", "readback_failed", "conflicting_final"} {
		t.Run(mode, func(t *testing.T) {
			db := receiptJournalPostgres(t)
			j, _ := journalFixture(t)
			final := anthropicReceiptFixture()
			fault := &anthropicReceiptFailOnceSQL{sqlExecutor: db, mode: mode}
			fault.fail.Store(true)
			repo := newUsageLogRepositoryWithSQL(nil, fault)
			repo.anthropicJournal = j
			require.Error(t, repo.SaveUsageReceipt(t.Context(), 11, final))
			if mode == "conflicting_final" {
				wrong := *final
				wrong.OutputTokens++
				payload, err := json.Marshal(wrong)
				require.NoError(t, err)
				_, err = db.Exec(`INSERT INTO provider_usage_receipts(api_key_id,request_id,payload,created_at) VALUES(11,$1,$2,$3)`, final.RequestID, string(payload), final.RecordedAt)
				require.NoError(t, err)
			}
			var wg sync.WaitGroup
			for range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					fresh := newUsageLogRepositoryWithSQL(nil, db)
					reopened, err := newChatReceiptJournal(j.dir)
					if err != nil {
						t.Errorf("reopen: %v", err)
						return
					}
					fresh.anthropicJournal = reopened
					got, err := fresh.GetUsageReceipt(t.Context(), 11, final.RequestID)
					if mode == "conflicting_final" {
						if !errors.Is(err, service.ErrChatReceiptConflict) || got != nil {
							t.Errorf("conflicting meter was accepted: %v", err)
						}
						return
					}
					if err != nil || got == nil || *got != *final {
						t.Errorf("final evidence changed or lost: %v", err)
					}
				}()
			}
			wg.Wait()
			frozen, err := j.load(11, final.RequestID)
			require.NoError(t, err)
			if mode == "conflicting_final" {
				require.NotNil(t, frozen)
				require.Equal(t, final, frozen.Receipt)
			} else {
				require.Nil(t, frozen, "only confirmed durable PG evidence permits journal cleanup")
			}
			var count int
			require.NoError(t, db.QueryRow(`SELECT count(*) FROM provider_usage_receipts`).Scan(&count))
			require.Equal(t, 1, count)
			fresh := newUsageLogRepositoryWithSQL(nil, db)
			fresh.anthropicJournal = j
			_, err = fresh.GetUsageReceipt(t.Context(), 22, final.RequestID)
			require.ErrorIs(t, err, service.ErrUsageLogNotFound)
		})
	}
}

func TestAnthropicReceiptPostgresRepeatedPersistenceKeepsFirstFinal(t *testing.T) {
	db := receiptJournalPostgres(t)
	j, _ := journalFixture(t)
	repo := newUsageLogRepositoryWithSQL(nil, db)
	repo.anthropicJournal = j
	final := anthropicReceiptFixture()
	require.NoError(t, repo.SaveUsageReceipt(t.Context(), 11, final))
	var wg sync.WaitGroup
	for n := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			repeated := *final
			repeated.RecordedAt = repeated.RecordedAt.Add(time.Duration(n+1) * time.Microsecond)
			if err := repo.SaveUsageReceipt(t.Context(), 11, &repeated); err != nil {
				t.Errorf("repeat persistence: %v", err)
			}
		}()
	}
	wg.Wait()
	got, err := repo.GetUsageReceipt(t.Context(), 11, final.RequestID)
	require.NoError(t, err)
	require.Equal(t, final, got)
	var count int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM provider_usage_receipts`).Scan(&count))
	require.Equal(t, 1, count)
}

func TestAnthropicReceiptPostgresDiskFailureKeepsHealthyDatabase(t *testing.T) {
	db := receiptJournalPostgres(t)
	j, _ := journalFixture(t)
	j.limit = 0
	repo := newUsageLogRepositoryWithSQL(nil, db)
	repo.anthropicJournal = j
	final := anthropicReceiptFixture()
	require.NoError(t, repo.SaveUsageReceipt(t.Context(), 11, final))
	got, err := repo.GetUsageReceipt(t.Context(), 11, final.RequestID)
	require.NoError(t, err)
	require.Equal(t, final, got)
}
