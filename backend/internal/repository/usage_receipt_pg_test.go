package repository

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/migrations"
	_ "github.com/lib/pq"
)

// Exercise the ownership predicate against PostgreSQL, not a mocked SQL string.
func TestUsageReceiptPostgresScopesRecordedUsage(t *testing.T) {
	dsn := os.Getenv("PG_TEST_DSN")
	if dsn == "" {
		t.Skip("PG_TEST_DSN is required for isolated PostgreSQL coverage")
	}
	parsed, parseErr := url.Parse(dsn)
	if parseErr != nil || (parsed.Path != "/plan_evidence_test" && parsed.Path != "/relay_status_test") {
		t.Fatal("use the owned throwaway database")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	if _, err = db.ExecContext(ctx, `CREATE SCHEMA receipt_scope_test`); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(ctx, `DROP SCHEMA receipt_scope_test CASCADE`)
	if _, err = db.ExecContext(ctx, `SET search_path TO receipt_scope_test`); err != nil {
		t.Fatal(err)
	}
	migration, err := migrations.FS.ReadFile("246_provider_usage_receipts.sql")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err = db.ExecContext(ctx, string(migration)); err != nil {
			t.Fatal(err)
		}
	}
	const id = "2c4e888b-836b-4992-9ff1-dc1699488902"
	repo := newUsageLogRepositoryWithSQL(nil, db)
	for _, entry := range []struct {
		key    int64
		output int
		model  string
	}{{11, 64000, "model-a"}, {22, 7, "model-b"}, {11, 1, "must-not-overwrite"}} {
		err = repo.SaveUsageReceipt(ctx, entry.key, &service.UsageReceipt{RequestID: id, Model: entry.model, OutputTokens: entry.output, RecordedAt: time.Now()})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		key    int64
		output int
		model  string
	}{{11, 64000, "model-a"}, {22, 7, "model-b"}} {
		got, err := repo.GetUsageReceipt(ctx, tc.key, id)
		if err != nil {
			t.Fatal(err)
		}
		if got.OutputTokens != tc.output || got.Model != tc.model {
			t.Fatalf("wrong owner usage: %+v", got)
		}
	}
	_, err = repo.GetUsageReceipt(ctx, 33, id)
	if !errors.Is(err, service.ErrUsageLogNotFound) {
		t.Fatalf("other key must see not-found: %v", err)
	}
	_, err = repo.GetUsageReceipt(ctx, 11, "36be0a49-fe70-4ab2-b497-4a0418c51ed5")
	if !errors.Is(err, service.ErrUsageLogNotFound) {
		t.Fatalf("not yet recorded must stay unknown: %v", err)
	}
	var count int
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM provider_usage_receipts`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("read-only query changed rows: %d %v", count, err)
	}
}
