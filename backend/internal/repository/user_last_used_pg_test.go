package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type lastUsedQueryCapture struct {
	*sql.DB
	query string
	args  []any
}

func (d *lastUsedQueryCapture) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	d.query, d.args = query, args
	return d.DB.QueryContext(ctx, query, args...)
}

func TestUserLatestUsageUsesBoundedIndexLookups(t *testing.T) {
	db := receiptJournalPostgres(t)
	_, err := db.Exec(`CREATE TABLE usage_logs(user_id bigint NOT NULL,created_at timestamptz NOT NULL);
 CREATE INDEX usage_user_created ON usage_logs(user_id,created_at);
 INSERT INTO usage_logs SELECT 1, timestamptz '2026-01-01 00:00:00Z'+n*interval '1 second' FROM generate_series(1,100000) n;
 INSERT INTO usage_logs VALUES(2,'2026-02-01 00:00:00+09'),(2,'2026-01-01 00:00:00Z');
 ANALYZE usage_logs;`)
	require.NoError(t, err)
	capture := &lastUsedQueryCapture{DB: db}
	repo := &userRepository{sql: capture}
	result, err := repo.GetLatestUsedAtByUserIDs(t.Context(), []int64{1, 2, 1, 3})
	require.NoError(t, err)
	require.Len(t, result, 2)
	require.True(t, result[1].Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Add(100000*time.Second)))
	require.True(t, result[2].Equal(time.Date(2026, 1, 31, 15, 0, 0, 0, time.UTC)))
	require.Equal(t, time.UTC, result[2].Location())
	// Inspect the actual repository statement, not a duplicated query fixture.
	var raw []byte
	require.NoError(t, db.QueryRowContext(t.Context(), "EXPLAIN (ANALYZE,FORMAT JSON) "+capture.query, capture.args...).Scan(&raw))
	var plans []map[string]any
	require.NoError(t, json.Unmarshal(raw, &plans))
	scans, limits := 0, 0
	var visit func(map[string]any)
	visit = func(plan map[string]any) {
		if plan["Relation Name"] == "usage_logs" {
			scans++
			require.Contains(t, []any{"Index Scan", "Index Only Scan"}, plan["Node Type"])
			require.Equal(t, "Backward", plan["Scan Direction"])
			require.LessOrEqual(t, plan["Actual Rows"].(float64), float64(1))
			require.LessOrEqual(t, plan["Actual Loops"].(float64), float64(3))
		}
		if plan["Node Type"] == "Limit" {
			limits++
		}
		if children, ok := plan["Plans"].([]any); ok {
			for _, child := range children {
				visit(child.(map[string]any))
			}
		}
	}
	visit(plans[0]["Plan"].(map[string]any))
	require.Equal(t, 1, scans)
	require.Equal(t, 1, limits)
	t.Logf("actual plan: %s", raw)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = repo.GetLatestUsedAtByUserIDs(canceled, []int64{1})
	require.ErrorIs(t, err, context.Canceled)
	empty, err := (&userRepository{}).GetLatestUsedAtByUserIDs(t.Context(), nil)
	require.NoError(t, err)
	require.Empty(t, empty)
}
