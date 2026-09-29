package repository

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestR256UserUsageTrendMetricSQL(t *testing.T) {
	fixture := videoBillingSQLFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := fixture.db.ExecContext(ctx, `
		ALTER TABLE users ADD COLUMN email text, ADD COLUMN username text;
		CREATE TABLE usage_logs (
			user_id bigint, created_at timestamptz, input_tokens bigint,
			output_tokens bigint, cache_creation_tokens bigint, cache_read_tokens bigint,
			total_cost numeric(20,8), actual_cost numeric(20,8)
		);
		UPDATE users SET email = 'fixture@example.test', username = 'fixture';
		INSERT INTO usage_logs VALUES
			(1, '2025-01-15T12:00:00Z', 800, 100, 50, 50, 0.25, 0.1),
			(2, '2025-01-15T12:00:00Z', 10, 0, 0, 0, 8, 5),
			(2, '2025-01-15T10:00:00Z', 999999, 0, 0, 0, 999, 999),
			(1, '2025-01-15T13:00:00Z', 999999, 0, 0, 0, 999, 999);
	`)
	require.NoError(t, err)
	repo := &usageLogRepository{sql: fixture.db}
	at := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		metric           string
		userID, tokens   int64
		cost, actualCost float64
	}{
		{"tokens", 1, 1000, 0.25, 0.1},
		{"actual_cost", 2, 10, 8, 5},
		{"unsupported", 1, 1000, 0.25, 0.1},
	} {
		t.Run(tc.metric, func(t *testing.T) {
			rows, err := repo.GetUserUsageTrend(ctx, at.Add(-time.Hour), at.Add(time.Hour), "day", 1, tc.metric)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.Equal(t, tc.userID, rows[0].UserID)
			require.Equal(t, tc.tokens, rows[0].Tokens)
			require.EqualValues(t, 1, rows[0].Requests)
			require.Equal(t, tc.cost, rows[0].Cost)
			require.Equal(t, tc.actualCost, rows[0].ActualCost)
		})
	}
}
