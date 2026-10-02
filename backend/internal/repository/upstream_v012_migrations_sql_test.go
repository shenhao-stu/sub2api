package repository

import (
	"context"
	"testing"

	"github.com/Wei-Shaw/sub2api/migrations"
	"github.com/stretchr/testify/require"
)

func TestV012AdditiveMigrationsSQL(t *testing.T) {
	repo := videoBillingSQLFixture(t)
	ctx := context.Background()
	_, err := repo.db.ExecContext(ctx, `
		CREATE TABLE payment_orders(id bigint PRIMARY KEY, amount numeric(20,2));
		CREATE TABLE user_platform_quotas(platform text CONSTRAINT user_platform_quotas_platform_check CHECK(platform IN ('grok','opencode_go')));
		CREATE TABLE composite_model_routes(target_platform text CONSTRAINT composite_model_routes_target_platform_check CHECK(target_platform IN ('grok','opencode_go')));
		INSERT INTO payment_orders VALUES (1,12.50);
		INSERT INTO user_platform_quotas VALUES ('grok'),('opencode_go');
		INSERT INTO composite_model_routes VALUES ('grok'),('opencode_go');`)
	require.NoError(t, err)
	for range 2 {
		tx, err := repo.db.BeginTx(ctx, nil)
		require.NoError(t, err)
		for _, name := range []string{"241_add_payment_order_bonus_amount.sql", "241_add_typesafe_platform.sql"} {
			sql, err := migrations.FS.ReadFile(name)
			require.NoError(t, err)
			_, err = tx.ExecContext(ctx, string(sql))
			require.NoError(t, err)
		}
		require.NoError(t, tx.Commit())
	}
	// The old binary omits bonus_amount on inserts and keeps serving old platforms.
	_, err = repo.db.ExecContext(ctx, `
		INSERT INTO payment_orders(id,amount) VALUES (2,20);
		INSERT INTO user_platform_quotas VALUES ('typesafe'),('grok');
		INSERT INTO composite_model_routes VALUES ('typesafe'),('opencode_go');`)
	require.NoError(t, err)
	var amount, bonus float64
	require.NoError(t, repo.db.QueryRowContext(ctx, `SELECT sum(amount), sum(bonus_amount) FROM payment_orders`).Scan(&amount, &bonus))
	require.Equal(t, 32.5, amount)
	require.Zero(t, bonus)
	_, err = repo.db.ExecContext(ctx, `INSERT INTO user_platform_quotas VALUES ('unrecognized')`)
	require.Error(t, err, "the migration must retain platform validation")
}
