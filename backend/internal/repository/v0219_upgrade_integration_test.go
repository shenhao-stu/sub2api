//go:build integration

package repository

import (
	"context"
	"io/fs"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/migrations"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestV0219UpgradePreservesAccountsAndBillingDefaults(t *testing.T) {
	ctx := context.Background()
	client := testEntClient(t)
	user := mustCreateUser(t, client, &service.User{Email: uuid.NewString() + "@example.test", PasswordHash: "fixture", Balance: 17.25})
	key := mustCreateApiKey(t, client, &service.APIKey{UserID: user.ID, Key: uuid.NewString(), Name: "upgrade", Quota: 50})
	group := mustCreateGroup(t, client, &service.Group{Name: "upgrade-" + uuid.NewString(), Platform: service.PlatformOpenAI, RateMultiplier: 0.75})
	legacy := fstest.MapFS{}
	files, err := fs.Glob(migrations.FS, "*.sql")
	require.NoError(t, err)
	for _, name := range files {
		if name == "247_scoped_admin_api_keys.sql" || name == "248_group_image_count_billing.sql" {
			continue
		}
		data, err := migrations.FS.ReadFile(name)
		require.NoError(t, err)
		legacy[name] = &fstest.MapFile{Data: data}
	}
	// Reconstruct g30 in the disposable integration database, including real rows.
	t.Cleanup(func() { require.NoError(t, ApplyMigrations(ctx, integrationDB)) })
	_, err = integrationDB.ExecContext(ctx, `DROP TABLE scoped_admin_api_keys;
		ALTER TABLE groups DROP COLUMN image_count_billing;
		DELETE FROM schema_migrations WHERE filename IN ('247_scoped_admin_api_keys.sql','248_group_image_count_billing.sql')`)
	require.NoError(t, err)
	require.NoError(t, applyMigrationsFS(ctx, integrationDB, legacy))
	snapshot := func() string {
		var result string
		require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT jsonb_build_object(
			'user', (SELECT to_jsonb(u) FROM users u WHERE id=$1),
			'key', (SELECT to_jsonb(k) FROM api_keys k WHERE id=$2),
			'group', (SELECT to_jsonb(g)-'image_count_billing' FROM groups g WHERE id=$3))::text`, user.ID, key.ID, group.ID).Scan(&result))
		return result
	}
	before := snapshot()
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func(i int) { defer wg.Done(); errs[i] = ApplyMigrations(ctx, integrationDB) }(i)
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, before, snapshot(), "upgrade must not change balances, quotas, keys or group pricing")
	var enabled bool
	require.NoError(t, integrationDB.QueryRowContext(ctx, "SELECT image_count_billing FROM groups WHERE id=$1", group.ID).Scan(&enabled))
	require.False(t, enabled)
	// Old code may continue and roll back against the additive schema.
	require.NoError(t, applyMigrationsFS(ctx, integrationDB, legacy))
	_, err = integrationDB.ExecContext(ctx, "UPDATE groups SET rate_multiplier=0.75 WHERE id=$1", group.ID)
	require.NoError(t, err)
	require.Equal(t, before, snapshot())
	_, err = integrationDB.ExecContext(ctx, "UPDATE groups SET image_count_billing=true WHERE id=$1", group.ID)
	require.NoError(t, err)
	require.NoError(t, ApplyMigrations(ctx, integrationDB))
	require.NoError(t, integrationDB.QueryRowContext(ctx, "SELECT image_count_billing FROM groups WHERE id=$1", group.ID).Scan(&enabled))
	require.True(t, enabled, "restart must retain an explicit administrator choice")
}
