//go:build integration

package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/migrations"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// The only new SQL file is additive to the migration ledger. The old migration
// runner and files must still accept the upgraded schema without a DB restore.
func TestV0218MigrationUpgradeAndOldRunnerCompatibility(t *testing.T) {
	ctx := context.Background()
	const name = "246_enable_gemini_image_generation_groups.sql"
	legacy := fstest.MapFS{}
	files, err := fs.Glob(migrations.FS, "*.sql")
	require.NoError(t, err)
	for _, file := range files {
		if file == name {
			continue
		}
		data, err := migrations.FS.ReadFile(file)
		require.NoError(t, err)
		legacy[file] = &fstest.MapFile{Data: data}
	}
	// Reconstruct the prior migration ledger in this disposable test database.
	_, err = integrationDB.ExecContext(ctx, "DELETE FROM schema_migrations WHERE filename=$1", name)
	require.NoError(t, err)
	require.NoError(t, applyMigrationsFS(ctx, integrationDB, legacy))

	platforms := []string{service.PlatformGemini, service.PlatformAntigravity, service.PlatformComposite, service.PlatformOpenAI, service.PlatformAnthropic}
	ids := make([]int64, len(platforms))
	for i, platform := range platforms {
		group := mustCreateGroup(t, testEntClient(t), &service.Group{Name: "upgrade-" + uuid.NewString(), Platform: platform})
		ids[i] = group.ID
		_, err := integrationDB.ExecContext(ctx, "UPDATE groups SET allow_image_generation=false WHERE id=$1", group.ID)
		require.NoError(t, err)
	}
	for range 2 {
		require.NoError(t, ApplyMigrations(ctx, integrationDB))
	}
	for i, id := range ids {
		var enabled bool
		require.NoError(t, integrationDB.QueryRowContext(ctx, "SELECT allow_image_generation FROM groups WHERE id=$1", id).Scan(&enabled))
		require.Equal(t, i < 2, enabled, platforms[i])
	}
	data, err := migrations.FS.ReadFile(name)
	require.NoError(t, err)
	digest := sha256.Sum256([]byte(strings.TrimSpace(string(data))))
	var checksum string
	require.NoError(t, integrationDB.QueryRowContext(ctx, "SELECT checksum FROM schema_migrations WHERE filename=$1", name).Scan(&checksum))
	require.Equal(t, hex.EncodeToString(digest[:]), checksum)
	// A deliberately disabled group stays disabled on restart: migration does not
	// run again. Deployment can restore an explicit pre-upgrade choice before traffic.
	_, err = integrationDB.ExecContext(ctx, "UPDATE groups SET allow_image_generation=false WHERE id=$1", ids[0])
	require.NoError(t, err)
	require.NoError(t, ApplyMigrations(ctx, integrationDB))
	require.NoError(t, applyMigrationsFS(ctx, integrationDB, legacy))
	var enabled bool
	require.NoError(t, integrationDB.QueryRowContext(ctx, "SELECT allow_image_generation FROM groups WHERE id=$1", ids[0]).Scan(&enabled))
	require.False(t, enabled)
}

func TestV0218ConcurrentBillingPreservesDedupAndQuotaIncrements(t *testing.T) {
	ctx := context.Background()
	client := testEntClient(t)
	user := mustCreateUser(t, client, &service.User{Email: "concurrent-" + uuid.NewString() + "@example.test", PasswordHash: "test-only", Balance: 100})
	key := mustCreateApiKey(t, client, &service.APIKey{UserID: user.ID, Key: "test-" + uuid.NewString(), Name: "concurrent", Quota: 100})
	account := mustCreateAccount(t, client, &service.Account{
		Name: "vertex-concurrent-" + uuid.NewString(), Platform: service.PlatformGemini, Type: service.AccountTypeServiceAccount,
		Extra: map[string]any{"quota_limit": 100.0, "quota_daily_limit": 100.0, "quota_weekly_limit": 100.0},
	})
	repo := NewUsageBillingRepository(client, integrationDB)
	const workers = 12
	sharedID := uuid.NewString()
	for _, unique := range []bool{false, true} {
		results := make([]*service.UsageBillingApplyResult, workers)
		errors := make([]error, workers)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range workers {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				requestID := sharedID
				if unique {
					requestID = uuid.NewString()
				}
				<-start
				results[i], errors[i] = repo.Apply(ctx, &service.UsageBillingCommand{
					RequestID: requestID, APIKeyID: key.ID, UserID: user.ID, AccountID: account.ID,
					AccountType: service.AccountTypeServiceAccount, BalanceCost: 1, APIKeyQuotaCost: 1,
					AccountQuotaCost: 1, AccountQuotaHasDailyLimit: true, AccountQuotaHasWeeklyLimit: true,
				})
			}(i)
		}
		close(start)
		wg.Wait()
		applied := 0
		for i := range workers {
			require.NoError(t, errors[i])
			require.NotNil(t, results[i])
			if results[i].Applied {
				applied++
			}
		}
		wantApplied, total := 1, 1.0
		if unique {
			wantApplied, total = workers, workers+1
		}
		require.Equal(t, wantApplied, applied)
		var balance, keyUsed, quota, daily, weekly float64
		require.NoError(t, integrationDB.QueryRowContext(ctx, "SELECT balance FROM users WHERE id=$1", user.ID).Scan(&balance))
		require.NoError(t, integrationDB.QueryRowContext(ctx, "SELECT quota_used FROM api_keys WHERE id=$1", key.ID).Scan(&keyUsed))
		require.NoError(t, integrationDB.QueryRowContext(ctx, "SELECT (extra->>'quota_used')::numeric,(extra->>'quota_daily_used')::numeric,(extra->>'quota_weekly_used')::numeric FROM accounts WHERE id=$1", account.ID).Scan(&quota, &daily, &weekly))
		require.Equal(t, 100-total, balance)
		for _, value := range []float64{keyUsed, quota, daily, weekly} {
			require.Equal(t, total, value)
		}
		var dedup int
		require.NoError(t, integrationDB.QueryRowContext(ctx, "SELECT count(*) FROM usage_billing_dedup WHERE api_key_id=$1", key.ID).Scan(&dedup))
		require.Equal(t, int(total), dedup)
	}
}
