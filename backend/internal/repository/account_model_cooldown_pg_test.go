package repository

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"entgo.io/ent/dialect/sql/schema"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/ent/migrate"
	_ "github.com/Wei-Shaw/sub2api/ent/runtime"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/migrations"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Only the external cache boundary is recorded. Updates, JSONB row locking,
// outbox deduplication and snapshot reads run against an isolated PostgreSQL.
type modelCooldownPGCache struct {
	service.SchedulerCache
	mu       sync.Mutex
	accounts []*service.Account
}

func (c *modelCooldownPGCache) SetAccount(_ context.Context, account *service.Account) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.accounts = append(c.accounts, account)
	return nil
}

func (c *modelCooldownPGCache) snapshots() []*service.Account {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*service.Account(nil), c.accounts...)
}

func modelCooldownPostgres(t *testing.T) (*accountRepository, *sql.DB, *modelCooldownPGCache) {
	t.Helper()
	dsn := os.Getenv("PG_TEST_DSN")
	if dsn == "" {
		t.Skip("PG_TEST_DSN for an owned isolated PostgreSQL is required")
	}
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	require.Contains(t, []string{"postgres", "postgresql"}, u.Scheme)
	require.Equal(t, "/relay_status_test", u.Path, "never run these tests against a production database")
	require.Contains(t, []string{"127.0.0.1", "localhost"}, u.Hostname(), "use the isolated local endpoint or its owned SSH tunnel")
	require.Empty(t, u.Fragment)
	for key := range u.Query() {
		require.Contains(t, []string{"sslmode", "connect_timeout", "application_name"}, key, "no connection or schema override is allowed")
	}
	base, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	base.SetMaxOpenConns(2)
	t.Cleanup(func() { _ = base.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var database string
	require.NoError(t, base.QueryRowContext(ctx, "SELECT current_database()").Scan(&database))
	require.Equal(t, "relay_status_test", database)
	name := "model_cooldown_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = base.ExecContext(ctx, "CREATE SCHEMA "+name)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		_, cleanupErr := base.ExecContext(cleanupCtx, "DROP SCHEMA "+name+" CASCADE")
		require.NoError(t, cleanupErr)
	})
	q := u.Query()
	q.Set("search_path", name)
	q.Set("connect_timeout", "5")
	u.RawQuery = q.Encode()
	db, err := sql.Open("postgres", u.String())
	require.NoError(t, err)
	db.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = db.Close() })
	driver := entsql.OpenDB(dialect.Postgres, db)
	client := dbent.NewClient(dbent.Driver(driver))
	tables := []*schema.Table{migrate.ProxiesTable, migrate.AccountsTable, migrate.GroupsTable, migrate.AccountGroupsTable}
	require.NoError(t, migrate.Create(ctx, migrate.NewSchema(driver), tables, migrate.WithForeignKeys(false)))
	// Run the actual outbox migrations, including the partial unique index, on
	// this new schema only. Reapplication also covers their idempotence.
	for range 2 {
		for _, file := range []string{"036_scheduler_outbox.sql", "152_scheduler_outbox_dedup_key.sql", "153_scheduler_outbox_pending_dedup_key_index_notx.sql"} {
			ddl, readErr := migrations.FS.ReadFile(file)
			require.NoError(t, readErr)
			_, execErr := db.ExecContext(ctx, string(ddl))
			require.NoError(t, execErr)
		}
	}
	cache := &modelCooldownPGCache{}
	return newAccountRepositoryWithSQL(client, db, cache), db, cache
}

func modelCooldownPGAccount(t *testing.T, repo *accountRepository) int64 {
	t.Helper()
	a, err := repo.client.Account.Create().
		SetName("isolated-model-cooldown").SetPlatform(service.PlatformOpenAI).
		SetType(service.AccountTypeAPIKey).SetCredentials(map[string]any{}).
		SetExtra(map[string]any{"unrelated": "keep", "nested": map[string]any{"keep": true}}).
		Save(context.Background())
	require.NoError(t, err)
	return a.ID
}

func modelCooldownPGExtra(t *testing.T, repo *accountRepository, id int64) map[string]any {
	t.Helper()
	a, err := repo.GetByID(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, "keep", a.Extra["unrelated"])
	require.Equal(t, map[string]any{"keep": true}, a.Extra["nested"])
	return a.Extra
}

func modelCooldownPGEntry(t *testing.T, extra map[string]any, model string) map[string]any {
	t.Helper()
	limits, ok := extra["model_rate_limits"].(map[string]any)
	require.True(t, ok)
	entry, ok := limits[model].(map[string]any)
	require.True(t, ok)
	return entry
}

func modelCooldownPGConcurrent(t *testing.T, count int, run func(context.Context, int) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := make(chan struct{})
	errors := make(chan error, count)
	var wg sync.WaitGroup
	for i := range count {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			errors <- run(ctx, index)
		}(i)
	}
	close(start)
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
}

func TestAccountModelCooldownPostgresConcurrentMax(t *testing.T) {
	repo, db, cache := modelCooldownPostgres(t)
	id := modelCooldownPGAccount(t, repo)
	const model = "gemini-4-argon"
	const workers = 24
	base := time.Now().UTC().Truncate(time.Second).Add(time.Hour)
	modelCooldownPGConcurrent(t, workers, func(ctx context.Context, i int) error {
		return repo.SetModelRateLimitIfLater(ctx, id, model, base.Add(time.Duration(i)*time.Minute), fmt.Sprintf("request-%d", i))
	})
	before := modelCooldownPGEntry(t, modelCooldownPGExtra(t, repo, id), model)
	require.Equal(t, base.Add((workers-1)*time.Minute).Format(time.RFC3339), before["rate_limit_reset_at"])
	require.Equal(t, "request-23", before["reason"])
	// A guaranteed late response from an older request must preserve the full
	// winning entry, including the reason and original limited-at timestamp.
	require.NoError(t, repo.SetModelRateLimitIfLater(context.Background(), id, model, base, "stale"))
	after := modelCooldownPGExtra(t, repo, id)
	require.Equal(t, before, modelCooldownPGEntry(t, after, model))
	snapshots := cache.snapshots()
	require.Len(t, snapshots, workers+1, "every successful update refreshes the local snapshot")
	require.Equal(t, after, snapshots[len(snapshots)-1].Extra)
	require.False(t, snapshots[len(snapshots)-1].IsSchedulableForModel(model))
	require.True(t, snapshots[len(snapshots)-1].IsSchedulableForModel("unrelated-model"))
	var events int
	require.NoError(t, db.QueryRow("SELECT count(*) FROM scheduler_outbox WHERE event_type=$1 AND account_id=$2", service.SchedulerOutboxEventAccountChanged, id).Scan(&events))
	require.Equal(t, 1, events, "pending notifications deduplicate without losing the committed latest state")
}

func TestAccountModelCooldownPostgresConcurrentModelsAndLegacyReplacement(t *testing.T) {
	repo, db, cache := modelCooldownPostgres(t)
	id := modelCooldownPGAccount(t, repo)
	models := []string{"gemini-4-argon", "gemini-3.8-flash", "model.with.dot", "model/quote,'[]"}
	base := time.Now().UTC().Truncate(time.Second).Add(time.Hour)
	const rounds = 8
	modelCooldownPGConcurrent(t, len(models)*rounds, func(ctx context.Context, i int) error {
		model, round := models[i%len(models)], i/len(models)
		return repo.SetModelRateLimitIfLater(ctx, id, model, base.Add(time.Duration(round)*time.Minute), fmt.Sprintf("round-%d", round))
	})
	extra := modelCooldownPGExtra(t, repo, id)
	for _, model := range models {
		entry := modelCooldownPGEntry(t, extra, model)
		require.Equal(t, base.Add((rounds-1)*time.Minute).Format(time.RFC3339), entry["rate_limit_reset_at"])
		require.Equal(t, "round-7", entry["reason"])
	}
	// The older general-purpose setter deliberately permits an admin/refresh
	// replacement. Only the new IfLater operation is monotonic.
	shorter := base.Add(-30 * time.Minute)
	require.NoError(t, repo.SetModelRateLimit(context.Background(), id, models[0], shorter, " replacement "))
	after := modelCooldownPGExtra(t, repo, id)
	require.Equal(t, shorter.Format(time.RFC3339), modelCooldownPGEntry(t, after, models[0])["rate_limit_reset_at"])
	require.Equal(t, "replacement", modelCooldownPGEntry(t, after, models[0])["reason"])
	for _, model := range models[1:] {
		require.Equal(t, modelCooldownPGEntry(t, extra, model), modelCooldownPGEntry(t, after, model))
	}
	snapshots := cache.snapshots()
	require.Len(t, snapshots, len(models)*rounds+1)
	require.Equal(t, after, snapshots[len(snapshots)-1].Extra)
	// Historical malformed metadata must remain repairable through the old
	// unconditional setter rather than being rejected by the new comparison.
	_, err := db.Exec("UPDATE accounts SET extra=jsonb_set(extra, ARRAY['model_rate_limits',$1,'rate_limit_reset_at'], to_jsonb('legacy-invalid-reset'::text)) WHERE id=$2", models[0], id)
	require.NoError(t, err)
	require.NoError(t, repo.SetModelRateLimit(context.Background(), id, models[0], shorter, "repaired"))
	repaired := modelCooldownPGEntry(t, modelCooldownPGExtra(t, repo, id), models[0])
	require.Equal(t, shorter.Format(time.RFC3339), repaired["rate_limit_reset_at"])
	require.Equal(t, "repaired", repaired["reason"])
}

func TestAccountModelCooldownPostgresOutboxAndNoopBoundaries(t *testing.T) {
	repo, db, cache := modelCooldownPostgres(t)
	id := modelCooldownPGAccount(t, repo)
	ctx := context.Background()
	reset := time.Now().UTC().Truncate(time.Second).Add(time.Hour)
	require.NoError(t, repo.SetModelRateLimitIfLater(ctx, id, "gemini-4-argon", reset))
	outbox := NewSchedulerOutboxRepository(db)
	first, err := outbox.ListAfterAndReleaseDedup(ctx, 0, 10)
	require.NoError(t, err)
	require.Len(t, first, 1)
	require.Equal(t, service.SchedulerOutboxEventAccountChanged, first[0].EventType)
	require.NotNil(t, first[0].AccountID)
	require.Equal(t, id, *first[0].AccountID)
	require.NoError(t, repo.SetModelRateLimitIfLater(ctx, id, "gemini-4-argon", reset.Add(time.Hour)))
	second, err := outbox.ListAfterAndReleaseDedup(ctx, first[0].ID, 10)
	require.NoError(t, err)
	require.Len(t, second, 1, "consumed dedup keys must permit a fresh scheduling notification")
	require.Greater(t, second[0].ID, first[0].ID)
	require.Equal(t, id, *second[0].AccountID)
	require.Len(t, cache.snapshots(), 2)

	require.NoError(t, repo.SetModelRateLimitIfLater(ctx, id, "", reset))
	require.ErrorIs(t, repo.SetModelRateLimitIfLater(ctx, id+1000, "gemini-4-argon", reset), service.ErrAccountNotFound)
	_, err = db.ExecContext(ctx, "UPDATE accounts SET deleted_at=NOW() WHERE id=$1", id)
	require.NoError(t, err)
	require.ErrorIs(t, repo.SetModelRateLimitIfLater(ctx, id, "gemini-4-argon", reset), service.ErrAccountNotFound)
	require.Len(t, cache.snapshots(), 2, "empty scopes and absent/deleted rows must not announce updates")
	remaining, err := outbox.ListAfterAndReleaseDedup(ctx, second[0].ID, 10)
	require.NoError(t, err)
	require.Empty(t, remaining)
}
