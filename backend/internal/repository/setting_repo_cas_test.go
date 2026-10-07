package repository

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/Wei-Shaw/sub2api/ent"
	_ "github.com/Wei-Shaw/sub2api/ent/runtime"
	"github.com/stretchr/testify/require"
)

func TestGrokCLIVersionCompareAndSetSQL(t *testing.T) {
	db := videoBillingSQLFixture(t).db
	_, err := db.Exec(`CREATE TABLE settings (id bigserial PRIMARY KEY, key varchar(100) UNIQUE NOT NULL, value text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now())`)
	require.NoError(t, err)
	client := ent.NewClient(ent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	r, ok := NewSettingRepository(client).(*settingRepository)
	require.True(t, ok)
	ctx := context.Background()
	changed, err := r.CompareAndSet(ctx, "grok_cli_cas", "", "1.0.50")
	require.NoError(t, err)
	require.True(t, changed)
	changed, err = r.CompareAndSet(ctx, "grok_cli_cas", "", "1.0.49")
	require.NoError(t, err)
	require.False(t, changed)
	var applied atomic.Int32
	var wg sync.WaitGroup
	for _, version := range []string{"1.0.51", "1.0.52"} {
		wg.Add(1)
		go func(v string) {
			defer wg.Done()
			changed, err := r.CompareAndSet(ctx, "grok_cli_cas", "1.0.50", v)
			require.NoError(t, err)
			if changed {
				applied.Add(1)
			}
		}(version)
	}
	wg.Wait()
	require.EqualValues(t, 1, applied.Load())
	got, err := r.GetValue(ctx, "grok_cli_cas")
	require.NoError(t, err)
	require.Contains(t, []string{"1.0.51", "1.0.52"}, got)
	changed, err = r.CompareAndSet(ctx, "grok_cli_cas", "1.0.50", "1.0.49")
	require.NoError(t, err)
	require.False(t, changed)
	retained, err := r.GetValue(ctx, "grok_cli_cas")
	require.NoError(t, err)
	require.Equal(t, got, retained)
}
