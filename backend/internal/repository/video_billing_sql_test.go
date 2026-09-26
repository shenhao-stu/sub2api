package repository

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/migrations"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func videoBillingSQLFixture(t *testing.T) *usageBillingRepository {
	t.Helper()
	dsn := os.Getenv("PG_TEST_DSN")
	if dsn == "" {
		t.Skip("isolated PostgreSQL requires PG_TEST_DSN")
	}
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	name := strings.TrimPrefix(u.Path, "/")
	require.True(t, (u.Scheme == "postgres" || u.Scheme == "postgresql") && (name == "r251_test" || strings.HasSuffix(name, "_test")), "only an explicitly named test database is allowed")
	admin, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	schema := fmt.Sprintf("r251_video_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(ctx, "CREATE SCHEMA "+pq.QuoteIdentifier(schema))
	require.NoError(t, err)
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	db, err := sql.Open("postgres", u.String())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
		_, dropErr := admin.ExecContext(context.Background(), "DROP SCHEMA "+pq.QuoteIdentifier(schema)+" CASCADE")
		require.NoError(t, dropErr)
		_ = admin.Close()
	})
	_, err = db.ExecContext(ctx, `
	CREATE TABLE users(id bigint PRIMARY KEY,balance numeric(20,8) NOT NULL,updated_at timestamptz,deleted_at timestamptz);
	CREATE TABLE api_keys(id bigint PRIMARY KEY,user_id bigint NOT NULL,status text DEFAULT 'active',quota numeric(20,8) DEFAULT 10,quota_used numeric(20,8) DEFAULT 0,
	 usage_5h numeric(20,8) DEFAULT 0,usage_1d numeric(20,8) DEFAULT 0,usage_7d numeric(20,8) DEFAULT 0,
	 window_5h_start timestamptz,window_1d_start timestamptz,window_7d_start timestamptz,updated_at timestamptz,deleted_at timestamptz);
	CREATE TABLE usage_billing_dedup(id bigserial PRIMARY KEY,request_id text,api_key_id bigint,request_fingerprint text,created_at timestamptz DEFAULT NOW(),UNIQUE(request_id,api_key_id));
	CREATE TABLE usage_billing_dedup_archive(request_id text,api_key_id bigint,request_fingerprint text);
	INSERT INTO users(id,balance) VALUES(1,100),(2,100);
	INSERT INTO api_keys(id,user_id,deleted_at) VALUES(10,1,NOW());`)
	require.NoError(t, err)
	migration, err := migrations.FS.ReadFile("241_durable_video_billing.sql")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, string(migration))
	require.NoError(t, err)
	return &usageBillingRepository{db: db}
}

func TestR251DeletedKeySettlementSQL(t *testing.T) {
	for _, mode := range []string{"quota", "rate", "both", "wrong owner"} {
		t.Run(mode, func(t *testing.T) {
			r := videoBillingSQLFixture(t)
			ctx := context.Background()
			cmd := &service.UsageBillingCommand{RequestID: "authorized-text", APIKeyID: 10, UserID: 1, AccountID: 20, AccountType: "oauth", Model: "fixture", InputTokens: 10, BalanceCost: 2}
			if mode != "rate" {
				cmd.APIKeyQuotaCost = 2
			}
			if mode != "quota" {
				cmd.APIKeyRateLimitCost = 2
			}
			if mode == "wrong owner" {
				cmd.UserID = 2
			}
			result, err := r.Apply(ctx, cmd)
			var balance, quota, rate float64
			if mode == "wrong owner" {
				require.ErrorIs(t, err, service.ErrAPIKeyNotFound)
				require.NoError(t, r.db.QueryRow(`SELECT balance FROM users WHERE id=2`).Scan(&balance))
				require.Equal(t, 100.0, balance)
				var count int
				require.NoError(t, r.db.QueryRow(`SELECT COUNT(*) FROM usage_billing_dedup`).Scan(&count))
				require.Zero(t, count)
			} else {
				require.NoError(t, err)
				require.True(t, result.Applied)
				result, err = r.Apply(ctx, cmd)
				require.NoError(t, err)
				require.False(t, result.Applied)
				require.NoError(t, r.db.QueryRow(`SELECT balance FROM users WHERE id=1`).Scan(&balance))
				require.Equal(t, 98.0, balance)
			}
			require.NoError(t, r.db.QueryRow(`SELECT quota_used,usage_5h FROM api_keys WHERE id=10`).Scan(&quota, &rate))
			if mode == "wrong owner" {
				require.Zero(t, quota)
				require.Zero(t, rate)
			} else {
				require.Equal(t, cmd.APIKeyQuotaCost, quota)
				require.Equal(t, cmd.APIKeyRateLimitCost, rate)
			}
		})
	}
}

func TestR251DurableVideoBillingSQL(t *testing.T) {
	r := videoBillingSQLFixture(t)
	ctx := context.Background()
	pending := service.GrokVideoPendingBilling{Owner: &service.VideoBillingOwner{UserID: 1, APIKeyID: 10, AccountID: 20}, Model: "grok-imagine-video", VideoResolution: "720p", VideoDurationSeconds: 12, CreatedAt: time.Now().Add(-48 * time.Hour).Format(time.RFC3339Nano)}
	require.NoError(t, r.StoreVideoBilling(ctx, "task", pending))
	job, err := r.GetVideoBilling(ctx, "task", 1, 10)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Nil(t, job.Snapshot.Owner.GroupID)
	require.Equal(t, "720p", job.Snapshot.VideoResolution)
	require.Equal(t, 12, job.Snapshot.VideoDurationSeconds)
	other, err := r.GetVideoBilling(ctx, "task", 2, 10)
	require.NoError(t, err)
	require.Nil(t, other)
	_, err = r.db.Exec(`UPDATE pending_video_billing SET next_check_at=NOW()-INTERVAL '1 second'`)
	require.NoError(t, err)
	jobs, err := r.ClaimVideoBillingDue(ctx, 1)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	jobs, err = r.ClaimVideoBillingDue(ctx, 1)
	require.NoError(t, err)
	require.Empty(t, jobs, "leased task must not be claimed concurrently")
	_, err = r.db.Exec(`UPDATE users SET deleted_at=NOW() WHERE id=1`)
	require.NoError(t, err)
	cmd := &service.UsageBillingCommand{RequestID: "grok-video:task", APIKeyID: 10, UserID: 1, AccountID: 20, AccountType: "oauth", Model: pending.Model, BalanceCost: 3, APIKeyQuotaCost: 3, APIKeyRateLimitCost: 3, HistoricalVideo: true}
	result, err := r.Apply(ctx, cmd)
	require.NoError(t, err)
	require.True(t, result.Applied)
	result, err = r.Apply(ctx, cmd)
	require.NoError(t, err)
	require.False(t, result.Applied)
	job, err = r.GetVideoBilling(ctx, "task", 1, 10)
	require.NoError(t, err)
	require.Equal(t, "settled", job.State, "charge and state must commit together")
	changed := *cmd
	changed.BalanceCost = 9
	changed.Model = "changed-price-model"
	changed.RequestFingerprint = ""
	result, err = r.Apply(ctx, &changed)
	require.NoError(t, err)
	require.False(t, result.Applied, "settled replay cannot be repriced")
	require.NoError(t, r.UpdateVideoBilling(ctx, "task", 10, "pending", time.Now(), "stale_worker"))
	job, err = r.GetVideoBilling(ctx, "task", 1, 10)
	require.NoError(t, err)
	require.Equal(t, "settled", job.State)
	var balance, quota float64
	require.NoError(t, r.db.QueryRow(`SELECT balance FROM users WHERE id=1`).Scan(&balance))
	require.Equal(t, 97.0, balance)
	require.NoError(t, r.db.QueryRow(`SELECT quota_used FROM api_keys WHERE id=10`).Scan(&quota))
	require.Equal(t, 3.0, quota)
	bad := *cmd
	bad.RequestID = "grok-video:missing-receipt"
	bad.RequestFingerprint = ""
	_, err = r.Apply(ctx, &bad)
	require.Error(t, err)
	var count int
	require.NoError(t, r.db.QueryRow(`SELECT COUNT(*) FROM usage_billing_dedup`).Scan(&count))
	require.Equal(t, 1, count)

	require.NoError(t, r.StoreVideoBilling(ctx, "concurrent", pending))
	type outcome struct {
		applied bool
		amount  float64
		err     error
	}
	outcomes := make(chan outcome, 2)
	for _, amount := range []float64{1, 2} {
		go func(amount float64) {
			concurrent := *cmd
			concurrent.RequestID = "grok-video:concurrent"
			concurrent.BalanceCost = amount
			concurrent.RequestFingerprint = ""
			result, err := r.Apply(ctx, &concurrent)
			outcomes <- outcome{applied: result != nil && result.Applied, amount: amount, err: err}
		}(amount)
	}
	charged, appliedCount := 0.0, 0
	for range 2 {
		result := <-outcomes
		require.NoError(t, result.err)
		if result.applied {
			appliedCount++
			charged = result.amount
		}
	}
	require.Equal(t, 1, appliedCount, "worker and client must settle one original task once")
	require.NoError(t, r.db.QueryRow(`SELECT balance FROM users WHERE id=1`).Scan(&balance))
	require.Equal(t, 97-charged, balance)
}

func TestR251VideoBillingRollbackSQL(t *testing.T) {
	r := videoBillingSQLFixture(t)
	ctx := context.Background()
	p := service.GrokVideoPendingBilling{Owner: &service.VideoBillingOwner{UserID: 1, APIKeyID: 10, AccountID: 20}, Model: "grok-imagine-video", VideoResolution: "720p", VideoDurationSeconds: 12}
	require.NoError(t, r.StoreVideoBilling(ctx, "rollback-task", p))
	_, err := r.db.Exec(`DELETE FROM users WHERE id=1`)
	require.NoError(t, err)
	cmd := &service.UsageBillingCommand{RequestID: "grok-video:rollback-task", APIKeyID: 10, UserID: 1, AccountID: 20, AccountType: "oauth", Model: p.Model, BalanceCost: 3, APIKeyQuotaCost: 3, HistoricalVideo: true}
	_, err = r.Apply(ctx, cmd)
	require.ErrorIs(t, err, service.ErrUserNotFound)
	job, err := r.GetVideoBilling(ctx, "rollback-task", 1, 10)
	require.NoError(t, err)
	require.Equal(t, "pending", job.State)
	var count int
	require.NoError(t, r.db.QueryRow(`SELECT COUNT(*) FROM usage_billing_dedup`).Scan(&count))
	require.Zero(t, count)
	var quota float64
	require.NoError(t, r.db.QueryRow(`SELECT quota_used FROM api_keys WHERE id=10`).Scan(&quota))
	require.Zero(t, quota)
}
