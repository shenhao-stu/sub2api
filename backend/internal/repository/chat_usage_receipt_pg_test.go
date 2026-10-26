package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/migrations"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

func TestChatReceiptPostgresClaimAndImmutableFinal(t *testing.T) {
	dsn := os.Getenv("PG_TEST_DSN")
	if dsn == "" {
		t.Skip("isolated PG_TEST_DSN required")
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Path != "/relay_status_test" && u.Path != "/plan_evidence_test") {
		t.Fatal("owned throwaway database required")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	schema := "chat_receipt_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = db.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatal(err)
	}
	defer db.Exec(`DROP SCHEMA ` + schema + ` CASCADE`)
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	scoped, err := sql.Open("postgres", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer scoped.Close()
	scoped.SetMaxOpenConns(8)
	migration, err := migrations.FS.ReadFile("246_provider_usage_receipts.sql")
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err = scoped.Exec(string(migration)); err != nil {
			t.Fatal(err)
		}
	}
	repo := newUsageLogRepositoryWithSQL(nil, scoped)
	capability, ok := any(repo).(service.ChatUsageReceiptRepository)
	if !ok {
		t.Fatal("baseline repository has no durable Chat receipt claim capability")
	}
	ctx := context.Background()
	claim := &service.ChatUsageReceiptClaim{APIKeyID: 11, RequestID: uuid.NewString(), Token: uuid.NewString(), Model: "gemini-synthetic", Endpoint: service.ChatReceiptEndpoint, PayloadHash: strings.Repeat("a", 64)}
	var won atomic.Int32
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, e := capability.ClaimChatUsageReceipt(ctx, claim)
			if e != nil {
				t.Error(e)
			}
			if ok {
				won.Add(1)
			}
		}()
	}
	wg.Wait()
	if won.Load() != 1 {
		t.Fatalf("duplicate generation admission: winners=%d", won.Load())
	}
	if _, err = repo.GetUsageReceipt(ctx, 11, claim.RequestID); !errors.Is(err, service.ErrUsageReceiptPending) {
		t.Fatalf("pending must be distinguished from missing: %v", err)
	}
	forged := *claim
	forged.Token = uuid.NewString()
	receipt := &service.UsageReceipt{Version: 2, Object: "sub2api.usage_receipt", Source: "wire_final", RequestID: claim.RequestID, Model: claim.Model, InboundEndpoint: claim.Endpoint, InputTokens: 100, OutputTokens: 64000, CacheReadTokens: 900, ReasoningTokens: 1000, BillingMode: "token", TerminalState: "completed", HTTPStatus: 200, RecordedAt: time.Now().UTC().Truncate(time.Microsecond)}
	if err = capability.CompleteChatUsageReceipt(ctx, &forged, 3, "client:synthetic-billing-id", receipt); !errors.Is(err, service.ErrChatReceiptConflict) {
		t.Fatalf("foreign token finalized: %v", err)
	}
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e := capability.CompleteChatUsageReceipt(ctx, claim, 3, "client:synthetic-billing-id", receipt); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	// A new repository models process restart: immutable evidence remains readable.
	restarted := newUsageLogRepositoryWithSQL(nil, scoped)
	got, err := restarted.GetUsageReceipt(ctx, 11, claim.RequestID)
	if err != nil || got.OutputTokens != 64000 || got.Version != 2 {
		t.Fatalf("restart lost final receipt: %+v %v", got, err)
	}
	for _, key := range []int64{12, 0} {
		if _, e := restarted.GetUsageReceipt(ctx, key, claim.RequestID); !errors.Is(e, service.ErrUsageLogNotFound) {
			t.Fatalf("cross-key read: %d %v", key, e)
		}
	}
	changed := *receipt
	changed.OutputTokens = 1
	if e := capability.CompleteChatUsageReceipt(ctx, claim, 3, "client:synthetic-billing-id", &changed); !errors.Is(e, service.ErrChatReceiptConflict) {
		t.Fatalf("meter overwrite: %v", e)
	}
	if e := capability.CompleteChatUsageReceipt(ctx, claim, 4, "client:synthetic-billing-id", receipt); !errors.Is(e, service.ErrChatReceiptConflict) {
		t.Fatalf("account overwrite: %v", e)
	}
	changed = *receipt
	changed.Model = "other"
	if e := capability.CompleteChatUsageReceipt(ctx, claim, 3, "client:synthetic-billing-id", &changed); !errors.Is(e, service.ErrChatReceiptConflict) {
		t.Fatalf("model overwrite: %v", e)
	}
	other := *claim
	other.APIKeyID = 12
	other.Token = uuid.NewString()
	ok, err = capability.ClaimChatUsageReceipt(ctx, &other)
	if err != nil || !ok {
		t.Fatalf("other key namespace: %v %v", ok, err)
	}
	if ok, e := capability.ClaimChatUsageReceipt(ctx, claim); e != nil || ok {
		t.Fatalf("final ID can replay: %v %v", ok, e)
	}
	var count int
	if e := scoped.QueryRow(`SELECT COUNT(*) FROM provider_usage_receipts`).Scan(&count); e != nil || count != 2 {
		t.Fatalf("row count %d %v", count, e)
	}
	t.Log("16 concurrent claims admitted exactly 1; 8 identical finalizers idempotent; pending unknown; restart/key/model/account/meter isolation verified")
}

func TestChatReceiptEnvelopeLegacyReaderCannotInventZero(t *testing.T) {
	claim := &service.ChatUsageReceiptClaim{APIKeyID: 11, RequestID: uuid.NewString(), Token: uuid.NewString(), Model: "synthetic", Endpoint: service.ChatReceiptEndpoint, PayloadHash: strings.Repeat("a", 64)}
	for _, envelope := range []chatReceiptEnvelope{{State: "pending", Claim: claim}, {State: "final", Claim: claim, AccountID: 3, BillingRequestID: "client:synthetic", Receipt: &service.UsageReceipt{Version: 2, RequestID: claim.RequestID, Model: claim.Model, OutputTokens: 7, RecordedAt: time.Now()}}} {
		payload, err := json.Marshal(envelope)
		if err != nil {
			t.Fatal(err)
		}
		var legacy service.UsageReceipt
		if err = json.Unmarshal(payload, &legacy); err != nil {
			t.Fatal(err)
		}
		if !legacy.RecordedAt.IsZero() {
			t.Fatal("legacy reader could accept an envelope as a zero receipt")
		}
	}
}
