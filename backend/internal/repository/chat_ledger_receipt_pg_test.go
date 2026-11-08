package repository

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestChatLedgerReceiptCommitsWithDebitOnce(t *testing.T) {
	db := contentionBillingPostgres(t)
	claims := newUsageLogRepositoryWithSQL(nil, db)
	billing := NewUsageBillingRepository(nil, db)
	ctx := t.Context()
	claim := &service.ChatUsageReceiptClaim{APIKeyID: 11, RequestID: uuid.NewString(), Token: uuid.NewString(),
		Model: "gemini-4-argon", Endpoint: service.ChatReceiptEndpoint, PayloadHash: service.HashUsageRequestPayload([]byte("{}"))}
	ok, err := claims.ClaimChatUsageReceipt(ctx, claim)
	require.NoError(t, err)
	require.True(t, ok)
	closeout := &service.UsageReceipt{
		Version: 2, Object: "sub2api.usage_receipt", Source: service.ChatLedgerReceiptSource,
		RequestID: claim.RequestID, Model: claim.Model, InboundEndpoint: claim.Endpoint, BillingMode: "token",
		InputTokens: 10, CacheReadTokens: 70, TerminalState: "failed", HTTPStatus: 502, RecordedAt: time.Now().UTC(),
	}
	cmd := service.UsageBillingCommand{RequestID: uuid.NewString(), APIKeyID: 11, UserID: 1, AccountID: 3,
		InputTokens: 10, CacheReadTokens: 70, BalanceCost: 0.5,
		ChatLedgerReceipt: &service.ChatLedgerReceipt{Claim: claim, Receipt: closeout}}
	forged := *claim
	forged.Token = uuid.NewString()
	bad := cmd
	bad.ChatLedgerReceipt = &service.ChatLedgerReceipt{Claim: &forged, Receipt: closeout}
	_, err = billing.Apply(ctx, &bad)
	require.ErrorIs(t, err, service.ErrChatReceiptConflict)
	_, err = claims.GetUsageReceipt(ctx, 11, claim.RequestID)
	require.ErrorIs(t, err, service.ErrUsageReceiptPending)
	var balance float64
	require.NoError(t, db.QueryRow(`SELECT balance FROM users WHERE id=1`).Scan(&balance))
	require.Equal(t, float64(1000), balance)
	var applied atomic.Int64
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			local := cmd
			result, err := billing.Apply(ctx, &local)
			if err != nil {
				t.Error(err)
			} else if result.Applied {
				applied.Add(1)
			}
		})
	}
	wg.Wait()
	require.Equal(t, int64(1), applied.Load())
	got, err := claims.GetUsageReceipt(ctx, 11, claim.RequestID)
	require.NoError(t, err)
	require.Equal(t, service.ChatLedgerReceiptSource, got.Source)
	require.Equal(t, 10, got.InputTokens)
	require.Equal(t, 70, got.CacheReadTokens)
	_, err = claims.GetUsageReceipt(ctx, 22, claim.RequestID)
	require.ErrorIs(t, err, service.ErrUsageLogNotFound)
	require.NoError(t, db.QueryRow(`SELECT balance FROM users WHERE id=1`).Scan(&balance))
	require.Equal(t, 999.5, balance)
	var dedup int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM usage_billing_dedup`).Scan(&dedup))
	require.Equal(t, 1, dedup)
}
