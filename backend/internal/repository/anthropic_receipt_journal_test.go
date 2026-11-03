package repository

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func anthropicReceiptFixture() *service.UsageReceipt {
	return &service.UsageReceipt{Object: "sub2api.usage_receipt", Version: 1, Source: "wire_final",
		RequestID: uuid.NewString(), Model: "claude-synthetic", InputTokens: 31, OutputTokens: 17,
		CacheReadTokens: 100, CacheCreationTokens: 20, CacheCreation1hTokens: 20,
		BillingMode: "token", InboundEndpoint: "/v1/messages", RecordedAt: time.Now().UTC().Truncate(time.Microsecond)}
}

func TestAnthropicReceiptJournalDoesNotChangeChatNamespace(t *testing.T) {
	parent, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	dir := filepath.Join(parent, "receipts")
	cfg := &config.Config{}
	cfg.Gateway.ChatUsageReceiptJournalDir = dir
	repo := ProvideUsageLogRepository(nil, nil, cfg).(*usageLogRepository)
	require.NoError(t, repo.chatJournalErr)
	require.NoError(t, repo.anthropicJournalErr)
	require.Equal(t, dir+".anthropic-v1", repo.anthropicJournal.dir)
	final := anthropicReceiptFixture()
	envelope, err := anthropicReceiptEnvelope(11, final)
	require.NoError(t, err)
	_, err = repo.anthropicJournal.persistAnthropicFinal(envelope)
	require.NoError(t, err)
	// An old Chat reader sees no new-protocol file in its persistent directory.
	oldChat, err := newChatReceiptJournal(dir)
	require.NoError(t, err)
	read, err := oldChat.load(11, final.RequestID)
	require.NoError(t, err)
	require.Nil(t, read)
	claim := *envelope.Claim
	claim.Endpoint = service.ChatReceiptEndpoint
	require.NoError(t, oldChat.reserve(&claim))
	recovered, err := repo.anthropicJournal.load(11, final.RequestID)
	require.NoError(t, err)
	require.Equal(t, final, recovered.Receipt)
}

func TestAnthropicReceiptRejectsInvalidFinalBeforePersistence(t *testing.T) {
	for _, mutate := range []func(*service.UsageReceipt){
		func(r *service.UsageReceipt) { r.OutputTokens = -1 },
		func(r *service.UsageReceipt) { r.CacheCreation1hTokens++ },
		func(r *service.UsageReceipt) { r.Source = "usage_log" },
		func(r *service.UsageReceipt) { r.InboundEndpoint = service.ChatReceiptEndpoint },
		func(r *service.UsageReceipt) { r.RecordedAt = time.Time{} },
		func(r *service.UsageReceipt) { r.RequestID = "caller-controlled" },
		func(r *service.UsageReceipt) { r.Model = "private\ncontent" },
	} {
		final := anthropicReceiptFixture()
		mutate(final)
		_, err := anthropicReceiptEnvelope(11, final)
		require.ErrorIs(t, err, service.ErrChatReceiptConflict)
	}
}

func TestAnthropicReceiptFinalSurvivesDatabaseOutage(t *testing.T) {
	j, _ := journalFixture(t)
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	repo := newUsageLogRepositoryWithSQL(nil, db)
	repo.anthropicJournal = j
	final := anthropicReceiptFixture()
	mock.ExpectExec("INSERT INTO provider_usage_receipts").WillReturnError(errors.New("synthetic database outage"))
	require.Error(t, repo.SaveUsageReceipt(context.Background(), 11, final))
	restarted, err := newChatReceiptJournal(j.dir)
	require.NoError(t, err)
	frozen, err := restarted.load(11, final.RequestID)
	require.NoError(t, err)
	require.NotNil(t, frozen, "an observed final must survive the failed INSERT and process restart")
	require.Equal(t, final, frozen.Receipt)
	require.NoError(t, mock.ExpectationsWereMet())
}
