package config

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestChatReceiptJournalDirectoryConfiguration(t *testing.T) {
	resetViperWithJWTSecret(t)
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, "/app/data/usage-receipt-journal", cfg.Gateway.ChatUsageReceiptJournalDir)
	t.Setenv("GATEWAY_CHAT_USAGE_RECEIPT_JOURNAL_DIR", "/synthetic-persistent/receipts")
	cfg, err = Load()
	require.NoError(t, err)
	require.Equal(t, "/synthetic-persistent/receipts", cfg.Gateway.ChatUsageReceiptJournalDir)
}
