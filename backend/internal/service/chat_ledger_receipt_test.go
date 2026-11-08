package service

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/stretchr/testify/require"
)

func TestChatLedgerReceiptFromTerminalPartialCPA(t *testing.T) {
	for _, tc := range []struct {
		name, payload string
		valid         bool
	}{
		{"terminal partial", `{"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":80,"input_tokens_details":{"cached_tokens":70}}}}`, true},
		{"primer", `{"type":"response.in_progress","response":{"status":"in_progress","usage":{"input_tokens":80}}}`, false},
		{"missing", `{"type":"response.completed","response":{"status":"completed"}}`, false},
		{"explicit zero is wire evidence", `{"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":80,"output_tokens":0}}}`, false},
		{"ambiguous", `{"type":"response.completed","usage":{"input_tokens":81},"response":{"status":"completed","usage":{"input_tokens":80}}}`, false},
		{"unaccounted output", `{"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":80,"total_tokens":81}}}`, false},
		{"unaccounted reasoning", `{"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":80,"output_tokens_details":{"reasoning_tokens":1}}}}`, false},
		{"impossible cache", `{"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":80,"input_tokens_details":{"cached_tokens":81}}}}`, false},
		{"malformed", `{"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":80,"output_tokens":null}}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var event apicompat.ResponsesStreamEvent
			require.NoError(t, json.Unmarshal([]byte(tc.payload), &event))
			require.Equal(t, tc.valid, event.PositivePartialReceiptInput())
		})
	}
	receipt := &UsageReceipt{
		Version: 2, Object: "sub2api.usage_receipt", Source: ChatLedgerReceiptSource,
		RequestID: "84bc58bf-80ce-4a37-9328-fc5bd0445b0d", Model: "gemini-4-argon",
		InboundEndpoint: ChatReceiptEndpoint, BillingMode: "token", RecordedAt: time.Now(),
		InputTokens: 10, CacheReadTokens: 70, TerminalState: "failed", HTTPStatus: http.StatusBadGateway,
	}
	require.True(t, ValidateChatUsageReceipt(receipt))
	receipt.InputTokens, receipt.CacheReadTokens = 0, 0
	require.False(t, ValidateChatUsageReceipt(receipt), "no booked usage must stay unknown")
	receipt.InputTokens, receipt.Model = 10, "gpt-6.1-sol"
	require.False(t, ValidateChatUsageReceipt(receipt))
}
