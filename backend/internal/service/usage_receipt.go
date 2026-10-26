package service

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/tidwall/gjson"
)

// UsageReceipt exposes the recorded token quantities of one authenticated key.
// It deliberately contains no account identity, credentials, prompts, or prices.
// A missing record is unknown; it must never be converted into a zero-usage bill.
type UsageReceipt struct {
	ReasoningTokens       int       `json:"reasoning_tokens"`
	TerminalState         string    `json:"terminal_state,omitempty"`
	HTTPStatus            int       `json:"http_status,omitempty"`
	WebSearchCalls        int       `json:"web_search_calls"`
	Object                string    `json:"object"`
	Version               int       `json:"version"`
	Source                string    `json:"source"`
	RequestID             string    `json:"request_id"`
	Model                 string    `json:"model"`
	InputTokens           int       `json:"input_tokens"`
	OutputTokens          int       `json:"output_tokens"`
	CacheReadTokens       int       `json:"cache_read_tokens"`
	CacheCreationTokens   int       `json:"cache_creation_tokens"`
	CacheCreation5mTokens int       `json:"cache_creation_5m_tokens"`
	CacheCreation1hTokens int       `json:"cache_creation_1h_tokens"`
	ServiceTier           string    `json:"service_tier"`
	BillingMode           string    `json:"billing_mode"`
	InboundEndpoint       string    `json:"inbound_endpoint"`
	RecordedAt            time.Time `json:"recorded_at"`
}

// Optional repository capability keeps unrelated usage readers unchanged.
type UsageReceiptReader interface {
	GetUsageReceipt(context.Context, int64, string) (*UsageReceipt, error)
}

var (
	ErrUsageReceiptUnavailable = errors.New("usage receipt unavailable")
	// ErrUsageReceiptPending is distinct from a missing receipt. A pending
	// claim reserves an identity but carries no authoritative meter yet, so it
	// must remain unknown and must never be represented as a zero receipt.
	ErrUsageReceiptPending = errors.New("usage receipt pending")
)

// PendingUsageReceipt is the safe wire representation for a durable claim
// whose final meter has not arrived. It intentionally contains no account,
// credential, prompt, or price fields.
type PendingUsageReceipt struct {
	Object    string `json:"object"`
	Version   int    `json:"version"`
	RequestID string `json:"request_id"`
	State     string `json:"state"`
}

func ValidUsageReceiptID(id string) bool {
	u, err := uuid.Parse(id)
	return err == nil && u != uuid.Nil && id == u.String()
}

func (s *UsageService) GetAPIKeyUsageReceipt(ctx context.Context, apiKeyID int64, id string) (*UsageReceipt, error) {
	if apiKeyID <= 0 || !ValidUsageReceiptID(id) {
		return nil, ErrUsageLogNotFound
	}
	reader, ok := s.usageRepo.(UsageReceiptReader)
	if !ok {
		return nil, ErrUsageReceiptUnavailable
	}
	receipt, err := reader.GetUsageReceipt(ctx, apiKeyID, id)
	if err != nil {
		return nil, err
	}
	if receipt == nil || receipt.WebSearchCalls < 0 || receipt.InputTokens < 0 || receipt.OutputTokens < 0 ||
		receipt.CacheReadTokens < 0 || receipt.CacheCreationTokens < 0 ||
		receipt.CacheCreation5mTokens < 0 || receipt.CacheCreation1hTokens < 0 || receipt.RecordedAt.IsZero() {
		return nil, ErrUsageReceiptUnavailable
	}
	if receipt.Version != 2 {
		receipt.Version = 1
	}
	if receipt.Version == 2 && (!ValidateChatUsageReceipt(receipt) || receipt.RequestID != id) {
		return nil, ErrUsageReceiptUnavailable
	}
	receipt.Object, receipt.Source = "sub2api.usage_receipt", "wire_final"
	receipt.RequestID = id
	return receipt, nil
}

// Receipt evidence freezes the final meter on the downstream wire, before
// RecordUsage applies any additional local account billing transformations.
// A message_start primer is not a final meter. Unknown/invalid counters never
// become a zero receipt, and old usage_logs are deliberately not a fallback.
func observeReceiptFinality(data string, usage *ClaudeUsage) {
	if usage == nil {
		return
	}
	if data == "[DONE]" {
		usage.receiptFinal = usage.receiptInputSeen && usage.receiptOutputSeen
		return
	}
	eventType := gjson.Get(data, "type").String()
	if eventType != "message_start" && eventType != "message_delta" && eventType != "message_stop" {
		return
	}
	if !gjson.Valid(data) {
		usage.receiptInvalid = true
		usage.receiptMeasured = nil
		return
	}
	v := gjson.Parse(data)
	if eventType == "message_start" {
		usage.receiptInvalid = false
		usage.receiptWire = UsageReceipt{}
	}
	counters := v.Get("usage")
	if eventType == "message_start" {
		counters = v.Get("message.usage")
	}
	if !validReceiptCounters(counters) {
		usage.receiptInvalid = true
		usage.receiptMeasured = nil
	}
	if counters.IsObject() && !usage.receiptInvalid {
		mergeReceiptWire(&usage.receiptWire, counters)
	}
	switch eventType {
	case "message_start":
		_, usage.receiptInputSeen = parseSSEUsageInt(v.Get("message.usage.input_tokens").Value())
		usage.receiptFinal = false
		usage.receiptOutputSeen = false
		usage.receiptMeasured = nil
	case "message_delta":
		_, valid := parseSSEUsageInt(v.Get("usage.output_tokens").Value())
		usage.receiptOutputSeen = usage.receiptOutputSeen || valid
		if v.Get("delta.stop_reason").String() != "" {
			usage.receiptFinal = usage.receiptInputSeen && valid
		}
	case "message_stop":
		usage.receiptFinal = usage.receiptInputSeen && usage.receiptOutputSeen
	}
}

func validReceiptCounters(counters gjson.Result) bool {
	if !counters.Exists() {
		return true
	}
	if !counters.IsObject() {
		return false
	}
	for _, parent := range []string{"cache_creation", "server_tool_use"} {
		if value := counters.Get(parent); value.Exists() && !value.IsObject() {
			return false
		}
	}
	for _, field := range []string{"input_tokens", "output_tokens", "cache_read_input_tokens", "cache_creation_input_tokens", "cache_creation_5m_input_tokens", "cache_creation_1h_input_tokens", "cache_creation.ephemeral_5m_input_tokens", "cache_creation.ephemeral_1h_input_tokens", "server_tool_use.web_search_requests"} {
		if value := counters.Get(field); value.Exists() {
			if _, ok := parseSSEUsageInt(value.Value()); !ok {
				return false
			}
		}
	}
	return true
}

// Read the actual output event, not ClaudeUsage: provider adapters can normalize
// internal accounting fields without changing the passthrough wire.
func mergeReceiptWire(r *UsageReceipt, counters gjson.Result) {
	for _, field := range []struct {
		name string
		dest *int
	}{
		{"input_tokens", &r.InputTokens}, {"output_tokens", &r.OutputTokens},
		{"cache_read_input_tokens", &r.CacheReadTokens}, {"cache_creation_input_tokens", &r.CacheCreationTokens},
		{"cache_creation_5m_input_tokens", &r.CacheCreation5mTokens}, {"cache_creation_1h_input_tokens", &r.CacheCreation1hTokens},
		{"cache_creation.ephemeral_5m_input_tokens", &r.CacheCreation5mTokens}, {"cache_creation.ephemeral_1h_input_tokens", &r.CacheCreation1hTokens},
	} {
		if value, ok := parseSSEUsageInt(counters.Get(field.name).Value()); ok {
			*field.dest = value
		}
	}
	if calls, ok := parseSSEUsageInt(counters.Get("server_tool_use.web_search_requests").Value()); ok {
		r.WebSearchCalls = max(r.WebSearchCalls, calls)
	}
	if r.ServiceTier == "" {
		switch strings.ToLower(strings.TrimSpace(counters.Get("speed").String())) {
		case "fast":
			r.ServiceTier = "priority"
		case "standard":
			r.ServiceTier = "standard"
		}
		if r.ServiceTier == "" {
			switch strings.ToLower(strings.TrimSpace(counters.Get("service_tier").String())) {
			case "priority":
				r.ServiceTier = "priority"
			case "standard", "batch":
				r.ServiceTier = "standard"
			}
		}
	}
}

func freezeReceiptUsage(u *ClaudeUsage) {
	if u == nil || !u.receiptFinal || u.receiptInvalid {
		return
	}
	copy := u.receiptWire
	u.receiptMeasured = &copy
}

type UsageReceiptWriter interface {
	SaveUsageReceipt(context.Context, int64, *UsageReceipt) error
}

func (s *GatewayService) recordProviderUsageReceipt(ctx context.Context, input *recordUsageCoreInput) {
	r := input.Result
	if r == nil || !r.Stream || r.Usage.receiptMeasured == nil || r.ImageCount != 0 || r.AudioUsage != nil || r.SearchCount != 0 || r.CacheStorageTokenHours != 0 {
		return
	}
	writer, ok := s.usageLogRepo.(UsageReceiptWriter)
	if !ok || input.APIKey == nil {
		return
	}
	id := strings.TrimPrefix(resolveUsageBillingRequestID(ctx, r.RequestID), "client:")
	if !ValidUsageReceiptID(id) {
		return
	}
	model := r.Model
	if input.OriginalModel != "" {
		model = input.OriginalModel
	}
	receipt := *r.Usage.receiptMeasured
	receipt.Object = "sub2api.usage_receipt"
	receipt.Version = 1
	receipt.Source = "wire_final"
	receipt.RequestID = id
	receipt.Model = model
	receipt.BillingMode = "token"
	receipt.InboundEndpoint = input.InboundEndpoint
	receipt.RecordedAt = time.Now().UTC()

	// Failure here must not prevent the existing Sub2API customer settlement.
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := writer.SaveUsageReceipt(writeCtx, input.APIKey.ID, &receipt); err != nil {
		slog.Error("provider_usage_receipt_write_failed", "api_key_id", input.APIKey.ID, "request_id", id)
	}
}
