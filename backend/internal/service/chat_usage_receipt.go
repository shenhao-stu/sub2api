package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const ChatReceiptVersionHeader = "X-Sub2API-Usage-Receipt"
const ChatReceiptIDHeader = "X-Sub2API-Usage-Receipt-ID"
const ChatReceiptEndpoint = "/v1/chat/completions"

type ChatUsageReceiptClaim struct {
	APIKeyID    int64  `json:"api_key_id"`
	RequestID   string `json:"request_id"`
	Model       string `json:"model"`
	Endpoint    string `json:"endpoint"`
	PayloadHash string `json:"payload_hash"`
	Token       string `json:"token"`
}

type ChatUsageReceiptRepository interface {
	ClaimChatUsageReceipt(context.Context, *ChatUsageReceiptClaim) (bool, error)
	CompleteChatUsageReceipt(context.Context, *ChatUsageReceiptClaim, int64, string, *UsageReceipt) error
}

var ErrChatReceiptConflict = errors.New("usage receipt identity already claimed")
var ErrChatReceiptUnsupported = errors.New("usage receipt protocol unavailable")

type chatReceiptContextKey struct{}
type chatReceiptCapture struct {
	claim          *ChatUsageReceiptClaim
	meter          *UsageReceipt
	terminal       string
	status         int
	rawMeter       *UsageReceipt
	rawTerminal    string
	rawReady       bool
	rawDone        bool
	rawInvalidTail bool
}

// Only adapters with final downstream Chat-meter capture can acknowledge v2.
func (s *OpenAIGatewayService) SupportsChatUsageReceipt(ctx context.Context, account *Account, body []byte) bool {
	// V2 has no cache-write TTL representation. Do not acknowledge a request
	// whose final meter could lose a real Kimi 1h charge.
	return account != nil && account.Platform != PlatformKimi && s.resolveUpstreamProtocolFor(ctx, account, APIProtocolChatCompletions, upstreamRoutingModel(account, body, "")) != APIProtocolAnthropic
}

// BeginChatUsageReceipt reserves an identity before generation. Reusing even an
// identical identity is never permission to send a second upstream request.
func (s *OpenAIGatewayService) BeginChatUsageReceipt(ctx context.Context, key int64, id, model string, body []byte) (context.Context, error) {
	if key <= 0 || !ValidUsageReceiptID(id) || strings.TrimSpace(model) == "" {
		return ctx, ErrChatReceiptUnsupported
	}
	// A stable server context identity is required for the private ledger join.
	clientID, _ := ctx.Value(ctxkey.ClientRequestID).(string)
	requestID, _ := ctx.Value(ctxkey.RequestID).(string)
	if strings.TrimSpace(clientID) == "" && strings.TrimSpace(requestID) == "" {
		return ctx, ErrChatReceiptUnsupported
	}
	repo, ok := s.usageLogRepo.(ChatUsageReceiptRepository)
	if !ok {
		return ctx, ErrChatReceiptUnsupported
	}
	claim := &ChatUsageReceiptClaim{APIKeyID: key, RequestID: id, Model: model, Endpoint: ChatReceiptEndpoint, PayloadHash: HashUsageRequestPayload(body), Token: uuid.NewString()}
	claimed, err := repo.ClaimChatUsageReceipt(ctx, claim)
	if err != nil {
		return ctx, err
	}
	if !claimed {
		return ctx, ErrChatReceiptConflict
	}
	return context.WithValue(ctx, chatReceiptContextKey{}, &chatReceiptCapture{claim: claim}), nil
}

func chatReceiptFromContext(ctx context.Context) *chatReceiptCapture {
	if ctx == nil {
		return nil
	}
	value, _ := ctx.Value(chatReceiptContextKey{}).(*chatReceiptCapture)
	return value
}

func observeChatReceiptMeter(c *gin.Context, meter *apicompat.ChatUsage, tier, terminal string) {
	capture := chatReceiptFromContext(c.Request.Context())
	if capture == nil {
		return
	}
	capture.meter = nil
	if meter == nil {
		return
	}
	raw, err := json.Marshal(meter)
	if err != nil {
		return
	}
	receipt := parseChatReceiptMeter(raw)
	if receipt == nil {
		return
	}
	switch tier {
	case "priority", "default", "standard", "flex":
		receipt.ServiceTier = tier
	}
	capture.meter = receipt
	capture.terminal = terminal
}

func ValidateChatUsageReceipt(receipt *UsageReceipt) bool {
	if receipt == nil || receipt.Version != 2 || receipt.Object != "sub2api.usage_receipt" || receipt.Source != "wire_final" || !ValidUsageReceiptID(receipt.RequestID) || receipt.Model == "" || receipt.InboundEndpoint != ChatReceiptEndpoint || receipt.BillingMode != "token" || receipt.RecordedAt.IsZero() {
		return false
	}
	if receipt.InputTokens < 0 || receipt.OutputTokens < 0 || receipt.CacheReadTokens < 0 || receipt.CacheCreationTokens < 0 || receipt.ReasoningTokens < 0 || receipt.ReasoningTokens > receipt.OutputTokens || receipt.WebSearchCalls != 0 || receipt.CacheCreation5mTokens != 0 || receipt.CacheCreation1hTokens != 0 {
		return false
	}
	if receipt.CacheReadTokens > math.MaxInt-receipt.InputTokens || receipt.CacheCreationTokens > math.MaxInt-receipt.InputTokens-receipt.CacheReadTokens {
		return false
	}
	switch receipt.ServiceTier {
	case "", "priority", "default", "standard", "flex":
	default:
		return false
	}
	switch receipt.TerminalState {
	case "completed", "incomplete":
		return receipt.HTTPStatus >= 200 && receipt.HTTPStatus < 300
	case "failed":
		return receipt.HTTPStatus >= 400 && receipt.HTTPStatus <= 599
	default:
		return false
	}
}

func writeChatReceiptAck(c *gin.Context) {
	if capture := chatReceiptFromContext(c.Request.Context()); capture != nil {
		c.Header(ChatReceiptVersionHeader, "v2")
		c.Header(ChatReceiptIDHeader, capture.claim.RequestID)
	}
}

// Strict receipt parsing is deliberately separate from permissive live billing:
// absent/invalid counters remain unknown and can never become an authoritative 0.
func parseChatReceiptMeter(data []byte) *UsageReceipt {
	fields, ok := receiptObject(data)
	if !ok {
		return nil
	}
	input, ok := receiptCounter(fields, "prompt_tokens")
	if !ok {
		return nil
	}
	output, ok := receiptCounter(fields, "completion_tokens")
	if !ok {
		return nil
	}
	var cached, write, reasoning int
	var nestedRead, nestedWrite, nestedReason bool
	for _, name := range []string{"prompt_tokens_details", "input_tokens_details", "completion_tokens_details", "output_tokens_details"} {
		if raw, exists := fields[name]; exists {
			details, valid := receiptObject(raw)
			if !valid {
				return nil
			}
			for key, value := range details {
				if strings.HasSuffix(key, "_tokens") {
					if _, valid = receiptInteger(value); !valid {
						return nil
					}
				}
			}
			if name == "prompt_tokens_details" || name == "input_tokens_details" {
				if !receiptMergeAlias(details, "cached_tokens", &cached, &nestedRead) ||
					!receiptMergeAlias(details, "cache_creation_tokens", &write, &nestedWrite) ||
					!receiptMergeAlias(details, "cache_write_tokens", &write, &nestedWrite) {
					return nil
				}
			} else if !receiptMergeAlias(details, "reasoning_tokens", &reasoning, &nestedReason) {
				return nil
			}
			for _, key := range []string{"audio_tokens", "image_tokens", "cache_creation_5m_input_tokens", "cache_creation_1h_input_tokens"} {
				if raw, exists := details[key]; exists {
					value, valid := receiptInteger(raw)
					if !valid || value != 0 {
						return nil
					}
				}
			}
		}
	}
	for name, raw := range fields {
		if strings.HasSuffix(name, "_tokens") {
			if _, valid := receiptInteger(raw); !valid {
				return nil
			}
		}
	}
	for _, group := range []struct {
		names []string
		dest  *int
		seen  bool
	}{
		{[]string{"cached_tokens", "cache_read_tokens", "cache_read_input_tokens", "prompt_cache_hit_tokens"}, &cached, nestedRead},
		{[]string{"cache_write_tokens", "cache_creation_tokens", "cache_write_input_tokens", "cache_creation_input_tokens"}, &write, nestedWrite},
		{[]string{"reasoning_tokens"}, &reasoning, nestedReason},
	} {
		for _, name := range group.names {
			if raw, exists := fields[name]; exists {
				value, valid := receiptInteger(raw)
				if !valid || (group.seen && value != *group.dest) {
					return nil
				}
				*group.dest = value
				group.seen = true
			}
		}
	}
	if raw, exists := fields["prompt_cache_miss_tokens"]; exists {
		miss, valid := receiptInteger(raw)
		if !valid || cached > input || miss != input-cached {
			return nil
		}
	}
	for _, name := range []string{"cache_creation_5m_input_tokens", "cache_creation_1h_input_tokens", "image_tokens", "audio_tokens"} {
		if raw, exists := fields[name]; exists {
			value, valid := receiptInteger(raw)
			if !valid || value != 0 {
				return nil
			}
		}
	}
	if cached > input || write > input-cached || reasoning > output {
		return nil
	}
	if raw, exists := fields["total_tokens"]; exists {
		total, valid := receiptInteger(raw)
		if !valid || output > math.MaxInt-input || total != input+output {
			return nil
		}
	}
	return &UsageReceipt{InputTokens: input - cached - write, OutputTokens: output, CacheReadTokens: cached, CacheCreationTokens: write, ReasoningTokens: reasoning}
}

// Aliases are evidence for one bucket, not additional billable quantities.
func receiptMergeAlias(fields map[string]json.RawMessage, key string, dest *int, seen *bool) bool {
	raw, exists := fields[key]
	if !exists {
		return true
	}
	value, valid := receiptInteger(raw)
	if !valid || (*seen && value != *dest) {
		return false
	}
	*dest, *seen = value, true
	return true
}
func receiptInteger(raw json.RawMessage) (int, bool) {
	var n *int
	if err := json.Unmarshal(raw, &n); err != nil || n == nil || *n < 0 {
		return 0, false
	}
	return *n, true
}
func receiptCounter(fields map[string]json.RawMessage, name string) (int, bool) {
	raw, ok := fields[name]
	if !ok {
		return 0, false
	}
	return receiptInteger(raw)
}
func receiptObject(data []byte) (map[string]json.RawMessage, bool) {
	d := json.NewDecoder(bytes.NewReader(data))
	tok, err := d.Token()
	if err != nil || tok != json.Delim('{') {
		return nil, false
	}
	fields := map[string]json.RawMessage{}
	for d.More() {
		tok, err = d.Token()
		if err != nil {
			return nil, false
		}
		name, ok := tok.(string)
		if !ok {
			return nil, false
		}
		name = strings.ToLower(name)
		if _, exists := fields[name]; exists {
			return nil, false
		}
		var raw json.RawMessage
		if d.Decode(&raw) != nil {
			return nil, false
		}
		fields[name] = raw
	}
	if _, err = d.Token(); err != nil {
		return nil, false
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, false
	}
	return fields, true
}

func (s *OpenAIGatewayService) completeChatUsageReceipt(ctx context.Context, c *gin.Context, account *Account, result *OpenAIForwardResult, forwardErr error) {
	capture := chatReceiptFromContext(ctx)
	if capture == nil || capture.meter == nil || result == nil || account == nil || result.ImageCount != 0 || result.AudioUsage != nil || result.SearchCount != 0 || result.WebSearchCalls != 0 {
		return
	}
	repo, ok := s.usageLogRepo.(ChatUsageReceiptRepository)
	if !ok {
		return
	}
	receipt := *capture.meter
	receipt.Object = "sub2api.usage_receipt"
	receipt.Version = 2
	receipt.Source = "wire_final"
	receipt.RequestID = capture.claim.RequestID
	receipt.Model = capture.claim.Model
	receipt.InboundEndpoint = ChatReceiptEndpoint
	receipt.BillingMode = "token"
	receipt.RecordedAt = time.Now().UTC()
	receipt.TerminalState = capture.terminal
	receipt.HTTPStatus = http.StatusOK
	if capture.terminal == "failed" {
		receipt.HTTPStatus = http.StatusBadGateway
	}
	if capture.status >= 400 && capture.status <= 599 {
		receipt.HTTPStatus = capture.status
	}
	if forwardErr != nil {
		receipt.TerminalState = "failed"
		receipt.HTTPStatus = http.StatusBadGateway
		if c.Writer.Status() >= 400 {
			receipt.HTTPStatus = c.Writer.Status()
		}
		if capture.status >= 400 && capture.status <= 599 {
			receipt.HTTPStatus = capture.status
		}
		if value, ok := c.Get(OpsStreamErrorKey); ok {
			if e, ok := value.(OpsStreamError); ok && e.IntendedStatus >= 400 && e.IntendedStatus <= 599 {
				receipt.HTTPStatus = e.IntendedStatus
			}
		}
	}
	if !ValidateChatUsageReceipt(&receipt) {
		return
	}
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := repo.CompleteChatUsageReceipt(writeCtx, capture.claim, account.ID, resolveUsageBillingRequestID(ctx, result.RequestID), &receipt); err != nil {
		slog.Error("provider_chat_usage_receipt_write_failed", "api_key_id", capture.claim.APIKeyID, "request_id", capture.claim.RequestID)
	}
}

// Native Chat meter is accepted only at a final JSON response or a terminal SSE
// meter followed by DONE. A primer followed by more answer text is not final.
func observeRawChatReceipt(c *gin.Context, payload []byte, streaming bool) {
	capture := chatReceiptFromContext(c.Request.Context())
	if capture == nil {
		return
	}
	if capture.rawInvalidTail {
		return
	}
	if string(bytes.TrimSpace(payload)) == "[DONE]" {
		if capture.rawReady {
			capture.rawDone = true
			capture.meter = capture.rawMeter
			capture.terminal = capture.rawTerminal
		}
		return
	}
	if capture.rawDone {
		capture.meter = nil
		capture.rawReady = false
		capture.rawInvalidTail = true
		return
	}
	fields, ok := receiptObject(payload)
	if !ok {
		capture.rawInvalidTail = true
		capture.rawReady = false
		return
	}
	var choices []struct {
		FinishReason *string         `json:"finish_reason"`
		Delta        json.RawMessage `json:"delta"`
		Message      json.RawMessage `json:"message"`
	}
	if raw, exists := fields["choices"]; exists {
		var objects []json.RawMessage
		if json.Unmarshal(raw, &objects) != nil || json.Unmarshal(raw, &choices) != nil {
			capture.rawInvalidTail = true
			capture.rawReady = false
			return
		}
		for _, object := range objects {
			if _, valid := receiptObject(object); !valid {
				capture.rawInvalidTail = true
				capture.rawReady = false
				return
			}
		}
	}
	terminal := ""
	for _, choice := range choices {
		if capture.rawTerminal != "" && (receiptHasContent(choice.Delta) || receiptHasContent(choice.Message)) {
			capture.rawInvalidTail = true
			capture.rawReady = false
			return
		}
		if choice.FinishReason != nil {
			switch *choice.FinishReason {
			case "stop", "tool_calls", "function_call":
				terminal = "completed"
			case "length", "content_filter":
				terminal = "incomplete"
			case "":
			default:
				capture.rawInvalidTail = true
				capture.rawReady = false
				return
			}
		}
	}
	if raw, exists := fields["error"]; exists && string(raw) != "null" {
		terminal = "failed"
		if errorFields, valid := receiptObject(raw); valid {
			if status, valid := receiptCounter(errorFields, "status_code"); valid && status >= 400 && status <= 599 {
				capture.status = status
			}
		}
	}
	if terminal != "" {
		capture.rawTerminal = terminal
	}
	raw, hasUsage := fields["usage"]
	if !hasUsage || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		if len(choices) > 0 {
			capture.rawReady = false
		}
		return
	}
	meter := parseChatReceiptMeter(raw)
	if meter == nil {
		// A partial or contradictory meter cannot later be erased by a complete
		// zero. Preserve uncertainty; permissive live billing remains unchanged.
		capture.rawInvalidTail = true
		capture.rawReady = false
		return
	}
	if capture.rawMeter != nil && (meter.InputTokens+meter.CacheReadTokens+meter.CacheCreationTokens < capture.rawMeter.InputTokens+capture.rawMeter.CacheReadTokens+capture.rawMeter.CacheCreationTokens || meter.OutputTokens < capture.rawMeter.OutputTokens) {
		capture.rawInvalidTail = true
		capture.rawReady = false
		return
	}
	if value, exists := fields["service_tier"]; exists {
		var tier string
		if json.Unmarshal(value, &tier) != nil {
			capture.rawReady = false
			return
		}
		switch tier {
		case "priority", "default", "standard", "flex":
			meter.ServiceTier = tier
		}
	}
	if !streaming {
		if terminal != "" {
			capture.meter = meter
			capture.terminal = terminal
		}
		return
	}
	// Usage-only chunks can complete an already observed finish; DONE alone
	// cannot upgrade a primer to an authoritative terminal meter.
	capture.rawReady = capture.rawTerminal != ""
	capture.rawMeter = meter
	if capture.rawTerminal == "failed" {
		capture.rawDone = true
		capture.meter = meter
		capture.terminal = capture.rawTerminal
	}
}

// Empty role/usage delimiters are not additional answer content after finish.
func receiptHasContent(data []byte) bool {
	trimmed := bytes.TrimSpace(data)
	return len(trimmed) != 0 && !bytes.Equal(trimmed, []byte("null")) && !bytes.Equal(trimmed, []byte("{}"))
}

// Metered HTTP errors already stop failover and emit a bounded error plus real
// totals. Completeness still comes from the original body, never from permissive
// result defaults. Capture only when those totals match the actual error wire.
func observeChatHTTPErrorReceipt(c *gin.Context, body []byte, status int, result *OpenAIForwardResult) {
	capture := chatReceiptFromContext(c.Request.Context())
	if capture == nil || result == nil || status < 400 || status > 599 {
		return
	}
	fields, valid := receiptObject(body)
	if !valid {
		return
	}
	raw, hasUsage := fields["usage"]
	if response, exists := fields["response"]; exists {
		nested, valid := receiptObject(response)
		if !valid {
			return
		}
		if inner, exists := nested["usage"]; exists {
			if hasUsage {
				return
			} // Do not choose between two source meters.
			raw, hasUsage = inner, true
		}
	}
	if !hasUsage {
		return
	}
	meterFields, valid := receiptObject(raw)
	if !valid {
		return
	}
	for _, pair := range [][2]string{{"input_tokens", "prompt_tokens"}, {"output_tokens", "completion_tokens"}} {
		if alias, exists := meterFields[pair[0]]; exists {
			if other, exists := meterFields[pair[1]]; exists {
				left, leftOK := receiptInteger(alias)
				right, rightOK := receiptInteger(other)
				if !leftOK || !rightOK || left != right {
					return
				}
			}
			meterFields[pair[1]] = alias
			delete(meterFields, pair[0])
		}
	}
	encoded, err := json.Marshal(meterFields)
	if err != nil {
		return
	}
	native := parseChatReceiptMeter(encoded)
	if native == nil {
		return
	}
	wire := result.Usage
	if wire.InputTokens != native.InputTokens+native.CacheReadTokens+native.CacheCreationTokens || wire.OutputTokens != native.OutputTokens || wire.CacheReadInputTokens != native.CacheReadTokens || wire.CacheCreationInputTokens != native.CacheCreationTokens || wire.ImageInputTokens != 0 || wire.ImageOutputTokens != 0 || wire.ImageCacheReadTokens != 0 {
		return
	}
	// The existing HTTP error envelope emits no separate reasoning counter.
	// Output already includes it; never invent a second charge or a wire detail.
	native.ReasoningTokens = 0
	switch tier := observedUpstreamResponseServiceTier(c); tier {
	case "priority", "default", "standard", "flex":
		native.ServiceTier = tier
	}
	capture.meter, capture.terminal, capture.status = native, "failed", status
}
