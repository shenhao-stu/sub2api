package service

import (
	"context"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
)

func TestReceiptRequiresProviderFinalMeter(t *testing.T) {
	for _, parse := range []struct {
		name string
		f    func(string, *ClaudeUsage)
	}{
		{"normal", (&GatewayService{}).parseSSEUsage}, {"passthrough", parseSSEUsagePassthrough},
	} {
		t.Run(parse.name, func(t *testing.T) {
			for _, tc := range []struct {
				name, start, delta string
				want               bool
			}{
				{"primer", "10", `{}`, false},
				{"partial", "10", `{"type":"message_delta","usage":{"output_tokens":2}}`, false},
				{"final", "10", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":64000}}`, true},
				{"explicit_zero", "0", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":0}}`, true},
				{"no_input", "null", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":7}}`, false},
				{"negative", "10", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":-1}}`, false},
				{"fraction", "10", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1.5}}`, false},
			} {
				var u ClaudeUsage
				parse.f(`{"type":"message_start","message":{"usage":{"input_tokens":`+tc.start+`,"output_tokens":2}}}`, &u)
				parse.f(tc.delta, &u)
				if u.receiptFinal != tc.want {
					t.Fatalf("%s final=%v want %v", tc.name, u.receiptFinal, tc.want)
				}
			}
		})
	}
}

func TestReceiptInvalidFinalMeterCannotReusePartialUsage(t *testing.T) {
	for _, parse := range []func(string, *ClaudeUsage){(&GatewayService{}).parseSSEUsage, parseSSEUsagePassthrough} {
		for _, broken := range []string{
			`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":-1}}`,
			`{"type":"message_delta","usage":{"output_tokens":null}}`,
			`{"type":"message_delta","usage":{"output_tokens":64000,"cache_read_input_tokens":-1}}`,
			`{"type":"message_delta","usage":{"output_tokens":64000`,
		} {
			var u ClaudeUsage
			parse(`{"type":"message_start","message":{"usage":{"input_tokens":1000}}}`, &u)
			parse(`{"type":"message_delta","usage":{"output_tokens":2}}`, &u)
			parse(broken, &u)
			parse(`{"type":"message_stop"}`, &u)
			if u.receiptMeasured != nil {
				t.Fatalf("invalid terminal meter reused partial output: %+v", u.receiptMeasured)
			}
		}
	}
}

func TestReceiptPreservesWireFieldsBeforeProviderNormalization(t *testing.T) {
	for _, parse := range []func(string, *ClaudeUsage){(&GatewayService{}).parseSSEUsage, parseSSEUsagePassthrough} {
		var u ClaudeUsage
		parse(`{"type":"message_start","message":{"usage":{"input_tokens":2,"prompt_tokens":1000,"cache_read_input_tokens":200,"cache_creation_input_tokens":100,"cache_creation_1h_input_tokens":100,"server_tool_use":{"web_search_requests":2}}}}`, &u)
		parse(`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":64,"speed":"fast","server_tool_use":{"web_search_requests":1}}}`, &u)
		r := u.receiptMeasured
		if r == nil || r.InputTokens != 2 || r.CacheCreation1hTokens != 100 || r.WebSearchCalls != 2 || r.ServiceTier != "priority" {
			t.Fatalf("wire evidence changed by provider-normalized accounting: %+v", r)
		}
	}
}

type receiptWriterStub struct {
	UsageLogRepository
	receipt *UsageReceipt
	key     int64
}

func (r *receiptWriterStub) SaveUsageReceipt(_ context.Context, key int64, v *UsageReceipt) error {
	r.key = key
	copy := *v
	r.receipt = &copy
	return nil
}
func TestReceiptCapturesProviderBucketsBeforeBillingPolicies(t *testing.T) {
	repo := &receiptWriterStub{}
	s := &GatewayService{usageLogRepo: repo}
	ctx := context.WithValue(context.Background(), ctxkey.ClientRequestID, "2c4e888b-836b-4992-9ff1-dc1699488902")
	r := &ForwardResult{Stream: true, Model: "model"}
	s.parseSSEUsage(`{"type":"message_start","message":{"usage":{"input_tokens":1000,"cache_creation_input_tokens":100,"cache_creation":{"ephemeral_1h_input_tokens":100}}}}`, &r.Usage)
	s.parseSSEUsage(`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":64000}}`, &r.Usage)
	s.recordProviderUsageReceipt(ctx, &recordUsageCoreInput{Result: r, APIKey: &APIKey{ID: 11}, ChannelUsageFields: ChannelUsageFields{OriginalModel: "requested"}, InboundEndpoint: "/v1/messages"})
	r.Usage.CacheReadInputTokens += r.Usage.InputTokens
	r.Usage.InputTokens = 0
	applyCacheTTLOverride(&r.Usage, "5m")
	if repo.receipt == nil || repo.key != 11 || repo.receipt.InputTokens != 1000 || repo.receipt.CacheCreation1hTokens != 100 || repo.receipt.Model != "requested" {
		t.Fatalf("provider evidence changed: %+v", repo.receipt)
	}
}
