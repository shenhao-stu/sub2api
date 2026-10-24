//go:build unit

package service

import (
	"context"
	"github.com/gin-gonic/gin"
	"net/http/httptest"
	"testing"
	"time"
)

type chatReceiptReadStub struct {
	UsageLogRepository
	value *UsageReceipt
}

func (s *chatReceiptReadStub) GetUsageReceipt(context.Context, int64, string) (*UsageReceipt, error) {
	return s.value, nil
}

func TestChatReceiptV2ReaderPreservesVersion(t *testing.T) {
	const id = "2c4e888b-836b-4992-9ff1-dc1699488902"
	repo := &chatReceiptReadStub{value: &UsageReceipt{Version: 2, Object: "sub2api.usage_receipt", Source: "wire_final", BillingMode: "token", TerminalState: "completed", HTTPStatus: 200, RequestID: id, Model: "gemini-argon", InputTokens: 100, OutputTokens: 50, InboundEndpoint: "/v1/chat/completions", RecordedAt: time.Now()}}
	svc := &UsageService{usageRepo: repo}
	got, err := svc.GetAPIKeyUsageReceipt(context.Background(), 1, id)
	if err != nil || got.Version != 2 {
		t.Fatalf("v2 identity changed: got=%+v err=%v", got, err)
	}
}

func TestChatReceiptStrictMeter(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		want      bool
	}{
		{"positive", `{"prompt_tokens":100,"completion_tokens":50,"total_tokens":150,"prompt_tokens_details":{"cached_tokens":80},"completion_tokens_details":{"reasoning_tokens":20}}`, true},
		{"zero", `{"prompt_tokens":0,"completion_tokens":0}`, true},
		{"empty", `{}`, false}, {"null", `null`, false}, {"partial", `{"prompt_tokens":10}`, false},
		{"negative", `{"prompt_tokens":10,"completion_tokens":-1}`, false},
		{"fraction", `{"prompt_tokens":10,"completion_tokens":1.5}`, false},
		{"overflow", `{"prompt_tokens":9223372036854775808,"completion_tokens":0}`, false},
		{"duplicate", `{"prompt_tokens":10,"prompt_tokens":0,"completion_tokens":0}`, false},
		{"case_duplicate", `{"prompt_tokens":10,"PROMPT_TOKENS":0,"completion_tokens":0}`, false},
		{"aux_null", `{"prompt_tokens":0,"completion_tokens":0,"total_tokens":null}`, false},
		{"cache_underflow", `{"prompt_tokens":10,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":11}}`, false},
		{"cache_conflict", `{"prompt_tokens":10,"completion_tokens":2,"prompt_tokens_details":{"cache_write_tokens":2,"cache_creation_tokens":3}}`, false},
		{"reasoning_conflict", `{"prompt_tokens":10,"completion_tokens":2,"completion_tokens_details":{"reasoning_tokens":3}}`, false},
		{"audio_unrepresented", `{"prompt_tokens":10,"completion_tokens":2,"completion_tokens_details":{"audio_tokens":1}}`, false},
		{"bad_total", `{"prompt_tokens":10,"completion_tokens":2,"total_tokens":0}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := parseChatReceiptMeter([]byte(tc.raw))
			if (got != nil) != tc.want {
				t.Fatalf("meter=%+v want=%v", got, tc.want)
			}
		})
	}
}

func TestChatReceiptRawFinality(t *testing.T) {
	for _, tc := range []struct {
		name   string
		frames []string
		want   bool
	}{
		{"terminal_zero", []string{`{"choices":[{"finish_reason":"stop"}]}`, `{"choices":[],"usage":{"prompt_tokens":0,"completion_tokens":0}}`, `[DONE]`}, true},
		{"usage_only_done_not_terminal", []string{`{"choices":[],"usage":{"prompt_tokens":0,"completion_tokens":0}}`, `[DONE]`}, false},
		{"partial_positive_then_final_zero", []string{`{"usage":{"prompt_tokens":1000}}`, `{"choices":[{"finish_reason":"stop"}],"usage":{"prompt_tokens":0,"completion_tokens":0}}`, `[DONE]`}, false},
		{"partial_positive_then_usage_only_zero", []string{`{"usage":{"prompt_tokens":1000}}`, `{"choices":[],"usage":{"prompt_tokens":0,"completion_tokens":0}}`, `[DONE]`}, false},
		{"null_primer_then_final", []string{`{"choices":[{"delta":{"role":"assistant"}}],"usage":null}`, `{"choices":[{"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5}}`, `[DONE]`}, true},
		{"positive_then_lower", []string{`{"usage":{"prompt_tokens":1000,"completion_tokens":50}}`, `{"choices":[{"finish_reason":"stop"}],"usage":{"prompt_tokens":100,"completion_tokens":5}}`, `[DONE]`}, false},
		{"duplicate_finish", []string{`{"choices":[{"finish_reason":"length","finish_reason":"stop"}],"usage":{"prompt_tokens":0,"completion_tokens":0}}`, `[DONE]`}, false},
		{"content_after_finish", []string{`{"choices":[{"finish_reason":"stop"}]}`, `{"choices":[{"delta":{"content":"synthetic late"}}]}`, `{"choices":[],"usage":{"prompt_tokens":0,"completion_tokens":0}}`, `[DONE]`}, false},
		{"unknown_finish", []string{`{"choices":[{"finish_reason":"unknown"}],"usage":{"prompt_tokens":0,"completion_tokens":0}}`, `[DONE]`}, false},
		{"no_done", []string{`{"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":2}}`}, false},
		{"primer_then_content", []string{`{"choices":[],"usage":{"prompt_tokens":0,"completion_tokens":0}}`, `{"choices":[{"delta":{"content":"synthetic"}}]}`, `[DONE]`}, false},
		{"positive_then_zero", []string{`{"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5}}`, `{"choices":[],"usage":{"prompt_tokens":0,"completion_tokens":0}}`, `[DONE]`}, false},
		{"duplicate_usage", []string{`{"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5},"usage":{"prompt_tokens":0,"completion_tokens":0}}`, `[DONE]`}, false},
		{"done_then_invalid", []string{`{"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5}}`, `[DONE]`, `broken`}, false},
		{"done_then_zero", []string{`{"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5}}`, `[DONE]`, `{"choices":[],"usage":{"prompt_tokens":0,"completion_tokens":0}}`, `[DONE]`}, false},
		{"failed_then_invalid", []string{`{"error":{"status_code":503},"usage":{"prompt_tokens":10,"completion_tokens":5}}`, `broken`}, false},
		{"late_positive", []string{`{"choices":[{"finish_reason":"stop"}]}`, `{"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5}}`, `[DONE]`}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			capture := &chatReceiptCapture{}
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("POST", ChatReceiptEndpoint, nil).WithContext(context.WithValue(context.Background(), chatReceiptContextKey{}, capture))
			for _, frame := range tc.frames {
				observeRawChatReceipt(c, []byte(frame), true)
			}
			if (capture.meter != nil) != tc.want {
				t.Fatalf("meter=%+v want=%v", capture.meter, tc.want)
			}
		})
	}
}

func TestChatReceiptCacheAliasesCannotBecomeNormalInput(t *testing.T) {
	for _, raw := range []string{
		`{"prompt_tokens":100,"completion_tokens":5,"prompt_cache_hit_tokens":90,"prompt_cache_miss_tokens":10}`,
		`{"prompt_tokens":100,"completion_tokens":5,"cache_read_input_tokens":90}`,
	} {
		r := parseChatReceiptMeter([]byte(raw))
		if r == nil || r.InputTokens != 10 || r.CacheReadTokens != 90 {
			t.Fatalf("cache classification lost: %+v", r)
		}
	}
}

func TestChatReceiptDetailAliases(t *testing.T) {
	for _, raw := range []string{
		`{"prompt_tokens":100,"completion_tokens":5,"input_tokens_details":{"cached_tokens":90},"output_tokens_details":{"reasoning_tokens":3}}`,
		`{"prompt_tokens":100,"completion_tokens":5,"input_tokens_details":{"cached_tokens":90},"prompt_tokens_details":{"cached_tokens":90},"output_tokens_details":{"reasoning_tokens":3},"completion_tokens_details":{"reasoning_tokens":3}}`,
	} {
		r := parseChatReceiptMeter([]byte(raw))
		if r == nil || r.InputTokens != 10 || r.CacheReadTokens != 90 || r.ReasoningTokens != 3 {
			t.Fatalf("detail aliases lost: %+v", r)
		}
	}
	for _, raw := range []string{
		`{"prompt_tokens":100,"completion_tokens":5,"input_tokens_details":{"cached_tokens":90},"prompt_tokens_details":{"cached_tokens":0}}`,
		`{"prompt_tokens":100,"completion_tokens":5,"output_tokens_details":{"reasoning_tokens":3},"completion_tokens_details":{"reasoning_tokens":0}}`,
		`{"prompt_tokens":100,"completion_tokens":5,"input_tokens_details":null}`,
	} {
		if r := parseChatReceiptMeter([]byte(raw)); r != nil {
			t.Fatalf("ambiguous details accepted: %+v", r)
		}
	}
}
