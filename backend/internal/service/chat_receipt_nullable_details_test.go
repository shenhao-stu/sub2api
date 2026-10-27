//go:build unit

package service

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestChatReceiptNullableDetailAlias(t *testing.T) {
	for _, tc := range []struct {
		name, details     string
		cached, reasoning int
		valid             bool
	}{
		{"chat_input", `"prompt_tokens_details":{"cached_tokens":90},"input_tokens_details":null`, 90, 0, true},
		{"responses_input", `"prompt_tokens_details":null,"input_tokens_details":{"cached_tokens":90}`, 90, 0, true},
		{"chat_output", `"completion_tokens_details":{"reasoning_tokens":3},"output_tokens_details":null`, 0, 3, true},
		{"responses_output", `"completion_tokens_details":null,"output_tokens_details":{"reasoning_tokens":3}`, 0, 3, true},
		{"explicit_zero", `"prompt_tokens_details":{"cached_tokens":0},"input_tokens_details":null`, 0, 0, true},
		{"no_counter", `"prompt_tokens_details":{},"input_tokens_details":null`, 0, 0, false},
		{"only_null", `"input_tokens_details":null`, 0, 0, false},
		{"both_null", `"prompt_tokens_details":null,"input_tokens_details":null`, 0, 0, false},
		{"invalid_counter", `"prompt_tokens_details":{"cached_tokens":null},"input_tokens_details":null`, 0, 0, false},
		{"conflicting_counts", `"prompt_tokens_details":{"cached_tokens":90},"input_tokens_details":{"cached_tokens":0}`, 0, 0, false},
		{"negative_counter", `"prompt_tokens_details":{"cached_tokens":-1},"input_tokens_details":null`, 0, 0, false},
		{"duplicate_counter", `"prompt_tokens_details":{"cached_tokens":90,"cached_tokens":0},"input_tokens_details":null`, 0, 0, false},
		{"duplicate_detail", `"prompt_tokens_details":{"cached_tokens":90},"input_tokens_details":{},"input_tokens_details":null`, 0, 0, false},
		{"cache_underflow", `"prompt_tokens_details":{"cached_tokens":101},"input_tokens_details":null`, 0, 0, false},
		{"wrong_shape", `"prompt_tokens_details":{"cached_tokens":90},"input_tokens_details":[]`, 0, 0, false},
		{"unrepresented_audio", `"prompt_tokens_details":{"cached_tokens":90,"audio_tokens":1},"input_tokens_details":null`, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			meter := parseChatReceiptMeter([]byte(`{"prompt_tokens":100,"completion_tokens":5,"total_tokens":105,` + tc.details + `}`))
			if (meter != nil) != tc.valid {
				t.Fatalf("meter=%+v, valid=%v", meter, tc.valid)
			}
			if meter != nil && (meter.InputTokens != 100-tc.cached || meter.OutputTokens != 5 || meter.CacheReadTokens != tc.cached || meter.ReasoningTokens != tc.reasoning) {
				t.Fatalf("meter changed explicit counters: %+v", meter)
			}
		})
	}
}

func TestChatReceiptRawNullableAliasFinality(t *testing.T) {
	capture := &chatReceiptCapture{}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", ChatReceiptEndpoint, nil).WithContext(context.WithValue(context.Background(), chatReceiptContextKey{}, capture))
	for _, frame := range []string{
		`{"choices":[{"delta":{"role":"assistant"}}],"usage":null}`,
		`{"choices":[{"delta":{"content":"OK"}}]}`,
		`{"choices":[{"finish_reason":"stop"}]}`,
		`{"choices":[],"usage":{"prompt_tokens":35,"completion_tokens":5,"total_tokens":40,"prompt_tokens_details":{"cached_tokens":0,"text_tokens":35,"audio_tokens":0,"image_tokens":0},"completion_tokens_details":{"text_tokens":0,"audio_tokens":0,"reasoning_tokens":0},"input_tokens":0,"output_tokens":0,"input_tokens_details":null,"claude_cache_creation_5_m_tokens":0,"claude_cache_creation_1_h_tokens":0}}`,
	} {
		observeRawChatReceipt(c, []byte(frame), true)
	}
	if capture.meter != nil {
		t.Fatal("receipt finalized before DONE")
	}
	observeRawChatReceipt(c, []byte(`[DONE]`), true)
	if capture.meter == nil || capture.meter.InputTokens != 35 || capture.meter.OutputTokens != 5 || capture.terminal != "completed" {
		t.Fatalf("final receipt missing or changed: %+v", capture)
	}
}
