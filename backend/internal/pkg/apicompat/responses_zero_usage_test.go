package apicompat

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"
)

func TestResponsesToChatZeroUsagePresence(t *testing.T) {
	for _, tc := range []struct {
		name, usage string
		present     bool
	}{
		{"complete_zero", `{"input_tokens":0,"output_tokens":0,"total_tokens":0}`, true},
		{"chat_alias_zero", `{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}`, true},
		{"positive", `{"input_tokens":100,"output_tokens":2,"input_tokens_details":{"cached_tokens":90}}`, true},
		{"positive_partial", `{"output_tokens":2}`, true},
		{"missing", "", false}, {"null", `null`, false}, {"empty", `{}`, false},
		{"input_only", `{"input_tokens":0}`, false}, {"output_only", `{"output_tokens":0}`, false},
		{"total_only", `{"total_tokens":0}`, false},
		{"inconsistent_total", `{"input_tokens":0,"output_tokens":0,"total_tokens":100}`, false},
		{"inconsistent_reasoning", `{"input_tokens":0,"output_tokens":0,"output_tokens_details":{"reasoning_tokens":2}}`, false},
		{"null_total", `{"input_tokens":0,"output_tokens":0,"total_tokens":null}`, false},
		{"null_reasoning", `{"input_tokens":0,"output_tokens":0,"output_tokens_details":{"reasoning_tokens":null}}`, false},
		{"duplicate_input", `{"input_tokens":12,"input_tokens":0,"output_tokens":0}`, false},
		{"duplicate_cache", `{"input_tokens":0,"output_tokens":0,"input_tokens_details":{"cached_tokens":12,"cached_tokens":0}}`, false},
		{"case_variant_counter", `{"input_tokens":0,"output_tokens":0,"TOTAL_TOKENS":12}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"synthetic answer"}]}]`
			if tc.usage != "" {
				body += `,"usage":` + tc.usage
			}
			body += `}`
			var response ResponsesResponse
			if err := json.Unmarshal([]byte(body), &response); err != nil {
				t.Fatal(err)
			}
			converted := ResponsesToChatCompletions(&response, "gemini-4-argon")
			if (converted.Usage != nil) != tc.present {
				t.Errorf("json usage=%+v want present=%v", converted.Usage, tc.present)
			}
			state := NewResponsesEventToChatState()
			state.IncludeUsage = true
			chunks := ResponsesEventToChatChunks(&ResponsesStreamEvent{Type: "response.completed", Response: &response}, state)
			var usage *ChatUsage
			for _, chunk := range chunks {
				if chunk.Usage != nil {
					usage = chunk.Usage
				}
			}
			if (usage != nil) != tc.present {
				t.Errorf("stream usage=%+v want present=%v", usage, tc.present)
			}
		})
	}
}

func TestResponsesToChatLateZeroKeepsPositiveUsage(t *testing.T) {
	state := NewResponsesEventToChatState()
	state.IncludeUsage = true
	state.Usage = &ChatUsage{PromptTokens: 100, CompletionTokens: 2, TotalTokens: 102}
	var response ResponsesResponse
	if err := json.Unmarshal([]byte(`{"status":"completed","usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}`), &response); err != nil {
		t.Fatal(err)
	}
	chunks := ResponsesEventToChatChunks(&ResponsesStreamEvent{Type: "response.completed", Response: &response}, state)
	if len(chunks) != 2 || chunks[1].Usage == nil || chunks[1].Usage.PromptTokens != 100 || chunks[1].Usage.CompletionTokens != 2 {
		t.Fatalf("positive usage erased: %+v", state.Usage)
	}
}

func TestResponsesToChatRetainsRawCounters(t *testing.T) {
	for _, tc := range []struct {
		name, first, last    string
		input, output, cache int
	}{
		{"positive_then_zero", `{"input_tokens":100,"output_tokens":50}`, `{"input_tokens":0,"output_tokens":0}`, 100, 50, 0},
		{"partial_positive_terminal", `{"input_tokens":100,"output_tokens":50}`, `{"input_tokens":110,"output_tokens":0}`, 110, 50, 0},
		{"cache_reclassification", `{"input_tokens":100,"output_tokens":50}`, `{"input_tokens":100,"input_tokens_details":{"cached_tokens":90}}`, 100, 50, 90},
		{"omitted_cache", `{"input_tokens":100,"output_tokens":50,"input_tokens_details":{"cached_tokens":90}}`, `{"input_tokens":110,"output_tokens":0}`, 110, 50, 90},
		{"complete_cache_correction", `{"input_tokens":100,"output_tokens":50,"input_tokens_details":{"cached_tokens":90}}`, `{"input_tokens":110,"output_tokens":51,"input_tokens_details":{"cached_tokens":0}}`, 110, 51, 0},
		{"zero_output_cache_correction", `{"input_tokens":100,"output_tokens":50,"input_tokens_details":{"cached_tokens":90}}`, `{"input_tokens":110,"output_tokens":0,"input_tokens_details":{"cached_tokens":0}}`, 110, 50, 0},
		{"cache_only", `{"input_tokens":100,"output_tokens":50}`, `{"input_tokens_details":{"cached_tokens":90}}`, 100, 50, 90},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, sameEvent := range []bool{false, true} {
				state := NewResponsesEventToChatState()
				state.IncludeUsage = true
				var first, last ResponsesUsage
				if err := json.Unmarshal([]byte(tc.first), &first); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal([]byte(tc.last), &last); err != nil {
					t.Fatal(err)
				}
				final := &ResponsesStreamEvent{Type: "response.completed", Response: &ResponsesResponse{Usage: &last}}
				if sameEvent {
					final.Usage = &first
				} else {
					ResponsesEventToChatChunks(&ResponsesStreamEvent{Type: "response.in_progress", Usage: &first}, state)
				}
				chunks := ResponsesEventToChatChunks(final, state)
				if len(chunks) != 2 || chunks[1].Usage == nil {
					t.Fatalf("missing usage: %+v", chunks)
				}
				got := chunks[1].Usage
				cache := 0
				if got.PromptTokensDetails != nil {
					cache = got.PromptTokensDetails.CachedTokens
				}
				if got.PromptTokens != tc.input || got.CompletionTokens != tc.output || cache != tc.cache {
					t.Fatalf("sameEvent=%v usage=%+v cache=%d", sameEvent, got, cache)
				}
			}
		})
	}
}

func TestResponsesToChatIgnoresStartingZero(t *testing.T) {
	var initial ResponsesUsage
	if err := json.Unmarshal([]byte(`{"input_tokens":0,"output_tokens":0}`), &initial); err != nil {
		t.Fatal(err)
	}
	state := NewResponsesEventToChatState()
	state.IncludeUsage = true
	ResponsesEventToChatChunks(&ResponsesStreamEvent{Type: "response.created", Response: &ResponsesResponse{Usage: &initial}}, state)
	chunks := ResponsesEventToChatChunks(&ResponsesStreamEvent{Type: "response.completed", Response: &ResponsesResponse{}}, state)
	if state.Usage != nil || len(chunks) != 1 {
		t.Fatalf("starting zero became terminal receipt: %+v", chunks)
	}
	converted := ResponsesToChatCompletions(&ResponsesResponse{Status: "in_progress", Usage: &initial}, "synthetic")
	if converted.Usage != nil {
		t.Fatalf("nonterminal JSON zero became receipt: %+v", converted.Usage)
	}
}

func TestResponsesUsageMeteringState(t *testing.T) {
	for _, tc := range []struct{ raw, want string }{
		{`null`, "missing"}, {`{}`, "incomplete"}, {`{"input_tokens":0}`, "incomplete"},
		{`{"input_tokens":0,"output_tokens":0}`, "explicit_zero"}, {`{"output_tokens":2}`, "positive"},
	} {
		var usage *ResponsesUsage
		if err := json.Unmarshal([]byte(tc.raw), &usage); err != nil {
			t.Fatal(err)
		}
		if got := usage.MeteringState(); got != tc.want {
			t.Fatalf("raw=%s got=%s want=%s", tc.raw, got, tc.want)
		}
	}
}

func TestResponsesToChatConflictingTerminalZero(t *testing.T) {
	for _, invalid := range []string{`{}`, `{"input_tokens":0}`, `{"input_tokens":0,"output_tokens":0,"total_tokens":12}`} {
		var event ResponsesStreamEvent
		payload := `{"type":"response.completed","usage":{"input_tokens":0,"output_tokens":0},"response":{"usage":` + invalid + `}}`
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			t.Fatal(err)
		}
		state := NewResponsesEventToChatState()
		state.IncludeUsage = true
		chunks := ResponsesEventToChatChunks(&event, state)
		if state.Usage != nil || len(chunks) != 1 {
			t.Fatalf("ambiguous zero emitted: %+v", chunks)
		}
	}
}

func TestResponsesToChatUnknownOuterCannotCertifyNestedZero(t *testing.T) {
	var evt ResponsesStreamEvent
	if err := json.Unmarshal([]byte(`{"type":"response.completed","usage":{"total_tokens":12},"response":{"usage":{"input_tokens":0,"output_tokens":0}}}`), &evt); err != nil {
		t.Fatal(err)
	}
	state := NewResponsesEventToChatState()
	state.IncludeUsage = true
	chunks := ResponsesEventToChatChunks(&evt, state)
	if state.Usage != nil || len(chunks) != 1 {
		t.Fatalf("unknown counters became zero: %+v", chunks)
	}
}

func TestResponsesToChatMergedTotalOverflowPreservesRealCounters(t *testing.T) {
	state := NewResponsesEventToChatState()
	state.IncludeUsage = true
	var first, last ResponsesStreamEvent
	if err := json.Unmarshal([]byte(fmt.Sprintf(`{"type":"response.in_progress","usage":{"input_tokens":%d,"output_tokens":0}}`, math.MaxInt)), &first); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(`{"type":"response.completed","response":{"usage":{"input_tokens":0,"output_tokens":1}}}`), &last); err != nil {
		t.Fatal(err)
	}
	ResponsesEventToChatChunks(&first, state)
	chunks := ResponsesEventToChatChunks(&last, state)
	usage := chunks[len(chunks)-1].Usage
	if usage == nil || usage.PromptTokens != math.MaxInt || usage.CompletionTokens != 1 {
		t.Fatalf("real counters erased: %+v", usage)
	}
	wire, err := json.Marshal(usage)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(wire, &fields); err != nil {
		t.Fatal(err)
	}
	if _, exists := fields["total_tokens"]; exists {
		t.Fatalf("unrepresentable total must be absent, not fabricated: %s", wire)
	}
}

func TestResponsesToChatFreshTotalOverflowAndNormalZero(t *testing.T) {
	for _, tc := range []struct {
		input, output int
		totalPresent  bool
	}{{math.MaxInt, 1, false}, {0, 0, true}, {100, 2, true}} {
		var response ResponsesResponse
		if err := json.Unmarshal([]byte(fmt.Sprintf(`{"status":"completed","usage":{"input_tokens":%d,"output_tokens":%d}}`, tc.input, tc.output)), &response); err != nil {
			t.Fatal(err)
		}
		got := ResponsesToChatCompletions(&response, "synthetic")
		if got.Usage == nil || got.Usage.PromptTokens != tc.input || got.Usage.CompletionTokens != tc.output {
			t.Fatalf("real counters changed: %+v", got.Usage)
		}
		data, err := json.Marshal(got.Usage)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(data, &fields); err != nil {
			t.Fatal(err)
		}
		_, exists := fields["total_tokens"]
		if exists != tc.totalPresent {
			t.Fatalf("unexpected total evidence: %s", data)
		}
		var roundtrip ChatUsage
		if err := json.Unmarshal(data, &roundtrip); err != nil {
			t.Fatal(err)
		}
		if roundtrip.PromptTokens != tc.input || roundtrip.CompletionTokens != tc.output {
			t.Fatalf("wire became false zero: %+v", roundtrip)
		}
	}
}

func TestResponsesUnknownCacheMetadataDoesNotCorrectWriteUsage(t *testing.T) {
	var raw ResponsesUsage
	if err := json.Unmarshal([]byte(`{"input_tokens":110,"output_tokens":2,"cache_write_time":0,"input_tokens_details":{"cache_write_input_tokens":0}}`), &raw); err != nil {
		t.Fatal(err)
	}
	if read, write := raw.TerminalCacheCorrections(); read || write {
		t.Fatal("unknown metadata became cache correction")
	}
}

func TestResponsesDuplicateReceiptCannotCertifyZero(t *testing.T) {
	for _, body := range []string{
		`{"type":"response.completed","usage":{"input_tokens":100,"output_tokens":5},"usage":{"input_tokens":0,"output_tokens":0}}`,
		`{"type":"response.completed","response":{"usage":{"input_tokens":100,"output_tokens":5}},"response":{"usage":{"input_tokens":0,"output_tokens":0}}}`,
		`{"type":"response.completed","Usage":{"input_tokens":100,"output_tokens":5},"usage":{"input_tokens":0,"output_tokens":0}}`,
	} {
		var evt ResponsesStreamEvent
		if err := json.Unmarshal([]byte(body), &evt); err != nil {
			t.Fatal(err)
		}
		state := NewResponsesEventToChatState()
		state.IncludeUsage = true
		for _, chunk := range ResponsesEventToChatChunks(&evt, state) {
			if chunk.Usage != nil && !chatUsageHasTokens(chunk.Usage) {
				t.Fatal("ambiguous receipt certified zero")
			}
		}
		if evt.TerminalMeteringState() != "incomplete" {
			t.Fatalf("ambiguity misreported: %s", evt.TerminalMeteringState())
		}
	}
	var response ResponsesResponse
	if err := json.Unmarshal([]byte(`{"status":"completed","usage":{"input_tokens":100,"output_tokens":5},"usage":{"input_tokens":0,"output_tokens":0}}`), &response); err != nil {
		t.Fatal(err)
	}
	if got := ResponsesToChatCompletions(&response, "synthetic"); got.Usage != nil && !chatUsageHasTokens(got.Usage) {
		t.Fatal("JSON duplicate receipt certified zero")
	}
}

func TestResponsesInvalidCountersCannotCorrectCache(t *testing.T) {
	for _, raw := range []string{`{"input_tokens":100,"output_tokens":-1,"input_tokens_details":{"cached_tokens":0}}`, `{"input_tokens":100,"output_tokens":1,"total_tokens":null,"input_tokens_details":{"cached_tokens":0}}`} {
		var usage ResponsesUsage
		if err := json.Unmarshal([]byte(raw), &usage); err != nil {
			t.Fatal(err)
		}
		if read, write := usage.TerminalCacheCorrections(); read || write {
			t.Fatal("invalid receipt corrected cache")
		}
	}
}

func TestResponsesCacheWriteAliasChangeDoesNotKeepStaleAlias(t *testing.T) {
	state := NewResponsesEventToChatState()
	state.IncludeUsage = true
	var first, last ResponsesStreamEvent
	if err := json.Unmarshal([]byte(`{"type":"response.in_progress","usage":{"input_tokens":100,"output_tokens":1,"input_tokens_details":{"cache_write_tokens":10}}}`), &first); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(`{"type":"response.completed","response":{"usage":{"input_tokens":110,"output_tokens":2,"input_tokens_details":{"cache_creation_tokens":20}}}}`), &last); err != nil {
		t.Fatal(err)
	}
	ResponsesEventToChatChunks(&first, state)
	ResponsesEventToChatChunks(&last, state)
	details := state.Usage.PromptTokensDetails
	if details == nil || details.CacheWriteTokens != 0 || details.CacheCreationTokens != 20 {
		t.Fatalf("stale alias changes cache price: %+v", details)
	}
}
