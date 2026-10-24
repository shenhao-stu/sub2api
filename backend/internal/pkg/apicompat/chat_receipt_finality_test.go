package apicompat

import (
	"encoding/json"
	"testing"
)

func TestChatReceiptSourceFinality(t *testing.T) {
	for _, tc := range []struct {
		name, payload string
		want          bool
	}{
		{"zero", `{"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":0,"output_tokens":0}}}`, true},
		{"positive", `{"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":10,"output_tokens":5}}}`, true},
		{"failed", `{"type":"response.failed","response":{"status":"failed","usage":{"input_tokens":10,"output_tokens":5}}}`, true},
		{"initial", `{"type":"response.created","response":{"usage":{"input_tokens":0,"output_tokens":0}}}`, false},
		{"partial", `{"type":"response.completed","response":{"usage":{"input_tokens":10}}}`, false},
		{"duplicate_outer", `{"type":"response.completed","usage":{"input_tokens":10,"output_tokens":5},"usage":{"input_tokens":0,"output_tokens":0}}`, false},
		{"duplicate_counter", `{"type":"response.completed","usage":{"input_tokens":10,"input_tokens":0,"output_tokens":0}}`, false},
		{"conflicting_locations", `{"type":"response.completed","usage":{"input_tokens":10,"output_tokens":5},"response":{"usage":{"input_tokens":0,"output_tokens":0}}}`, false},
		{"wrong_terminal", `{"type":"response.completed","response":{"status":"failed","usage":{"input_tokens":10,"output_tokens":5}}}`, false},
		{"null_aux", `{"type":"response.completed","usage":{"input_tokens":10,"output_tokens":5,"total_tokens":null}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var event ResponsesStreamEvent
			if err := json.Unmarshal([]byte(tc.payload), &event); err != nil {
				t.Fatal(err)
			}
			if got := event.CompleteReceiptMetering(); got != tc.want {
				t.Fatalf("complete=%v want=%v", got, tc.want)
			}
		})
	}
}
