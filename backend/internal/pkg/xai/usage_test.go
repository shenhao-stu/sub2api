package xai

import (
	"math"
	"testing"
)

func TestIncludeIndependentReasoningTokens(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                         string
		input, output, total, reason int64
		want                         int64
	}{
		{name: "xAI chat example", input: 32, output: 9, total: 135, reason: 94, want: 103},
		{name: "xAI responses example", input: 32, output: 9, total: 151, reason: 110, want: 119},
		{name: "OpenAI inclusive output", input: 32, output: 103, total: 135, reason: 94, want: 103},
		{name: "gap smaller than reasoning", input: 10, output: 20, total: 33, reason: 5, want: 20},
		{name: "gap larger than reasoning", input: 10, output: 20, total: 36, reason: 5, want: 20},
		{name: "overflowing sum", input: math.MaxInt64, output: 2, total: 8, reason: 3, want: 2},
		{name: "largest valid total", input: 0, output: math.MaxInt64 - 3, total: math.MaxInt64, reason: 3, want: math.MaxInt64},
		{name: "explicit zero input", input: 0, output: 2, total: 5, reason: 3, want: 5},
		{name: "explicit zero output", input: 100, output: 0, total: 103, reason: 3, want: 3},
		{name: "absent total", input: 10, output: 20, total: 0, reason: 5, want: 20},
		{name: "total already complete", input: 10, output: 20, total: 30, reason: 5, want: 20},
		{name: "inconsistent undershoot", input: 10, output: 20, total: 25, reason: 5, want: 20},
		{name: "zero reasoning", input: 10, output: 20, total: 35, reason: 0, want: 20},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := IncludeIndependentReasoningTokens(tt.input, tt.output, tt.total, tt.reason)
			if got != tt.want {
				t.Fatalf("got %d, want %d", got, tt.want)
			}
		})
	}
}
