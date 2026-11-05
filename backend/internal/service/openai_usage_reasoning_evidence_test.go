package service

import (
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"testing"
)

func TestOpenAIUsageReasoningRequiresCompleteEvidence(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name, raw string
		output    int
	}{
		{"inclusive", `{"input_tokens":100,"output_tokens":8,"total_tokens":108,"output_tokens_details":{"reasoning_tokens":3}}`, 8},
		{"exclusive", `{"input_tokens":100,"output_tokens":5,"total_tokens":108,"output_tokens_details":{"reasoning_tokens":3}}`, 8},
		{"chat_exclusive", `{"prompt_tokens":32,"completion_tokens":9,"total_tokens":135,"completion_tokens_details":{"reasoning_tokens":94}}`, 103},
		{"missing_prompt", `{"output_tokens":8,"total_tokens":108,"output_tokens_details":{"reasoning_tokens":3}}`, 8},
		{"missing_prompt_exact_gap", `{"output_tokens":8,"total_tokens":11,"output_tokens_details":{"reasoning_tokens":3}}`, 8},
		{"missing_output", `{"input_tokens":100,"total_tokens":103,"output_tokens_details":{"reasoning_tokens":3}}`, 0},
		{"zero_output", `{"input_tokens":100,"output_tokens":0,"total_tokens":103,"output_tokens_details":{"reasoning_tokens":3}}`, 3},
		{"zero_input", `{"input_tokens":0,"output_tokens":8,"total_tokens":11,"output_tokens_details":{"reasoning_tokens":3}}`, 11},
		{"null_detail_alias", `{"input_tokens":100,"output_tokens":5,"total_tokens":108,"output_tokens_details":{"reasoning_tokens":3},"completion_tokens_details":null}`, 8},
		{"unrelated_extension", `{"input_tokens":100,"output_tokens":5,"total_tokens":108,"output_tokens_details":{"reasoning_tokens":3},"vendorMetadata":{"source":"synthetic"}}`, 8},
		{"case_variant_counter", `{"input_tokens":100,"INPUT_TOKENS":99,"output_tokens":5,"total_tokens":108,"output_tokens_details":{"reasoning_tokens":3}}`, 5},
		{"string_total", `{"input_tokens":100,"output_tokens":5,"total_tokens":"108","output_tokens_details":{"reasoning_tokens":3}}`, 5},
		{"fractional_reasoning", `{"input_tokens":100,"output_tokens":5,"total_tokens":108,"output_tokens_details":{"reasoning_tokens":3.5}}`, 5},
		{"negative_input", `{"input_tokens":-1,"output_tokens":5,"total_tokens":7,"output_tokens_details":{"reasoning_tokens":3}}`, 5},
		{"missing_total", `{"input_tokens":100,"output_tokens":8,"output_tokens_details":{"reasoning_tokens":3}}`, 8},
		{"larger_gap", `{"input_tokens":100,"output_tokens":8,"total_tokens":112,"output_tokens_details":{"reasoning_tokens":3}}`, 8},
		{"smaller_gap", `{"input_tokens":100,"output_tokens":8,"total_tokens":110,"output_tokens_details":{"reasoning_tokens":3}}`, 8},
		{"null_input", `{"input_tokens":null,"output_tokens":8,"total_tokens":11,"output_tokens_details":{"reasoning_tokens":3}}`, 8},
		{"string_input", `{"input_tokens":"100","output_tokens":5,"total_tokens":108,"output_tokens_details":{"reasoning_tokens":3}}`, 5},
		{"fractional_input", `{"input_tokens":100.5,"output_tokens":5,"total_tokens":108,"output_tokens_details":{"reasoning_tokens":3}}`, 5},
		{"duplicate_input", `{"input_tokens":100,"input_tokens":101,"output_tokens":5,"total_tokens":108,"output_tokens_details":{"reasoning_tokens":3}}`, 5},
		{"conflicting_alias", `{"input_tokens":100,"prompt_tokens":101,"output_tokens":5,"total_tokens":108,"output_tokens_details":{"reasoning_tokens":3}}`, 5},
		{"matching_alias", `{"input_tokens":100,"prompt_tokens":100,"output_tokens":5,"total_tokens":108,"output_tokens_details":{"reasoning_tokens":3}}`, 8},
		{"duplicate_reasoning", `{"input_tokens":100,"output_tokens":5,"total_tokens":108,"output_tokens_details":{"reasoning_tokens":3,"reasoning_tokens":4}}`, 5},
		{"conflicting_reasoning_alias", `{"input_tokens":100,"output_tokens":5,"total_tokens":108,"output_tokens_details":{"reasoning_tokens":3},"completion_tokens_details":{"reasoning_tokens":4}}`, 5},
		{"matching_reasoning_alias", `{"input_tokens":100,"output_tokens":5,"total_tokens":108,"output_tokens_details":{"reasoning_tokens":3},"completion_tokens_details":{"reasoning_tokens":3}}`, 8},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			u, ok := openAIUsageFromGJSON(gjson.Parse(tt.raw))
			require.True(t, ok)
			require.Equal(t, tt.output, u.OutputTokens)
		})
	}
}
