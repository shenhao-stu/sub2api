package xai

import (
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
)

// IncludeIndependentReasoningTokens adds reasoning tokens to billed output
// only when total_tokens proves they are not already folded into output.
//
// Official xAI Chat Completions example: prompt=32, completion=9,
// reasoning=94, total=135. Responses example: input=32, output=9,
// reasoning=110, total=151. OpenAI's canonical completion_tokens already
// includes reasoning and total equals input+output.
func IncludeIndependentReasoningTokens(input, output, total, reasoning int64) int64 {
	if input < 0 || output < 0 || reasoning <= 0 || total <= 0 {
		return output
	}
	// Subtraction after bounds checks avoids overflow and does not turn an
	// unrelated or partial total into evidence of independent reasoning.
	if input > total || output > total-input || total-input-output != reasoning {
		return output
	}
	return total - input
}

// IncludeIndependentReasoningFromUsage leaves the caller's observed output
// unchanged unless explicit, unambiguous wire counters prove the adjustment.
// In particular, a missing counter is not an explicit zero.
func IncludeIndependentReasoningFromUsage(usage gjson.Result, input, output int64) int64 {
	if !usage.Get("output_tokens_details.reasoning_tokens").Exists() && !usage.Get("completion_tokens_details.reasoning_tokens").Exists() {
		return output
	}
	fields, ok := usageFields(usage)
	if !ok {
		return output
	}
	sourceInput, inputOK := usageCounter(fields, "input_tokens", "prompt_tokens")
	sourceOutput, outputOK := usageCounter(fields, "output_tokens", "completion_tokens")
	total, totalOK := usageCounter(fields, "total_tokens")
	if !inputOK || !outputOK || !totalOK || sourceInput != input || sourceOutput != output {
		return output
	}
	var reasoning int64
	seen := false
	for _, name := range []string{"output_tokens_details", "completion_tokens_details"} {
		detail, exists := fields[name]
		if !exists || detail.Type == gjson.Null {
			continue
		}
		members, valid := usageFields(detail)
		if !valid {
			return output
		}
		if _, exists := members["reasoning_tokens"]; !exists {
			continue
		}
		count, valid := usageCounter(members, "reasoning_tokens")
		if !valid || (seen && count != reasoning) {
			return output
		}
		reasoning, seen = count, true
	}
	if !seen {
		return output
	}
	return IncludeIndependentReasoningTokens(input, output, total, reasoning)
}

func usageFields(value gjson.Result) (map[string]gjson.Result, bool) {
	if !value.IsObject() || !gjson.Valid(value.Raw) {
		return nil, false
	}
	fields := make(map[string]gjson.Result)
	valid := true
	value.ForEach(func(key, value gjson.Result) bool {
		name := strings.ToLower(key.String())
		switch name {
		case "input_tokens", "prompt_tokens", "output_tokens", "completion_tokens", "total_tokens",
			"output_tokens_details", "completion_tokens_details", "reasoning_tokens":
		default:
			return true
		}
		// Do not choose between duplicate or case-variant counter locations.
		if _, exists := fields[name]; exists || name != key.String() {
			valid = false
			return false
		}
		fields[name] = value
		return true
	})
	return fields, valid
}

func usageCounter(fields map[string]gjson.Result, names ...string) (int64, bool) {
	var count int64
	seen := false
	for _, name := range names {
		value, exists := fields[name]
		if !exists {
			continue
		}
		parsed, err := strconv.ParseInt(value.Raw, 10, 64)
		if value.Type != gjson.Number || err != nil || parsed < 0 || (seen && parsed != count) {
			return 0, false
		}
		count, seen = parsed, true
	}
	return count, seen
}
