package commandcode

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"
	"strings"
)

func convertUsage(raw json.RawMessage) (map[string]any, error) {
	var values map[string]any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if d.Decode(&values) != nil || values == nil {
		return nil, ErrUsage
	}
	read := func(paths ...string) (int64, bool, error) { return usageNumber(values, paths...) }
	input, hasInput, err := read("inputTokens", "input_tokens", "prompt_tokens")
	if err != nil || !hasInput {
		return nil, ErrUsage
	}
	output, hasOutput, err := read("outputTokens", "output_tokens", "completion_tokens")
	if err != nil || !hasOutput {
		return nil, ErrUsage
	}
	total, hasTotal, err := read("totalTokens", "total_tokens")
	if err != nil || (hasTotal && total != input+output) {
		return nil, ErrUsage
	}
	cacheRead, hasRead, err := read("cachedInputTokens", "cache_read_input_tokens", "inputTokenDetails.cacheReadTokens", "inputTokens.cacheRead", "prompt_tokens_details.cached_tokens")
	if err != nil {
		return nil, ErrUsage
	}
	cacheWrite, hasWrite, err := read("cache_creation_input_tokens", "inputTokenDetails.cacheWriteTokens", "inputTokens.cacheWrite", "prompt_tokens_details.cache_creation_tokens")
	if err != nil || cacheRead+cacheWrite > input {
		return nil, ErrUsage
	}
	noCache, hasNoCache, err := read("inputTokenDetails.noCacheTokens", "inputTokens.noCache")
	if err != nil || (hasNoCache && noCache+cacheRead+cacheWrite > input) {
		return nil, ErrUsage
	}
	reasoning, hasReasoning, err := read("reasoningTokens", "outputTokenDetails.reasoningTokens", "outputTokens.reasoning", "completion_tokens_details.reasoning_tokens")
	if err != nil || reasoning > output {
		return nil, ErrUsage
	}
	result := map[string]any{"prompt_tokens": input, "completion_tokens": output, "total_tokens": input + output}
	if hasRead || hasWrite {
		details := map[string]any{}
		if hasRead {
			details["cached_tokens"] = cacheRead
		}
		if hasWrite {
			details["cache_creation_tokens"] = cacheWrite
			result["cache_creation_input_tokens"] = cacheWrite
		}
		result["prompt_tokens_details"] = details
	}
	if hasReasoning {
		result["completion_tokens_details"] = map[string]any{"reasoning_tokens": reasoning}
	}
	return result, nil
}

func usageNumber(values map[string]any, paths ...string) (int64, bool, error) {
	var result int64
	found := false
	for _, path := range paths {
		var value any = values
		for _, key := range strings.Split(path, ".") {
			object, ok := value.(map[string]any)
			if !ok {
				value = nil
				break
			}
			value = object[key]
		}
		if value == nil {
			continue
		}
		if object, ok := value.(map[string]any); ok {
			value = object["total"]
		}
		if value == nil {
			continue
		}
		n, ok := value.(json.Number)
		if !ok {
			return 0, false, ErrUsage
		}
		f, err := strconv.ParseFloat(string(n), 64)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || f < 0 || f > 1e12 || math.Trunc(f) != f {
			return 0, false, ErrUsage
		}
		integer := int64(f)
		if found && integer != result {
			return 0, false, ErrUsage
		}
		result, found = integer, true
	}
	return result, found, nil
}
