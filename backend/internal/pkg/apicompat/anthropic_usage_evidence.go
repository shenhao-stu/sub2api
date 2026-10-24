package apicompat

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"
)

// AnthropicUsageEvidence retains wire presence independently of normalized counters.
// Initial output counts are not a final receipt; message_delta is cumulative.
type AnthropicUsageEvidence struct {
	inputSeen, finalOutput, invalid, nonzeroSeen bool
}

func uniqueUsageObject(data []byte) (map[string]json.RawMessage, bool) {
	d := json.NewDecoder(bytes.NewReader(data))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return nil, false
	}
	fields := make(map[string]json.RawMessage)
	for d.More() {
		token, err = d.Token()
		name, ok := token.(string)
		if err != nil || !ok {
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
	_, err = d.Token()
	return fields, err == nil && json.Valid(data)
}

func (e AnthropicUsageEvidence) observeUsage(data []byte, terminal bool) AnthropicUsageEvidence {
	if len(data) == 0 || bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return e
	}
	fields, valid := uniqueUsageObject(data)
	if !valid || !responsesZeroCountersValidObject(data, false, false) {
		e.invalid = true
		return e
	}
	if nested, exists := fields["cache_creation"]; exists && !responsesZeroCountersValidObject(nested, true, true) {
		e.invalid = true
	}
	e.nonzeroSeen = e.nonzeroSeen || !responsesZeroCountersValid(data)
	e.inputSeen = e.inputSeen || fields["input_tokens"] != nil
	e.finalOutput = e.finalOutput || (terminal && fields["output_tokens"] != nil)
	return e
}

// ObserveEventJSON must see the original event, including ones the typed decoder
// rejects, so an invalid late receipt cannot certify earlier placeholder zeros.
func (e AnthropicUsageEvidence) ObserveEventJSON(data []byte) AnthropicUsageEvidence {
	fields, valid := uniqueUsageObject(data)
	if !valid {
		e.invalid = true
		return e
	}
	var kind string
	if json.Unmarshal(fields["type"], &kind) != nil {
		e.invalid = true
		return e
	}
	switch kind {
	case "message_start":
		message, ok := uniqueUsageObject(fields["message"])
		if !ok {
			e.invalid = true
			return e
		}
		return e.observeUsage(message["usage"], false)
	case "message_delta":
		if _, ok := uniqueUsageObject(fields["delta"]); !ok {
			e.invalid = true
		}
		var delta struct {
			StopReason string `json:"stop_reason"`
		}
		terminal := json.Unmarshal(fields["delta"], &delta) == nil && delta.StopReason != ""
		if terminal {
			e.finalOutput = false
		}
		return e.observeUsage(fields["usage"], terminal)
	case "content_block_start", "content_block_delta":
		e.finalOutput = false
	}
	return e
}

func (e AnthropicUsageEvidence) ResponsesUsage(u AnthropicUsage) *ResponsesUsage {
	total := 0
	for _, count := range []int{u.InputTokens, u.CacheReadInputTokens, u.CacheCreationInputTokens, u.OutputTokens} {
		if count < 0 || total > math.MaxInt-count {
			return nil
		}
		total += count
	}
	input := total - u.OutputTokens
	complete := e.inputSeen && e.finalOutput && !e.invalid
	if total == 0 && (!complete || e.nonzeroSeen) {
		return nil
	}
	result := &ResponsesUsage{
		explicitTokenTotals: complete, receiptCountersValid: complete,
		zeroTokenCountersValid: complete && !e.nonzeroSeen,
		InputTokens:            input, OutputTokens: u.OutputTokens, TotalTokens: total,
		CacheCreationInputTokens: u.CacheCreationInputTokens,
	}
	if u.CacheReadInputTokens > 0 {
		result.InputTokensDetails = &ResponsesInputTokensDetails{CachedTokens: u.CacheReadInputTokens}
	}
	return result
}
