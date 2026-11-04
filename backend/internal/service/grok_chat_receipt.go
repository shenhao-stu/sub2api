package service

import (
	"encoding/json"
	"math"
	"strconv"
)

// xAI can report visible completion and reasoning separately. Only an explicit,
// independent total proving that split allows normalization into receipt output.
func parseGrokExclusiveChatReceiptMeter(data []byte) *UsageReceipt {
	fields, ok := receiptObject(data)
	if !ok {
		return nil
	}
	input, inputOK := receiptCounter(fields, "prompt_tokens")
	visible, visibleOK := receiptCounter(fields, "completion_tokens")
	total, totalOK := receiptCounter(fields, "total_tokens")
	details, detailsOK := receiptObject(fields["completion_tokens_details"])
	reasoning, reasoningOK := receiptCounter(details, "reasoning_tokens")
	if !inputOK || !visibleOK || !totalOK || !detailsOK || !reasoningOK || reasoning <= 0 ||
		visible > math.MaxInt-input || reasoning > math.MaxInt-input-visible || total != input+visible+reasoning {
		return nil
	}
	fields["completion_tokens"] = json.RawMessage(strconv.Itoa(visible + reasoning))
	normalized, err := json.Marshal(fields)
	if err != nil {
		return nil
	}
	return parseChatReceiptMeter(normalized)
}
