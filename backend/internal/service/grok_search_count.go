package service

import (
	"github.com/tidwall/gjson"
	"strconv"
	"strings"
)

// The same meter serves JSON, SSE and protocol conversion. A tool declaration
// or client function is not evidence of an executed native search.
func countGrokNativeSearchCallsFromJSONBytes(body []byte) int {
	return countGrokNativeSearchCallsInSSEDataDedup(body, make(map[string]struct{}))
}
func countGrokNativeSearchCallsFromSSEBody(body string) int {
	seen := make(map[string]struct{})
	total := 0
	forEachOpenAISSEDataPayload(body, func(data []byte) { total += countGrokNativeSearchCallsInSSEDataDedup(data, seen) })
	return total
}
func countGrokNativeSearchCallsInSSEData(data []byte) int {
	return countGrokNativeSearchCallsInSSEDataDedup(data, make(map[string]struct{}))
}

// ID, call ID and output index are aliases of one invocation. Missing IDs are
// safe when the provider gives an output index; otherwise wait for final output.
// Identical unindexed item.done events cannot prove distinct executions.
func countGrokNativeSearchCallsInSSEDataDedup(data []byte, seen map[string]struct{}) int {
	if !gjson.ValidBytes(data) {
		return 0
	}
	if seen == nil {
		seen = make(map[string]struct{})
	}
	total := 0
	add := func(item gjson.Result, index int) {
		if !isGrokNativeSearchOutputItem(item) {
			return
		}
		keys := make([]string, 0, 3)
		for _, field := range []string{"id", "call_id"} {
			if id := strings.TrimSpace(item.Get(field).String()); id != "" {
				keys = append(keys, field+":"+id)
			}
		}
		if index >= 0 {
			keys = append(keys, "index:"+strconv.Itoa(index))
		}
		if len(keys) == 0 {
			return
		}
		known := false
		for _, key := range keys {
			if _, ok := seen[key]; ok {
				known = true
			}
		}
		if !known {
			total++
		}
		for _, key := range keys {
			seen[key] = struct{}{}
		}
	}
	switch strings.TrimSpace(gjson.GetBytes(data, "type").String()) {
	case "response.output_item.done":
		index := -1
		if raw := gjson.GetBytes(data, "output_index"); raw.Type == gjson.Number && raw.Num >= 0 && raw.Num == float64(int(raw.Num)) {
			index = int(raw.Num)
		}
		add(gjson.GetBytes(data, "item"), index)
	case "", "response.completed", "response.done", "response.failed", "response.incomplete":
		output := gjson.GetBytes(data, "response.output")
		if !output.IsArray() {
			output = gjson.GetBytes(data, "output")
		}
		if output.IsArray() {
			for index, item := range output.Array() {
				add(item, index)
			}
		}
	}
	return total
}

func isGrokNativeSearchOutputItem(item gjson.Result) bool {
	status := strings.ToLower(strings.TrimSpace(item.Get("status").String()))
	// Completion events can omit per-item status. Explicit non-success is never billed.
	if status != "" && status != "completed" {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(item.Get("type").String())) {
	case "web_search_call", "x_search_call", "tool_search_call":
		return true
	default:
		return false
	}
}
