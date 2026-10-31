package service

import (
	"encoding/json"
	"math"
	"strings"

	"github.com/tidwall/gjson"
)

// Only a complete, consistent upstream observation can establish a cache TTL.
// This evidence is private until a policy failure needs a converted wire meter.
type policyCacheCreationSplit struct {
	total int
	Five  int `json:"ephemeral_5m_input_tokens"`
	Hour  int `json:"ephemeral_1h_input_tokens"`
}

func policyUsageObject(raw []byte) (map[string]json.RawMessage, bool) {
	if !json.Valid(raw) || !gjson.ParseBytes(raw).IsObject() {
		return nil, false
	}
	fields := make(map[string]json.RawMessage)
	valid := true
	gjson.ParseBytes(raw).ForEach(func(key, value gjson.Result) bool {
		name := strings.ToLower(key.String())
		if _, exists := fields[name]; exists {
			valid = false
			return false
		}
		fields[name] = json.RawMessage(value.Raw)
		return true
	})
	return fields, valid
}

func observePolicyCacheEvent(usage *ClaudeUsage, raw []byte) {
	if usage == nil {
		return
	}
	root, ok := policyUsageObject(raw)
	if !ok {
		usage.policyCacheCreation = nil
		return
	}
	var kind string
	_ = json.Unmarshal(root["type"], &kind)
	if kind == "message_start" {
		root, ok = policyUsageObject(root["message"])
		if !ok {
			usage.policyCacheCreation = nil
			return
		}
		usage.policyCacheCreation = nil
	}
	rawUsage, exists := root["usage"]
	if !exists {
		return
	}
	fields, ok := policyUsageObject(rawUsage)
	if !ok {
		usage.policyCacheCreation = nil
		return
	}
	if fields["cache_creation_input_tokens"] == nil && fields["cache_creation"] == nil {
		return
	}
	usage.policyCacheCreation = nil
	buckets, ok := policyUsageObject(fields["cache_creation"])
	if !ok {
		return
	}
	values := []int{}
	for _, rawCount := range []json.RawMessage{fields["cache_creation_input_tokens"], buckets["ephemeral_5m_input_tokens"], buckets["ephemeral_1h_input_tokens"]} {
		var count *int
		if json.Unmarshal(rawCount, &count) != nil || count == nil || *count < 0 {
			return
		}
		values = append(values, *count)
	}
	total, five, hour := values[0], values[1], values[2]
	if five > math.MaxInt-hour || five+hour != total {
		return
	}
	usage.policyCacheCreation = &policyCacheCreationSplit{total: total, Five: five, Hour: hour}
}

func knownPolicyCacheCreation(usage *ClaudeUsage) *policyCacheCreationSplit {
	if usage == nil || usage.policyCacheCreation == nil {
		return nil
	}
	split := usage.policyCacheCreation
	if split.total != usage.CacheCreationInputTokens || split.Five > math.MaxInt-split.Hour || split.Five+split.Hour != split.total {
		return nil
	}
	return split
}

// Normal successful adapters retain their existing behavior. Only a rejected
// request restores the original observed split into the partial accounting result.
func restorePolicyCacheCreation(usage *ClaudeUsage) {
	if split := knownPolicyCacheCreation(usage); split != nil {
		usage.CacheCreation5mTokens, usage.CacheCreation1hTokens = split.Five, split.Hour
	}
}

func policyClaudeUsageToOpenAIUsage(usage *ClaudeUsage) OpenAIUsage {
	converted := claudeUsageToOpenAIUsage(usage)
	if split := knownPolicyCacheCreation(usage); split != nil {
		converted.CacheCreation5mTokens, converted.CacheCreation1hTokens = split.Five, split.Hour
		converted.CacheCreationTTLKnown = true
		converted.policyCacheCreation = split
	}
	return converted
}

func hasObservedInputCache(usage *ClaudeUsage) bool {
	return usage != nil && (usage.InputTokens > 0 || usage.CacheReadInputTokens > 0 || usage.CacheCreationInputTokens > 0 || usage.CacheCreation5mTokens > 0 || usage.CacheCreation1hTokens > 0)
}

// Only the policy error writer exposes private TTL evidence. JSON decoding into
// the shared OpenAIUsage type cannot activate this additional wire contract.
func policyOpenAIWireUsage(usage *OpenAIUsage) any {
	if usage == nil || usage.policyCacheCreation == nil {
		return usage
	}
	return struct {
		*OpenAIUsage
		CacheCreation *policyCacheCreationSplit `json:"cache_creation"`
	}{usage, usage.policyCacheCreation}
}
