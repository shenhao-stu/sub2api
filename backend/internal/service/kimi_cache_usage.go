package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const kimiCacheMeteringContextKey = "sub2api.kimi_cache_metering"

type kimiCacheWriteBreakdown struct {
	FiveMinutes int `json:"ephemeral_5m_input_tokens"`
	OneHour     int `json:"ephemeral_1h_input_tokens"`
}

// Kimi's response headers describe the actual cache entry lifetime. The current
// request's TTL cannot establish it: an existing cache entry keeps its lifetime.
func kimiCacheWriteHeaders(headers http.Header, aggregate int) (kimiCacheWriteBreakdown, bool) {
	var split kimiCacheWriteBreakdown
	if aggregate < 0 {
		return split, false
	}
	read := func(name string) (int, bool) {
		var values []string
		for key, items := range headers {
			if strings.EqualFold(key, name) {
				values = append(values, items...)
			}
		}
		if len(values) != 1 {
			return 0, false
		}
		value := strings.TrimSpace(values[0])
		if value == "" {
			return 0, false
		}
		for _, c := range value {
			if c < '0' || c > '9' {
				return 0, false
			}
		}
		n, err := strconv.ParseUint(value, 10, strconv.IntSize)
		if err != nil || n > uint64(math.MaxInt) {
			return 0, false
		}
		return int(n), true
	}
	five, ok := read("Msh-Usage-Cache-Write-Tokens-5m")
	if !ok {
		return split, false
	}
	hour, ok := read("Msh-Usage-Cache-Write-Tokens-1h")
	if !ok || hour > math.MaxInt-five || five+hour != aggregate {
		return split, false
	}
	return kimiCacheWriteBreakdown{FiveMinutes: five, OneHour: hour}, true
}

type kimiCacheUsageState struct {
	Aggregate int
	Split     kimiCacheWriteBreakdown
	Known     bool
}

const kimiCacheUsageStateKey = "sub2api.kimi_cache_usage"

func applyKimiCacheWriteHeaders(c *gin.Context, headers http.Header, usage OpenAIUsage, rawEnvelope []byte) OpenAIUsage {
	if c == nil || !c.GetBool(kimiCacheMeteringContextKey) {
		return usage
	}
	usage.CacheCreation5mTokens, usage.CacheCreation1hTokens, usage.CacheCreationTTLKnown = 0, 0, false
	state := kimiCacheUsageState{Aggregate: usage.CacheCreationInputTokens}
	envelope, envelopeOK := kimiUsageObject(rawEnvelope)
	rawUsage := envelope["usage"]
	aggregate, valid := kimiCacheWriteAggregate(rawUsage)
	valid = valid && envelopeOK
	if valid && aggregate == usage.CacheCreationInputTokens && usage.InputTokens >= 0 && usage.CacheReadInputTokens >= 0 && usage.CacheReadInputTokens <= usage.InputTokens && aggregate <= usage.InputTokens-usage.CacheReadInputTokens {
		state.Split, state.Known = kimiCacheWriteEvidence(rawUsage, headers)
	}
	if !state.Known {
		if previous, exists := c.Get(kimiCacheUsageStateKey); exists {
			if prior, ok := previous.(kimiCacheUsageState); ok && prior.Known && prior.Aggregate == state.Aggregate {
				// A malformed or incomplete tail cannot erase earlier exact evidence
				// for the same aggregate. Never attach it to a changed quantity.
				state = prior
			}
		}
	}
	if state.Known {
		usage.CacheCreation5mTokens, usage.CacheCreation1hTokens, usage.CacheCreationTTLKnown = state.Split.FiveMinutes, state.Split.OneHour, true
	}
	// Converted JSON can erase duplicate fields or contradictory provider data.
	// Bind downstream TTL to the raw upstream evidence, never re-infer it from the
	// normalized conversion or from a different usage snapshot.
	c.Set(kimiCacheUsageStateKey, state)
	return usage
}

// Explicit aliases are evidence for the same aggregate, never additive. This
// narrow reader also rejects duplicate or malformed counters before adding TTL.
func kimiCacheWriteAggregate(raw json.RawMessage) (int, bool) {
	fields, ok := kimiUsageObject(raw)
	if !ok {
		return 0, false
	}
	aggregate, seen := 0, false
	merge := func(v json.RawMessage) bool {
		n, valid := receiptInteger(v)
		if !valid || (seen && aggregate != n) {
			return false
		}
		aggregate, seen = n, true
		return true
	}
	for _, name := range []string{"cache_creation_input_tokens", "cache_write_tokens", "cache_creation_tokens"} {
		if value, exists := fields[name]; exists && !merge(value) {
			return 0, false
		}
	}
	for _, details := range []string{"prompt_tokens_details", "input_tokens_details"} {
		if rawDetails, exists := fields[details]; exists {
			nested, valid := kimiUsageObject(rawDetails)
			if !valid {
				return 0, false
			}
			for _, name := range []string{"cache_write_tokens", "cache_creation_tokens"} {
				if value, exists := nested[name]; exists && !merge(value) {
					return 0, false
				}
			}
		}
	}
	return aggregate, seen
}

// Preserve an explicit, validated split in the existing Anthropic usage shape
// on all three downstream protocols. No prompt, response text or URL is added.
func injectKimiCacheWriteUsage(c *gin.Context, body []byte) []byte {
	if c == nil || !c.GetBool(kimiCacheMeteringContextKey) {
		return body
	}
	value, _ := c.Get(kimiCacheUsageStateKey)
	state, hasState := value.(kimiCacheUsageState)
	updateUsage := func(raw json.RawMessage) json.RawMessage {
		aggregate, valid := kimiCacheWriteAggregate(raw)
		clean, ok := stripKimiCacheWriteBreakdown(raw)
		if !ok {
			return raw
		}
		if valid && hasState && state.Known && aggregate == state.Aggregate {
			if updated, err := sjson.SetBytes(clean, "cache_creation", state.Split); err == nil {
				clean = updated
			}
		}
		return clean
	}
	// Preserve duplicate envelope fields for downstream validation while clearing
	// disputed TTL from every occurrence. Parsing into a map would choose a winner.
	rewrite := func(name string, raw json.RawMessage) json.RawMessage {
		if strings.EqualFold(name, "usage") {
			return updateUsage(raw)
		}
		if name == "message" || name == "response" {
			if out, ok := rewriteKimiJSONObject(raw, func(key string, value json.RawMessage) (json.RawMessage, bool) {
				if strings.EqualFold(key, "usage") {
					return updateUsage(value), true
				}
				return value, true
			}); ok {
				return out
			}
		}
		return raw
	}
	out, ok := rewriteKimiJSONObject(body, func(key string, value json.RawMessage) (json.RawMessage, bool) { return rewrite(key, value), true })
	if !ok {
		return body
	}
	return out
}

func injectKimiCacheWriteSSE(c *gin.Context, sse string) string {
	if c == nil || !c.GetBool(kimiCacheMeteringContextKey) {
		return sse
	}
	lines := strings.Split(sse, "\n")
	for i, line := range lines {
		if payload, ok := extractOpenAISSEDataLine(line); ok {
			if updated := injectKimiCacheWriteUsage(c, []byte(payload)); string(updated) != payload {
				lines[i] = "data: " + string(updated)
			}
		}
	}
	return strings.Join(lines, "\n")
}

// Messages uses a top-level cache_control while Chat uses prompt_cache_options.
// Convert only the documented Kimi vocabulary and reject ambiguous values
// before dispatch, rather than silently dropping the user's requested lifetime.
func kimiMessagesCacheOptions(body []byte) (json.RawMessage, error) {
	if !gjson.GetBytes(body, "cache_control").Exists() {
		return nil, nil
	}
	fields, ok := kimiUsageObject(body)
	if !ok {
		return nil, fmt.Errorf("invalid Messages request object")
	}
	raw, exists := fields["cache_control"]
	if !exists {
		return nil, nil
	}
	control, ok := kimiUsageObject(raw)
	if !ok {
		return nil, fmt.Errorf("cache_control must be an object")
	}
	var kind, ttl string
	if raw, exists := control["type"]; !exists || json.Unmarshal(raw, &kind) != nil || kind != "ephemeral" {
		return nil, fmt.Errorf("cache_control.type must be ephemeral")
	}
	ttl = "5m"
	if raw, exists := control["ttl"]; exists {
		if json.Unmarshal(raw, &ttl) != nil || (ttl != "5m" && ttl != "1h") {
			return nil, fmt.Errorf("cache_control.ttl must be 5m or 1h")
		}
	}
	for key := range control {
		if key != "type" && key != "ttl" {
			return nil, fmt.Errorf("unsupported cache_control field")
		}
	}
	return json.Marshal(map[string]string{"mode": "implicit", "ttl": ttl})
}

// Retain original key spelling while reusing the strict duplicate-key check.
// JSON wire names are case sensitive even though the duplicate check also
// catches case-folded collisions, which some parsers would resolve differently.
func kimiUsageObject(raw []byte) (map[string]json.RawMessage, bool) {
	if _, ok := receiptObject(raw); !ok {
		return nil, false
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return nil, false
	}
	return fields, true
}

func kimiCacheWriteEvidence(raw []byte, headers http.Header) (kimiCacheWriteBreakdown, bool) {
	var zero kimiCacheWriteBreakdown
	aggregate, ok := kimiCacheWriteAggregate(raw)
	if !ok {
		return zero, false
	}
	fields, ok := kimiUsageObject(raw)
	if !ok {
		return zero, false
	}
	var bodySplit kimiCacheWriteBreakdown
	bodyKnown := false
	for name := range fields {
		if strings.EqualFold(name, "cache_creation") && name != "cache_creation" {
			return zero, false
		}
	}
	if body, exists := fields["cache_creation"]; exists {
		parts, valid := kimiUsageObject(body)
		if !valid {
			return zero, false
		}
		five, fiveOK := receiptInteger(parts["ephemeral_5m_input_tokens"])
		hour, hourOK := receiptInteger(parts["ephemeral_1h_input_tokens"])
		if !fiveOK || !hourOK || hour > math.MaxInt-five || five+hour != aggregate {
			return zero, false
		}
		bodySplit = kimiCacheWriteBreakdown{FiveMinutes: five, OneHour: hour}
		bodyKnown = true
	}
	headerPresent := false
	for key := range headers {
		if strings.EqualFold(key, "Msh-Usage-Cache-Write-Tokens-5m") || strings.EqualFold(key, "Msh-Usage-Cache-Write-Tokens-1h") {
			headerPresent = true
		}
	}
	if !headerPresent {
		return bodySplit, bodyKnown
	}
	headerSplit, valid := kimiCacheWriteHeaders(headers, aggregate)
	if !valid || (bodyKnown && bodySplit != headerSplit) {
		return zero, false
	}
	return headerSplit, true
}

// Remove only the disputed TTL object, retaining every aggregate and other raw
// field, including duplicate counters for downstream validation. Do not reduce
// conflicting evidence to an arbitrary last-key-wins value.
func stripKimiCacheWriteBreakdown(raw []byte) ([]byte, bool) {
	return rewriteKimiJSONObject(raw, func(key string, value json.RawMessage) (json.RawMessage, bool) {
		return value, !strings.EqualFold(key, "cache_creation")
	})
}

func rewriteKimiJSONObject(raw []byte, rewrite func(string, json.RawMessage) (json.RawMessage, bool)) ([]byte, bool) {
	d := json.NewDecoder(bytes.NewReader(raw))
	tok, err := d.Token()
	if err != nil || tok != json.Delim('{') {
		return raw, false
	}
	var out bytes.Buffer
	out.WriteByte('{')
	first := true
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return raw, false
		}
		key, ok := token.(string)
		if !ok {
			return raw, false
		}
		var value json.RawMessage
		if d.Decode(&value) != nil {
			return raw, false
		}
		value, keep := rewrite(key, value)
		if !keep {
			continue
		}
		if !first {
			out.WriteByte(',')
		}
		first = false
		encoded, _ := json.Marshal(key)
		out.Write(encoded)
		out.WriteByte(':')
		out.Write(value)
	}
	if tok, err = d.Token(); err != nil || tok != json.Delim('}') {
		return raw, false
	}
	if _, err = d.Token(); err != io.EOF {
		return raw, false
	}
	out.WriteByte('}')
	return out.Bytes(), true
}
