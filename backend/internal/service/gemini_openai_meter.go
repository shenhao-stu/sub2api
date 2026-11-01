package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"math"
	"net/http"
)

type geminiOpenAIMeterState struct {
	counters map[string]int
	usage    ClaudeUsage
	receipt  *UsageReceipt
	invalid  bool
}

// Intermediate counters may be partial. Only an intact, non-regressing meter
// can produce final evidence; observed counters never become guessed zeroes.
func (m *geminiOpenAIMeterState) observe(data []byte) {
	raw := gjson.GetBytes(data, "usageMetadata")
	if !raw.Exists() {
		return
	}
	fields, ok := receiptObject([]byte(raw.Raw))
	if !ok {
		m.invalid = true
		return
	}
	if m.counters == nil {
		m.counters = map[string]int{}
	}
	next := make(map[string]int, len(m.counters))
	for key, value := range m.counters {
		next[key] = value
	}
	names := []string{"prompttokencount", "candidatestokencount", "cachedcontenttokencount", "thoughtstokencount"}
	complete := fields[names[0]] != nil && fields[names[1]] != nil
	for _, name := range names {
		raw, present := fields[name]
		if !present {
			if complete && next[name] > 0 && name != names[0] && name != names[1] {
				m.invalid = true
			}
			continue
		}
		value, valid := receiptInteger(raw)
		if !valid || value < next[name] {
			m.invalid = true
			return
		}
		next[name] = value
	}
	if rawDetails, present := fields["candidatestokensdetails"]; present {
		var details []json.RawMessage
		if json.Unmarshal(rawDetails, &details) != nil || string(rawDetails) == "null" {
			m.invalid = true
			return
		}
		image := 0
		seen := map[string]bool{}
		for _, rawDetail := range details {
			detail, valid := receiptObject(rawDetail)
			var modality string
			if !valid || json.Unmarshal(detail["modality"], &modality) != nil || modality == "" || seen[modality] {
				m.invalid = true
				return
			}
			seen[modality] = true
			count, valid := receiptInteger(detail["tokencount"])
			if !valid {
				m.invalid = true
				return
			}
			if modality == "IMAGE" {
				image = count
			}
		}
		if image < next["imagetokens"] {
			m.invalid = true
			return
		}
		next["imagetokens"] = image
	} else if complete && next["imagetokens"] > 0 {
		m.invalid = true
	}
	prompt, cached, output, thoughts := next[names[0]], next[names[2]], next[names[1]], next[names[3]]
	if (fields[names[1]] != nil && next["imagetokens"] > output) || output > math.MaxInt-thoughts || (fields[names[0]] != nil && cached > prompt) {
		m.invalid = true
		return
	}
	m.counters = next
	m.usage = ClaudeUsage{InputTokens: max(prompt-cached, 0), OutputTokens: output + thoughts, CacheReadInputTokens: cached, ImageOutputTokens: next["imagetokens"]}
	m.receipt = nil
	if complete && !m.invalid {
		m.receipt = &UsageReceipt{InputTokens: m.usage.InputTokens, OutputTokens: m.usage.OutputTokens, CacheReadTokens: cached, ReasoningTokens: thoughts}
	}
}

func geminiOpenAIStreamFailure(c *gin.Context, disconnected bool, fields openAIClientErrorFields, cause error, usage *ClaudeUsage) error {
	setOpsUpstreamError(c, fields.StatusCode, fields.Message, "")
	if !disconnected {
		meter := claudeUsageToOpenAIUsage(usage)
		writeOpenAIErrorFields(c, fields, true, &meter)
	}
	if cause != nil {
		return fmt.Errorf("%s: %w", fields.Message, cause)
	}
	return errors.New(fields.Message)
}

func geminiOpenAIErrorFields(data []byte) *openAIClientErrorFields {
	value := gjson.GetBytes(data, "error")
	if !value.Exists() {
		return nil
	}
	status := int(value.Get("code").Int())
	if status < 400 || status > 599 {
		status = http.StatusBadGateway
	}
	message := sanitizeUpstreamErrorMessage(value.Get("message").String())
	if message == "" {
		message = "Upstream returned a stream error"
	}
	kind := "upstream_error"
	if status == 429 {
		kind = "rate_limit_error"
	}
	if status == 400 {
		kind = "invalid_request_error"
	}
	return &openAIClientErrorFields{Type: kind, Code: "upstream_stream_error", Message: message, StatusCode: status}
}
func geminiMalformedToolError() *openAIClientErrorFields {
	return &openAIClientErrorFields{Type: "upstream_error", Code: "invalid_tool_call", Message: "Upstream returned an invalid function call; observed usage is retained", StatusCode: 502}
}

type geminiCancelCloser func()

func (cancel geminiCancelCloser) Close() error { cancel(); return nil }

func boundedGeminiUpstreamContext(ctx context.Context) (context.Context, func()) {
	detached, release := detachUpstreamContext(ctx)
	upstream, cancel := context.WithCancel(detached)
	stop := boundDisconnectedResponseDrain(ctx, geminiCancelCloser(cancel))
	return upstream, func() { stop(); cancel(); release() }
}
