package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/util/responseheaders"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

func (s *GeminiMessagesCompatService) writeOpenAINativeGeminiJSON(c *gin.Context, resp *http.Response, native map[string]any, raw []byte, model string, output *geminiResponsesOutput, meterOverride *geminiOpenAIMeterState) (*ClaudeUsage, error) {
	meter := meterOverride
	if meter == nil {
		meter = &geminiOpenAIMeterState{}
		meter.observe(raw)
	}
	usage := meter.usage
	fail := func(fields *openAIClientErrorFields) (*ClaudeUsage, error) {
		setOpsUpstreamError(c, fields.StatusCode, fields.Message, "")
		if c.Request.Context().Err() == nil {
			u := claudeUsageToOpenAIUsage(&usage)
			writeOpenAIErrorFields(c, *fields, false, &u)
		}
		return &usage, errors.New(fields.Message)
	}
	fields := geminiOpenAIErrorFields(raw)
	if fields != nil {
		return fail(fields)
	}
	finish := extractGeminiFinishReason(native)
	if finish != "STOP" && finish != "MAX_TOKENS" {
		return fail(&openAIClientErrorFields{StatusCode: 502, Type: "upstream_error", Code: "invalid_upstream_terminal", Message: "Upstream did not return a supported successful terminal state"})
	}
	if meter.invalid || meter.receipt == nil {
		return fail(&openAIClientErrorFields{StatusCode: 502, Type: "upstream_error", Code: "upstream_incomplete_response", Message: "Upstream did not return valid final usage"})
	}
	visible := false
	for _, part := range extractGeminiParts(native) {
		if text, ok := part["text"].(string); ok && text != "" && !isGeminiThoughtPart(part) {
			visible = true
		}
		if data, ok := part["inlineData"].(map[string]any); ok {
			mime, _ := data["mimeType"].(string)
			payload, _ := data["data"].(string)
			visible = visible || (isGeminiInlineImageMIMEType(mime) && isValidBase64(payload))
		}
		if call, ok := part["functionCall"].(map[string]any); ok {
			if fields := validateGeminiOpenAIFunction(call); fields != nil {
				return fail(fields)
			}
			visible = true
		}
	}
	if !visible {
		return fail(&openAIClientErrorFields{StatusCode: 502, Type: "upstream_error", Code: "upstream_empty_completion", Message: "Upstream returned no response content; reported usage is retained"})
	}
	usage.receiptMeasured = meter.receipt
	chat, _, err := geminiResponseToChatCompletions(native, model, raw, &usage)
	if err != nil {
		return &usage, err
	}
	wire, err := geminiOpenAIResponseWire(chat, native, output)
	if err != nil {
		return &usage, err
	}
	responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	if c.Request.Context().Err() == nil {
		c.JSON(http.StatusOK, wire)
	}
	return &usage, nil
}

func validateGeminiOpenAIFunction(call map[string]any) *openAIClientErrorFields {
	name, _ := call["name"].(string)
	if strings.TrimSpace(name) == "" {
		return geminiMalformedToolError()
	}
	if args, exists := call["args"]; exists && args != nil {
		raw, _ := json.Marshal(args)
		if text, ok := args.(string); ok {
			raw = []byte(text)
		}
		trimmed := bytes.TrimSpace(raw)
		if !json.Valid(trimmed) || len(trimmed) == 0 || trimmed[0] != '{' {
			return geminiMalformedToolError()
		}
	}
	return nil
}

// OAuth requires an upstream stream even for buffered callers. Preserve every
// tool/content part and final meter instead of keeping only the last chunk.
func (s *GeminiMessagesCompatService) handleOpenAIBufferedStreamFromGemini(c *gin.Context, resp *http.Response, model string, isOAuth bool, output *geminiResponsesOutput) (*ClaudeUsage, error) {
	meter := &geminiOpenAIMeterState{}
	var parts []any
	finish := ""
	finalMeterSeen := false
	doneSeen := false
	var failure *openAIClientErrorFields
	stream := newAnthropicCompatStream(s.cfg, c, resp, "gemini-openai-buffered", "")
	defer stream.stop()
	observe := func(raw []byte) {
		meter.observe(raw)
		var native map[string]any
		if decodeGeminiChatObject(raw, &native) != nil {
			failure = &openAIClientErrorFields{StatusCode: 502, Type: "upstream_error", Message: "Upstream sent malformed stream data"}
			return
		}
		if f := geminiOpenAIErrorFields(raw); f != nil {
			failure = f
			return
		}
		if finish != "" && len(extractGeminiParts(native)) > 0 {
			failure = &openAIClientErrorFields{StatusCode: 502, Type: "upstream_error", Message: "Upstream sent content after its terminal response"}
			return
		}
		if f := extractGeminiFinishReason(native); f != "" {
			finish = f
		}
		if finish != "" && gjson.GetBytes(raw, "usageMetadata").Exists() && meter.receipt != nil {
			finalMeterSeen = true
		}
		for _, part := range extractGeminiParts(native) {
			parts = append(parts, part)
		}
	}
	for {
		line, ok := stream.nextLine()
		if !ok {
			break
		}
		payload, ok := extractOpenAISSEDataLine(line)
		if !ok || strings.TrimSpace(payload) == "" {
			continue
		}
		if strings.TrimSpace(payload) == "[DONE]" {
			doneSeen = true
			continue
		}
		if doneSeen {
			failure = &openAIClientErrorFields{StatusCode: 502, Type: "upstream_error", Message: "Upstream sent data after its stream terminator"}
		}
		raw := []byte(payload)
		if isOAuth {
			if inner, err := unwrapGeminiResponse(raw); err == nil {
				raw = inner
			}
		}
		observe(raw)
	}
	if stream.readErr != nil && !errors.Is(stream.readErr, io.EOF) && failure == nil {
		failure = &openAIClientErrorFields{StatusCode: 502, Type: "upstream_error", Message: "Upstream stream ended before a complete response"}
	}
	if !finalMeterSeen {
		meter.receipt = nil
	}
	native := map[string]any{"candidates": []any{map[string]any{"content": map[string]any{"role": "model", "parts": parts}, "finishReason": finish}}}
	if failure != nil {
		native["error"] = map[string]any{"code": failure.StatusCode, "message": failure.Message}
	}
	raw, marshalErr := json.Marshal(native)
	if marshalErr != nil {
		return &meter.usage, fmt.Errorf("encode collected Gemini response: %w", marshalErr)
	}
	return s.writeOpenAINativeGeminiJSON(c, resp, native, raw, model, output, meter)
}
