package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

func wantsAnthropicBufferedStream(account *Account, model string, clientStream bool) bool {
	if clientStream || account == nil || !account.IsAnthropicAPIKeyPassthroughEnabled() {
		return false
	}
	models, _ := json.Marshal(account.Extra["anthropic_buffered_stream_models"])
	var selected []string
	if json.Unmarshal(models, &selected) != nil {
		return false
	}
	for _, candidate := range selected {
		if candidate == model {
			return true
		}
	}
	return false
}

type anthropicBufferedMessage struct {
	message    map[string]json.RawMessage
	blocks     []map[string]json.RawMessage
	closed     []bool
	toolJSON   []*strings.Builder
	textFields []map[string]*strings.Builder
	usage      ClaudeUsage
	inputUsage bool
	finalUsage bool
	terminal   bool
}

func (m *anthropicBufferedMessage) apply(frame openAICompatSSEFrame) error {
	if strings.TrimSpace(frame.Data) == "" {
		return nil
	}
	var event struct {
		Type         string                     `json:"type"`
		Index        *int                       `json:"index"`
		Message      map[string]json.RawMessage `json:"message"`
		ContentBlock map[string]json.RawMessage `json:"content_block"`
		Delta        map[string]json.RawMessage `json:"delta"`
		Usage        map[string]json.RawMessage `json:"usage"`
	}
	if json.Unmarshal([]byte(frame.Data), &event) != nil || event.Type == "" || (frame.EventType != "" && frame.EventType != event.Type) {
		return errors.New("invalid Anthropic stream event")
	}
	if event.Type != "message_stop" {
		parseSSEUsagePassthrough(frame.Data, &m.usage)
	}
	if event.Type == "ping" {
		return nil
	}
	if event.Type == "error" {
		return &sseStreamErrorEventError{RawData: frame.Data}
	}
	if event.Type == "message_start" {
		if m.message != nil || event.Message == nil || gjson.Get(frame.Data, "message.type").String() != "message" {
			return errors.New("invalid Anthropic message start")
		}
		m.message = event.Message
		m.inputUsage = anthropicNonnegativeCounter(gjson.Get(frame.Data, "message.usage.input_tokens").Raw)
		var initial []json.RawMessage
		if json.Unmarshal(m.message["content"], &initial) != nil || len(initial) != 0 {
			return errors.New("Anthropic stream must start with empty content")
		}
		return nil
	}
	if m.message == nil {
		return errors.New("Anthropic event before message start")
	}
	switch event.Type {
	case "content_block_start":
		if event.Index == nil || *event.Index != len(m.blocks) || event.ContentBlock == nil {
			return errors.New("invalid Anthropic content block start")
		}
		m.blocks = append(m.blocks, event.ContentBlock)
		m.closed = append(m.closed, false)
		m.toolJSON = append(m.toolJSON, &strings.Builder{})
		m.textFields = append(m.textFields, make(map[string]*strings.Builder))
	case "content_block_delta", "content_block_stop":
		if event.Index == nil || *event.Index < 0 || *event.Index >= len(m.blocks) || m.closed[*event.Index] {
			return errors.New("invalid Anthropic content block index")
		}
		i := *event.Index
		if event.Type == "content_block_stop" {
			if text := m.toolJSON[i].String(); text != "" {
				if !json.Valid([]byte(text)) || !gjson.Parse(text).IsObject() {
					return errors.New("invalid Anthropic tool input JSON")
				}
				m.blocks[i]["input"] = json.RawMessage(text)
			}
			for field, value := range m.textFields[i] {
				m.blocks[i][field], _ = json.Marshal(value.String())
			}
			m.closed[i] = true
			return nil
		}
		var kind string
		_ = json.Unmarshal(event.Delta["type"], &kind)
		var blockType string
		_ = json.Unmarshal(m.blocks[i]["type"], &blockType)
		expected := map[string]string{"text_delta": "text", "thinking_delta": "thinking", "signature_delta": "thinking", "citations_delta": "text", "input_json_delta": "tool_use"}[kind]
		if expected == "" || (blockType != expected && !(kind == "input_json_delta" && blockType == "server_tool_use")) {
			return errors.New("Anthropic delta does not match its content block")
		}
		if kind == "citations_delta" {
			var citation map[string]json.RawMessage
			if json.Unmarshal(event.Delta["citation"], &citation) != nil || citation == nil {
				return errors.New("invalid Anthropic citation")
			}
			var citations []json.RawMessage
			if raw := m.blocks[i]["citations"]; len(raw) > 0 && json.Unmarshal(raw, &citations) != nil {
				return errors.New("invalid Anthropic citation list")
			}
			m.blocks[i]["citations"], _ = json.Marshal(append(citations, event.Delta["citation"]))
			return nil
		}
		field := map[string]string{"text_delta": "text", "thinking_delta": "thinking", "signature_delta": "signature", "input_json_delta": "partial_json"}[kind]
		if field == "" {
			return fmt.Errorf("unsupported Anthropic content delta: %s", kind)
		}
		var delta string
		if json.Unmarshal(event.Delta[field], &delta) != nil {
			return errors.New("invalid Anthropic string delta")
		}
		if field == "partial_json" {
			m.toolJSON[i].WriteString(delta)
		} else {
			value := m.textFields[i][field]
			if value == nil {
				var initial string
				if raw := m.blocks[i][field]; len(raw) != 0 && json.Unmarshal(raw, &initial) != nil {
					return errors.New("invalid Anthropic block field")
				}
				value = &strings.Builder{}
				value.WriteString(initial)
				m.textFields[i][field] = value
			}
			value.WriteString(delta)
		}
	case "message_delta":
		for key, value := range event.Delta {
			switch key {
			case "content", "model", "id", "role", "type", "usage":
				return errors.New("Anthropic delta cannot replace message identity")
			}
			m.message[key] = value
		}
		var usage map[string]json.RawMessage
		_ = json.Unmarshal(m.message["usage"], &usage)
		if usage == nil {
			usage = make(map[string]json.RawMessage)
		}
		mergeAnthropicRawObject(usage, event.Usage)
		m.message["usage"], _ = json.Marshal(usage)
		if value, present := event.Usage["input_tokens"]; present {
			m.inputUsage = anthropicNonnegativeCounter(string(value))
		}
		if value, present := event.Usage["output_tokens"]; present {
			m.finalUsage = anthropicNonnegativeCounter(string(value))
		}
	case "message_stop":
		var reason string
		_ = json.Unmarshal(m.message["stop_reason"], &reason)
		if !m.inputUsage || !m.finalUsage || m.usage.receiptInvalid || reason == "" {
			return errors.New("Anthropic stream is missing final usage or stop reason")
		}
		if len(m.blocks) == 0 {
			return errors.New("Anthropic stream returned no content blocks")
		}
		for _, closed := range m.closed {
			if !closed {
				return errors.New("Anthropic stream ended inside a content block")
			}
		}
		parseSSEUsagePassthrough(frame.Data, &m.usage)
		m.terminal = true
	default:
		return errors.New("unsupported Anthropic stream event")
	}
	return nil
}

func anthropicNonnegativeCounter(raw string) bool {
	if raw == "" || strings.TrimSpace(raw) == "null" {
		return false
	}
	var value int64
	return json.Unmarshal([]byte(raw), &value) == nil && value >= 0
}

func mergeAnthropicRawObject(dst, patch map[string]json.RawMessage) {
	for key, value := range patch {
		var oldObject, newObject map[string]json.RawMessage
		if json.Unmarshal(dst[key], &oldObject) == nil && oldObject != nil && json.Unmarshal(value, &newObject) == nil && newObject != nil {
			mergeAnthropicRawObject(oldObject, newObject)
			dst[key], _ = json.Marshal(oldObject)
		} else {
			dst[key] = value
		}
	}
}

func collectAnthropicBufferedStream(reader io.Reader, limit int64) (*anthropicBufferedMessage, error) {
	m := &anthropicBufferedMessage{}
	bounded := &io.LimitedReader{R: reader, N: limit + 1}
	scanner := bufio.NewScanner(bounded)
	scanner.Buffer(make([]byte, 64*1024), int(limit+1))
	var parser openAICompatSSEFrameParser
	var consumed int64
	for scanner.Scan() {
		consumed += int64(len(scanner.Bytes())) + 1
		if consumed > limit {
			return m, ErrUpstreamResponseBodyTooLarge
		}
		frame, ready := parser.AddLine(strings.TrimRight(scanner.Text(), "\r"))
		if ready {
			if err := m.apply(frame); err != nil {
				return m, err
			}
			if m.terminal {
				return m, nil
			}
		}
	}
	if bounded.N <= 0 || errors.Is(scanner.Err(), bufio.ErrTooLong) {
		return m, ErrUpstreamResponseBodyTooLarge
	}
	if scanner.Err() != nil {
		return m, scanner.Err()
	}
	if frame, ready := parser.Finish(); ready {
		if err := m.apply(frame); err != nil {
			return m, err
		}
	}
	if !m.terminal {
		return m, errors.New("Anthropic stream ended without message_stop")
	}
	return m, nil
}

func (s *GatewayService) writeAnthropicBufferedStream(ctx context.Context, resp *http.Response, c *gin.Context, input anthropicPassthroughForwardInput) (*ForwardResult, error) {
	collected, err := awaitAnthropicBody(c, resp.Body, func() (*anthropicBufferedMessage, error) {
		return collectAnthropicBufferedStream(resp.Body, resolveUpstreamResponseReadLimit(s.cfg))
	})
	result := &ForwardResult{RequestID: resp.Header.Get("x-request-id"), UpstreamHeaders: resp.Header,
		Usage: collected.usage, ClientDisconnect: c.Request.Context().Err() != nil, Model: input.OriginalModel, UpstreamModel: input.RequestModel,
		Stream: false, Duration: time.Since(input.StartTime)}
	metadata, _ := json.Marshal(collected.message)
	observer := beginUpstreamResponseModelObservation(c)
	observer.ObserveAnthropic(metadata)
	result.UpstreamResponseModel = observedUpstreamResponseModel(c)
	result.UpstreamResponseModelConflict = observedUpstreamResponseModelConflict(c)
	result.UpstreamResponseServiceTier = observedUpstreamResponseServiceTier(c)
	if err != nil {
		status := http.StatusBadGateway
		var upstream *sseStreamErrorEventError
		if errors.As(err, &upstream) {
			raw := []byte(upstream.RawData)
			status = openAIImagesExplicitErrorStatus(raw, anthropicStreamErrorStatus(raw))
		}
		var reportedUsage map[string]json.RawMessage
		_ = json.Unmarshal(collected.message["usage"], &reportedUsage)
		WriteAnthropicErrorResponse(c, status, gin.H{"type": "error", "usage": reportedUsage, "error": gin.H{
			"type": "upstream_error", "message": "Upstream stream did not complete; request was not retried", "status_code": status,
		}})
		MarkResponseCommitted(c)
		if !result.Usage.hasObservedTokens() {
			result = nil
		}
		return result, err
	}
	collected.message["content"], _ = json.Marshal(collected.blocks)
	body, err := json.Marshal(collected.message)
	if err != nil {
		return result, err
	}
	if IsForceCacheBilling(ctx) && collected.usage.InputTokens > 0 {
		body, err = classifyAnthropicResponseInputAsCacheRead(body, &collected.usage)
		if err != nil {
			return result, err
		}
	}
	result.Usage = collected.usage
	writeAnthropicPassthroughResponseHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	c.Writer.Header().Del("Content-Length")
	c.Writer.Header().Del("Content-Encoding")
	c.Writer.Header().Del("Transfer-Encoding")
	c.Header("Content-Type", "application/json; charset=utf-8")
	c.Data(http.StatusOK, "application/json; charset=utf-8", bytes.TrimSpace(reverseToolNamesIfPresent(c, body)))
	return result, nil
}
