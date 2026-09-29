package commandcode

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"strings"
)

type event struct {
	Type              string          `json:"type"`
	ID                string          `json:"id"`
	Text              *string         `json:"text"`
	Delta             *string         `json:"delta"`
	Content           *string         `json:"content"`
	ToolCallID        string          `json:"toolCallId"`
	ToolName          string          `json:"toolName"`
	Input             json.RawMessage `json:"input"`
	Args              json.RawMessage `json:"args"`
	ProviderExecuted  bool            `json:"providerExecuted"`
	FinishReason      string          `json:"finishReason"`
	FinishReasonSnake string          `json:"finish_reason"`
	Usage             json.RawMessage `json:"usage"`
	TotalUsage        json.RawMessage `json:"totalUsage"`
}

type completion struct {
	text      strings.Builder
	reasoning strings.Builder
	calls     []map[string]any
	finish    string
	usage     map[string]any
}

type eventConsumer struct {
	request    preparedRequest
	result     completion
	emit       func(map[string]any) error
	finished   bool
	callIDs    map[string]bool
	pending    map[string]bool
	stepUsage  json.RawMessage
	stepFinish string
	onUsage    func(map[string]any) error
}

func readEvents(reader io.Reader, input preparedRequest, emit func(map[string]any) error, onUsage func(map[string]any) error) (completion, error) {
	c := eventConsumer{
		request: input, emit: emit, onUsage: onUsage,
		callIDs: make(map[string]bool), pending: make(map[string]bool),
	}
	limited := &io.LimitedReader{R: reader, N: MaxStreamBytes + 1}
	scanner := bufio.NewScanner(limited)
	scanner.Buffer(make([]byte, 4096), MaxEventBytes)
	var frame []byte
	for scanner.Scan() {
		if limited.N == 0 {
			return c.result, ErrLimit
		}
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			if len(frame) > 0 {
				return c.result, ErrProtocol
			}
			continue
		}
		if bytes.HasPrefix(line, []byte(":")) || bytes.HasPrefix(line, []byte("event:")) || bytes.HasPrefix(line, []byte("id:")) || bytes.HasPrefix(line, []byte("retry:")) {
			continue
		}
		if bytes.HasPrefix(line, []byte("data:")) {
			line = bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
			if len(frame) > 0 {
				frame = append(frame, '\n')
			}
			frame = append(frame, line...)
			if len(frame) > MaxEventBytes {
				return c.result, ErrLimit
			}
			if bytes.Equal(frame, []byte("[DONE]")) {
				if !c.finished {
					return c.result, io.ErrUnexpectedEOF
				}
				return c.result, nil
			}
			if !json.Valid(frame) {
				continue
			}
			line = frame
		} else if len(frame) > 0 {
			return c.result, ErrProtocol
		}
		if err := c.accept(line); err != nil {
			return c.result, err
		}
		frame = nil
	}
	if scanner.Err() != nil {
		if scanner.Err() == bufio.ErrTooLong {
			return c.result, ErrLimit
		}
		// Response headers have already arrived. Replaying a failed body read
		// could duplicate an upstream generation whose usage was never reported.
		return c.result, ErrUpstream
	}
	if len(frame) > 0 {
		return c.result, ErrProtocol
	}
	if !c.finished {
		return c.result, io.ErrUnexpectedEOF
	}
	return c.result, nil
}

func (c *eventConsumer) accept(line []byte) error {
	var e event
	if json.Unmarshal(line, &e) != nil || e.Type == "" {
		return ErrProtocol
	}
	if c.finished {
		return ErrProtocol
	}
	switch e.Type {
	case "text-delta", "reasoning-delta", "reasoning":
		text := e.Text
		if text == nil {
			text = e.Delta
		}
		if text == nil {
			text = e.Content
		}
		if text == nil {
			return ErrProtocol
		}
		reasoning := e.Type != "text-delta"
		if c.emit != nil {
			if reasoning {
				return c.emit(map[string]any{"reasoning_content": *text})
			}
			return c.emit(map[string]any{"content": *text})
		}
		if reasoning {
			c.result.reasoning.WriteString(*text)
		} else {
			c.result.text.WriteString(*text)
		}
	case "tool-call":
		if e.ProviderExecuted || !c.request.tools[e.ToolName] || e.ToolCallID == "" || len(e.ToolCallID) > 512 || c.callIDs[e.ToolCallID] || len(c.result.calls) >= 256 {
			return ErrProtocol
		}
		if !c.request.parallel && len(c.result.calls) > 0 {
			return ErrProtocol
		}
		args := e.Input
		if len(args) == 0 {
			args = e.Args
		}
		var asString string
		if json.Unmarshal(args, &asString) == nil {
			args = []byte(asString)
		}
		if !jsonObject(args) {
			return ErrProtocol
		}
		var compact bytes.Buffer
		if json.Compact(&compact, args) != nil {
			return ErrProtocol
		}
		call := map[string]any{"id": e.ToolCallID, "type": "function", "function": map[string]any{"name": e.ToolName, "arguments": compact.String()}}
		c.callIDs[e.ToolCallID] = true
		delete(c.pending, e.ToolCallID)
		c.result.calls = append(c.result.calls, call)
		if c.emit != nil {
			return c.emit(map[string]any{"tool_calls": []any{map[string]any{"index": len(c.result.calls) - 1, "id": e.ToolCallID, "type": "function", "function": call["function"]}}})
		}
	case "finish-step":
		c.stepUsage = e.Usage
		c.stepFinish = e.FinishReason
	case "finish":
		raw := e.TotalUsage
		if absent(raw) {
			raw = e.Usage
		}
		if absent(raw) {
			raw = c.stepUsage
		}
		usage, err := convertUsage(raw)
		if err != nil {
			return err
		}
		c.result.usage = usage
		if c.onUsage != nil {
			if err := c.onUsage(usage); err != nil {
				return err
			}
		}
		if len(c.pending) > 0 {
			return ErrProtocol
		}
		reason := e.FinishReason
		if reason == "" {
			reason = e.FinishReasonSnake
		}
		if reason == "" {
			reason = c.stepFinish
		}
		finish, err := finishReason(reason)
		if err != nil {
			return err
		}
		if len(c.result.calls) > 0 {
			if finish == "stop" {
				finish = "tool_calls"
			}
		} else if finish == "tool_calls" {
			return ErrProtocol
		}
		if finish == "stop" && c.request.choice.kind == "required" {
			return ErrProtocol
		}
		if c.request.choice.kind == "tool" {
			if finish == "stop" {
				return ErrProtocol
			}
			for _, call := range c.result.calls {
				if call["function"].(map[string]any)["name"] != c.request.choice.name {
					return ErrProtocol
				}
			}
		}
		c.finished, c.result.finish = true, finish
	case "error", "tool-error", "abort":
		return ErrUpstream
	case "tool-input-start":
		id := e.ToolCallID
		if id == "" {
			id = e.ID
		}
		if id == "" || len(id) > 512 || c.pending[id] || c.callIDs[id] || len(c.pending) >= 256 {
			return ErrProtocol
		}
		c.pending[id] = true
	case "tool-input-delta", "tool-input-end":
		id := e.ToolCallID
		if id == "" {
			id = e.ID
		}
		if !c.pending[id] {
			return ErrProtocol
		}
	case "start", "start-step", "text-start", "text-end", "reasoning-start", "reasoning-end", "provider-metadata":
		// These events carry framing metadata; content has its own typed events.
	default:
		return ErrProtocol
	}
	return nil
}

func finishReason(reason string) (string, error) {
	switch reason {
	case "stop", "end_turn", "stop_sequence", "complete":
		return "stop", nil
	case "length", "max_tokens", "max_output_tokens", "model_context_window_exceeded":
		return "length", nil
	case "tool_calls", "tool-calls", "tool_use":
		return "tool_calls", nil
	case "content_filter":
		return "content_filter", nil
	default:
		return "", ErrUpstream
	}
}
