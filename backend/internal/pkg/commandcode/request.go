// Package commandcode adapts the CommandCode GO generate protocol to Chat Completions.
// The wire format was independently implemented from the reviewed public protocol.
package commandcode

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"
)

const (
	Endpoint        = "https://api.commandcode.ai/alpha/generate"
	MaxRequestBytes = 8 << 20
	MaxStreamBytes  = 32 << 20
	MaxEventBytes   = 1 << 20
)

// Errors deliberately contain no input, credentials, transport URL or upstream body.
var (
	ErrRequest   = errors.New("commandcode: unsupported or invalid request")
	ErrProtocol  = errors.New("commandcode: invalid upstream event")
	ErrUsage     = errors.New("commandcode: missing or invalid upstream usage")
	ErrLimit     = errors.New("commandcode: response exceeds size limit")
	ErrUpstream  = errors.New("commandcode: upstream generation failed")
	ErrTransport = errors.New("commandcode: upstream transport failed")
)

type request struct {
	Model               string          `json:"model"`
	Messages            []message       `json:"messages"`
	Tools               []tool          `json:"tools"`
	ToolChoice          json.RawMessage `json:"tool_choice"`
	MaxTokens           *int            `json:"max_tokens"`
	MaxCompletionTokens *int            `json:"max_completion_tokens"`
	Stream              *bool           `json:"stream"`
	StreamOptions       *struct {
		IncludeUsage bool `json:"include_usage"`
	} `json:"stream_options"`
	Temperature       *float64 `json:"temperature"`
	ReasoningEffort   string   `json:"reasoning_effort"`
	ParallelToolCalls *bool    `json:"parallel_tool_calls"`
	N                 *int     `json:"n"`
}

type message struct {
	Role             string          `json:"role"`
	Content          json.RawMessage `json:"content"`
	ToolCalls        []toolCall      `json:"tool_calls"`
	ToolCallID       string          `json:"tool_call_id"`
	ReasoningContent string          `json:"reasoning_content"`
}

type tool struct {
	Type     string `json:"type"`
	Function struct {
		Name        string          `json:"name"`
		Description string          `json:"description"`
		Parameters  json.RawMessage `json:"parameters"`
		Strict      *bool           `json:"strict"`
	} `json:"function"`
}

type toolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type preparedRequest struct {
	body     []byte
	model    string
	tools    map[string]bool
	choice   toolChoice
	parallel bool
}

type toolChoice struct{ kind, name string }

func strictJSON(data []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	d.UseNumber()
	if d.Decode(target) != nil {
		return ErrRequest
	}
	if d.Decode(new(any)) != io.EOF {
		return ErrRequest
	}
	return nil
}

func prepareRequest(body []byte, stream bool, now time.Time) (preparedRequest, error) {
	var in request
	if len(body) == 0 || len(body) > MaxRequestBytes || strictJSON(body, &in) != nil {
		return preparedRequest{}, ErrRequest
	}
	if strings.TrimSpace(in.Model) == "" || len(in.Model) > 512 || len(in.Messages) == 0 || len(in.Messages) > 4096 || len(in.Tools) > 256 {
		return preparedRequest{}, ErrRequest
	}
	if in.Stream != nil && *in.Stream != stream {
		return preparedRequest{}, ErrRequest
	}
	if in.N != nil && *in.N != 1 {
		return preparedRequest{}, ErrRequest
	}
	limit := 4096
	for _, value := range []*int{in.MaxTokens, in.MaxCompletionTokens} {
		if value == nil {
			continue
		}
		if *value <= 0 || *value > 1_000_000 {
			return preparedRequest{}, ErrRequest
		}
		limit = *value
	}
	params := map[string]any{"model": in.Model, "canonicalID": in.Model, "max_tokens": limit, "stream": true}
	if in.Temperature != nil {
		if math.IsNaN(*in.Temperature) || math.IsInf(*in.Temperature, 0) || *in.Temperature < 0 || *in.Temperature > 2 {
			return preparedRequest{}, ErrRequest
		}
		params["temperature"] = *in.Temperature
	}
	if in.ReasoningEffort != "" {
		params["reasoning_effort"] = in.ReasoningEffort
	}
	if in.ParallelToolCalls != nil {
		params["parallel_tool_calls"] = *in.ParallelToolCalls
	}
	definitions, names, err := convertTools(in.Tools)
	if err != nil {
		return preparedRequest{}, err
	}
	choice, err := convertToolChoice(in.ToolChoice, names)
	if err != nil {
		return preparedRequest{}, err
	}
	if choice.kind == "none" {
		definitions = []map[string]any{}
		names = map[string]bool{}
	}
	params["tools"] = definitions
	switch choice.kind {
	case "", "none":
	case "auto":
		params["tool_choice"] = map[string]string{"type": "auto"}
	case "required":
		params["tool_choice"] = map[string]string{"type": "any"}
	default:
		params["tool_choice"] = map[string]string{"type": "tool", "name": choice.name}
	}
	messages, system, err := convertMessages(in.Messages)
	if err != nil {
		return preparedRequest{}, err
	}
	params["messages"] = messages
	// A missing system lets the upstream inject its own coding-agent prompt.
	if len(system) == 0 {
		system = []map[string]any{{"type": "text", "text": " "}}
	}
	params["system"] = system
	id, err := uuid()
	if err != nil {
		return preparedRequest{}, ErrRequest
	}
	out, err := json.Marshal(map[string]any{
		"config": map[string]any{"workingDir": "/tmp", "date": now.UTC().Format("2006-01-02"), "environment": "linux", "structure": []any{}, "isGitRepo": false, "currentBranch": "", "mainBranch": "", "gitStatus": "", "recentCommits": []any{}},
		"memory": nil, "permissionMode": "standard", "threadId": id, "params": params,
	})
	if err != nil {
		return preparedRequest{}, ErrRequest
	}
	parallel := in.ParallelToolCalls == nil || *in.ParallelToolCalls
	return preparedRequest{body: out, model: in.Model, tools: names, choice: choice, parallel: parallel}, nil
}

func convertTools(input []tool) ([]map[string]any, map[string]bool, error) {
	result := make([]map[string]any, 0, len(input))
	names := make(map[string]bool, len(input))
	for _, t := range input {
		fn := t.Function
		if t.Type != "function" || !validName(fn.Name) || names[fn.Name] || (fn.Strict != nil && *fn.Strict) {
			return nil, nil, ErrRequest
		}
		schema := fn.Parameters
		if absent(schema) {
			schema = json.RawMessage(`{"type":"object","properties":{}}`)
		}
		if !jsonObject(schema) {
			return nil, nil, ErrRequest
		}
		names[fn.Name] = true
		result = append(result, map[string]any{"name": fn.Name, "description": fn.Description, "input_schema": schema})
	}
	return result, names, nil
}

func convertToolChoice(raw json.RawMessage, names map[string]bool) (toolChoice, error) {
	if absent(raw) {
		return toolChoice{}, nil
	}
	var choice string
	if json.Unmarshal(raw, &choice) == nil {
		switch choice {
		case "auto", "none":
			return toolChoice{kind: choice}, nil
		case "required":
			if len(names) > 0 {
				return toolChoice{kind: choice}, nil
			}
		}
		return toolChoice{}, ErrRequest
	}
	var fn struct {
		Type     string `json:"type"`
		Function struct {
			Name string `json:"name"`
		} `json:"function"`
	}
	if strictJSON(raw, &fn) != nil || fn.Type != "function" || !names[fn.Function.Name] {
		return toolChoice{}, ErrRequest
	}
	return toolChoice{kind: "tool", name: fn.Function.Name}, nil
}

func convertMessages(input []message) ([]map[string]any, []map[string]any, error) {
	result := make([]map[string]any, 0, len(input))
	var system []map[string]any
	known := make(map[string]string)
	answered := make(map[string]bool)
	for _, m := range input {
		switch m.Role {
		case "system", "developer", "user", "assistant", "tool":
		default:
			return nil, nil, ErrRequest
		}
		if m.Role != "assistant" && (len(m.ToolCalls) > 0 || m.ReasoningContent != "") {
			return nil, nil, ErrRequest
		}
		if m.Role != "tool" && m.ToolCallID != "" {
			return nil, nil, ErrRequest
		}
		parts, err := contentParts(m.Content, m.Role == "user")
		if err != nil {
			return nil, nil, err
		}
		if m.Role == "system" || m.Role == "developer" {
			system = append(system, parts...)
			continue
		}
		if m.Role == "tool" {
			name := known[m.ToolCallID]
			if name == "" || answered[m.ToolCallID] {
				return nil, nil, ErrRequest
			}
			answered[m.ToolCallID] = true
			texts := make([]string, 0, len(parts))
			for _, part := range parts {
				texts = append(texts, part["text"].(string))
			}
			parts = []map[string]any{{"type": "tool-result", "toolCallId": m.ToolCallID, "toolName": name, "output": map[string]string{"type": "text", "value": strings.Join(texts, "\n")}}}
		}
		if m.ReasoningContent != "" {
			parts = append([]map[string]any{{"type": "reasoning", "text": m.ReasoningContent}}, parts...)
		}
		for _, call := range m.ToolCalls {
			if call.Type != "function" || call.ID == "" || len(call.ID) > 512 || known[call.ID] != "" || !validName(call.Function.Name) || !jsonObject([]byte(call.Function.Arguments)) {
				return nil, nil, ErrRequest
			}
			known[call.ID] = call.Function.Name
			parts = append(parts, map[string]any{"type": "tool-call", "toolCallId": call.ID, "toolName": call.Function.Name, "input": json.RawMessage(call.Function.Arguments)})
		}
		if len(parts) == 0 {
			return nil, nil, ErrRequest
		}
		result = append(result, map[string]any{"role": m.Role, "content": parts})
	}
	if len(result) == 0 {
		return nil, nil, ErrRequest
	}
	return result, system, nil
}

func contentParts(raw json.RawMessage, images bool) ([]map[string]any, error) {
	if absent(raw) {
		return nil, nil
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return []map[string]any{{"type": "text", "text": text}}, nil
	}
	var input []struct {
		Type     string  `json:"type"`
		Text     *string `json:"text"`
		ImageURL *struct {
			URL    string `json:"url"`
			Detail string `json:"detail"`
		} `json:"image_url"`
		CacheControl json.RawMessage `json:"cache_control"`
	}
	if strictJSON(raw, &input) != nil {
		return nil, ErrRequest
	}
	result := make([]map[string]any, 0, len(input))
	for _, p := range input {
		var part map[string]any
		switch p.Type {
		case "text":
			if p.Text == nil || p.ImageURL != nil {
				return nil, ErrRequest
			}
			part = map[string]any{"type": "text", "text": *p.Text}
		case "image_url":
			if !images || p.ImageURL == nil || p.Text != nil || (p.ImageURL.Detail != "" && p.ImageURL.Detail != "auto") {
				return nil, ErrRequest
			}
			header, data, ok := strings.Cut(p.ImageURL.URL, ",")
			if !ok || !strings.HasPrefix(header, "data:image/") || !strings.HasSuffix(header, ";base64") {
				return nil, ErrRequest
			}
			if decoded, err := base64.StdEncoding.DecodeString(data); err != nil || len(decoded) == 0 {
				return nil, ErrRequest
			}
			media := strings.TrimSuffix(strings.TrimPrefix(header, "data:"), ";base64")
			part = map[string]any{"type": "image", "image": p.ImageURL.URL, "mimeType": media}
		default:
			return nil, ErrRequest
		}
		if !absent(p.CacheControl) {
			var cache struct {
				Type string `json:"type"`
				TTL  string `json:"ttl"`
			}
			if strictJSON(p.CacheControl, &cache) != nil || cache.Type != "ephemeral" || (cache.TTL != "" && cache.TTL != "5m" && cache.TTL != "1h") {
				return nil, ErrRequest
			}
			part["cache_control"] = p.CacheControl
		}
		result = append(result, part)
	}
	return result, nil
}

func absent(raw []byte) bool {
	return len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}
func jsonObject(raw []byte) bool {
	raw = bytes.TrimSpace(raw)
	return len(raw) > 1 && raw[0] == '{' && json.Valid(raw)
}
func validName(name string) bool {
	if len(name) == 0 || len(name) > 128 {
		return false
	}
	for _, ch := range name {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '_' || ch == '-') {
			return false
		}
	}
	return true
}
func uuid() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6], b[8] = b[6]&15|64, b[8]&63|128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}
