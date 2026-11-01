package service

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/tidwall/sjson"
)

type geminiChatCall struct {
	ID           string                     `json:"id"`
	Type         string                     `json:"type"`
	Function     apicompat.ChatFunctionCall `json:"function"`
	ExtraContent json.RawMessage            `json:"extra_content"`
}

type geminiChatMessage struct {
	Role         string                      `json:"role"`
	Content      json.RawMessage             `json:"content"`
	Name         string                      `json:"name"`
	ToolCallID   string                      `json:"tool_call_id"`
	ToolCalls    []geminiChatCall            `json:"tool_calls"`
	FunctionCall *apicompat.ChatFunctionCall `json:"function_call"`
}

type geminiChatContent struct {
	Role  string           `json:"role"`
	Parts []map[string]any `json:"parts"`
}

type geminiChatPendingCall struct {
	name   string
	result map[string]any
}

func geminiChatInputError(param, message string) *openAIClientErrorFields {
	return &openAIClientErrorFields{Type: "invalid_request_error", Code: "unsupported_conversation_turn", Param: param, Message: message, StatusCode: http.StatusBadRequest}
}

// Gemini v1 pairs function responses by name and call order, not Chat IDs.
// IDs resolve only within the pending model turn; no history is moved across it.
func geminiChatContents(body []byte) ([]byte, *openAIClientErrorFields) {
	var req struct {
		Messages []geminiChatMessage `json:"messages"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, geminiChatInputError("messages", "Invalid Chat message structure.")
	}
	var out []geminiChatContent
	var pending []geminiChatPendingCall
	var pendingByID map[string]int
	var pendingByName map[string]map[int]struct{}
	var slots []int
	flush := func(param string) *openAIClientErrorFields {
		if len(pending) == 0 {
			return nil
		}
		if len(slots) != len(pending) {
			return geminiChatInputError(param, "Return one result for each pending tool call before another model turn.")
		}
		for i, call := range pending {
			if call.result == nil {
				return geminiChatInputError(param, "A pending tool call has no matching result.")
			}
			out[len(out)-1].Parts[slots[i]] = call.result
		}
		pending = nil
		pendingByID = nil
		pendingByName = nil
		slots = nil
		return nil
	}
	appendUser := func(parts []map[string]any) {
		if len(parts) == 0 {
			return
		}
		if len(out) > 0 && out[len(out)-1].Role == "user" {
			out[len(out)-1].Parts = append(out[len(out)-1].Parts, parts...)
		} else {
			out = append(out, geminiChatContent{Role: "user", Parts: parts})
		}
	}
	for i, m := range req.Messages {
		param := fmt.Sprintf("messages[%d]", i)
		switch m.Role {
		case "system", "developer":
			continue
		case "assistant":
			if err := flush(param); err != nil {
				return nil, err
			}
			parts, err := geminiChatContentParts(m.Content, param+".content", true)
			if err != nil {
				return nil, err
			}
			calls := m.ToolCalls
			if m.FunctionCall != nil {
				if len(calls) > 0 {
					return nil, geminiChatInputError(param, "Use either tool_calls or function_call, not both.")
				}
				calls = []geminiChatCall{{ID: fmt.Sprintf("legacy_%d", i), Type: "function", Function: *m.FunctionCall}}
			}
			signatures := make([]string, len(calls))
			signed := false
			for j, call := range calls {
				signature, err := geminiChatSignature(call.ExtraContent, fmt.Sprintf("%s.tool_calls[%d].extra_content", param, j))
				if err != nil {
					return nil, err
				}
				signatures[j] = signature
				signed = signed || signature != ""
			}
			pendingByID = make(map[string]int, len(calls))
			pendingByName = make(map[string]map[int]struct{})
			for j, call := range calls {
				cp := fmt.Sprintf("%s.tool_calls[%d]", param, j)
				_, duplicateID := pendingByID[call.ID]
				if call.ID == "" || duplicateID || strings.TrimSpace(call.Function.Name) == "" || (call.Type != "" && call.Type != "function") {
					return nil, geminiChatInputError(cp, "Tool calls require a unique ID, function type and non-empty function name.")
				}
				args := json.RawMessage(call.Function.Arguments)
				if !json.Valid(args) || len(bytes.TrimSpace(args)) == 0 || bytes.TrimSpace(args)[0] != '{' {
					return nil, geminiChatInputError(cp+".function.arguments", "Function arguments must be a JSON object.")
				}
				signature := signatures[j]
				if signature == "" && !signed {
					signature = geminiDummyThoughtSignature
				}
				part := map[string]any{"functionCall": map[string]any{"name": call.Function.Name, "args": args}}
				if signature != "" {
					part["thoughtSignature"] = signature
				}
				parts = append(parts, part)
				pendingByID[call.ID] = len(pending)
				if pendingByName[call.Function.Name] == nil {
					pendingByName[call.Function.Name] = make(map[int]struct{})
				}
				pendingByName[call.Function.Name][len(pending)] = struct{}{}
				pending = append(pending, geminiChatPendingCall{name: call.Function.Name})
			}
			if len(parts) > 0 {
				out = append(out, geminiChatContent{Role: "model", Parts: parts})
			}
		case "user":
			parts, err := geminiChatContentParts(m.Content, param+".content", false)
			if err != nil {
				return nil, err
			}
			appendUser(parts)
		case "tool", "function":
			index, found := pendingByID[m.ToolCallID]
			if m.Role == "function" && m.ToolCallID == "" {
				remaining := pendingByName[m.Name]
				if len(remaining) > 1 {
					return nil, geminiChatInputError(param, "Ambiguous function result; use tool_call_id.")
				}
				found = false
				for candidate := range remaining {
					index = candidate
					found = true
				}
			}
			if !found {
				index = -1
			}
			if index < 0 || pending[index].result != nil {
				return nil, geminiChatInputError(param+".tool_call_id", "Tool result does not match an unanswered call in the preceding model turn.")
			}
			if !json.Valid(m.Content) {
				return nil, geminiChatInputError(param+".content", "Tool result content must be a JSON value.")
			}
			response := any(m.Content)
			if bytes.TrimSpace(m.Content)[0] != '{' {
				response = map[string]any{"output": m.Content}
			}
			pending[index].result = map[string]any{"functionResponse": map[string]any{"name": pending[index].name, "response": response}}
			delete(pendingByName[pending[index].name], index)
			appendUser([]map[string]any{nil})
			slots = append(slots, len(out[len(out)-1].Parts)-1)
		default:
			return nil, geminiChatInputError(param+".role", "Unsupported message role.")
		}
	}
	if err := flush("messages"); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(splitGeminiChatToolResponseTails(out))
	if err != nil {
		return nil, geminiChatInputError("messages", "Unable to encode conversation.")
	}
	return encoded, nil
}

// Vertex rejects text/media after a functionResponse in the same user Content.
// Split that boundary without moving parts. Apply to every historical turn so
// extending a conversation does not change the already translated prefix.
func splitGeminiChatToolResponseTails(contents []geminiChatContent) []geminiChatContent {
	out := make([]geminiChatContent, 0, len(contents))
	for _, content := range contents {
		if content.Role != "user" {
			out = append(out, content)
			continue
		}
		start := 0
		seenResponse := false
		for i, part := range content.Parts {
			if _, isResponse := part["functionResponse"]; isResponse {
				seenResponse = true
				continue
			}
			if seenResponse {
				out = append(out, geminiChatContent{Role: content.Role, Parts: content.Parts[start:i]})
				start = i
				seenResponse = false
			}
		}
		out = append(out, geminiChatContent{Role: content.Role, Parts: content.Parts[start:]})
	}
	return out
}

func geminiChatSignature(raw json.RawMessage, param string) (string, *openAIClientErrorFields) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", nil
	}
	var extra struct {
		Google struct {
			ThoughtSignature string `json:"thought_signature"`
		} `json:"google"`
	}
	if json.Unmarshal(raw, &extra) != nil {
		return "", geminiChatInputError(param, "Invalid Google thought signature metadata.")
	}
	return extra.Google.ThoughtSignature, nil
}

func geminiChatContentParts(raw json.RawMessage, param string, assistant bool) ([]map[string]any, *openAIClientErrorFields) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		if assistant && strings.TrimSpace(text) == "" {
			return nil, nil
		}
		return []map[string]any{{"text": text}}, nil
	}
	var blocks []struct {
		Type     string `json:"type"`
		Text     string `json:"text"`
		ImageURL struct {
			URL      string `json:"url"`
			MIMEType string `json:"mime_type"`
		} `json:"image_url"`
		File struct {
			FileData string `json:"file_data"`
		} `json:"file"`
		ExtraContent json.RawMessage `json:"extra_content"`
	}
	if json.Unmarshal(raw, &blocks) != nil {
		return nil, geminiChatInputError(param, "Message content must be text or a supported content-parts array.")
	}
	var parts []map[string]any
	for i, block := range blocks {
		bp := fmt.Sprintf("%s[%d]", param, i)
		var part map[string]any
		switch block.Type {
		case "text":
			part = map[string]any{"text": block.Text}
		case "thinking", "reasoning", "redacted_thinking":
			if assistant {
				continue
			}
			return nil, geminiChatInputError(bp, "Reasoning content is only supported on assistant history.")
		case "image_url":
			var err *openAIClientErrorFields
			part, err = geminiChatMediaPart(block.ImageURL.URL, block.ImageURL.MIMEType, bp+".image_url", true)
			if err != nil {
				return nil, err
			}
		case "file":
			var err *openAIClientErrorFields
			part, err = geminiChatMediaPart(block.File.FileData, "", bp+".file.file_data", false)
			if err != nil {
				return nil, err
			}
		default:
			return nil, geminiChatInputError(bp+".type", "This content type cannot be represented in the Gemini Chat interface.")
		}
		signature, err := geminiChatSignature(block.ExtraContent, bp+".extra_content")
		if err != nil {
			return nil, err
		}
		if signature != "" {
			part["thoughtSignature"] = signature
		}
		parts = append(parts, part)
	}
	return parts, nil
}

func geminiChatMediaPart(uri, declaredMIME, param string, image bool) (map[string]any, *openAIClientErrorFields) {
	if strings.HasPrefix(uri, "data:") {
		meta, data, ok := strings.Cut(strings.TrimPrefix(uri, "data:"), ",")
		if !ok || !strings.HasSuffix(meta, ";base64") || data == "" {
			return nil, geminiChatInputError(param, "Use a non-empty base64 data URI.")
		}
		kind, _, err := mime.ParseMediaType(strings.TrimSuffix(meta, ";base64"))
		if err != nil || (image && !strings.HasPrefix(kind, "image/")) {
			return nil, geminiChatInputError(param, "Invalid media MIME type.")
		}
		if _, err := io.Copy(io.Discard, base64.NewDecoder(base64.StdEncoding, strings.NewReader(data))); err != nil {
			return nil, geminiChatInputError(param, "Invalid base64 media data.")
		}
		return map[string]any{"inlineData": map[string]any{"mimeType": kind, "data": data}}, nil
	}
	u, err := url.Parse(uri)
	if !image || err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil {
		return nil, geminiChatInputError(param, "Use an inline data URI or a public HTTP(S) image URI with mime_type.")
	}
	host := strings.ToLower(u.Hostname())
	ip := net.ParseIP(host)
	if host == "localhost" || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".local") || (ip != nil && (ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsUnspecified())) {
		return nil, geminiChatInputError(param, "Image URI must identify a public resource.")
	}
	kind, _, err := mime.ParseMediaType(declaredMIME)
	if err != nil || !strings.HasPrefix(kind, "image/") {
		return nil, geminiChatInputError(param+".mime_type", "Remote Gemini images require an explicit image MIME type; no URL is downloaded or guessed.")
	}
	return map[string]any{"fileData": map[string]any{"fileUri": uri, "mimeType": kind}}, nil
}

// Keep the existing tool/config spelling normalization without re-decoding
// conversation values through float64.
func normalizeGeminiChatRequest(body []byte) []byte {
	var req map[string]json.RawMessage
	if json.Unmarshal(body, &req) != nil {
		return body
	}
	var tools []map[string]json.RawMessage
	if json.Unmarshal(req["tools"], &tools) != nil {
		return body
	}
	changed := false
	for _, tool := range tools {
		if search, ok := tool["googleSearch"]; ok {
			if _, exists := tool["google_search"]; !exists {
				tool["google_search"] = search
				delete(tool, "googleSearch")
				changed = true
			}
		}
	}
	if !changed {
		return body
	}
	encoded, err := json.Marshal(tools)
	if err != nil {
		return body
	}
	patched, err := sjson.SetRawBytes(body, "tools", encoded)
	if err != nil {
		return body
	}
	return patched
}
