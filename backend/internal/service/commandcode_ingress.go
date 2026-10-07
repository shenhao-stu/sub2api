package service

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/commandcode"
)

type commandCodeFields map[string]json.RawMessage

// Validate before compatibility conversion: those converters intentionally drop
// provider-specific options which a native GO account must never silently accept.
func validateCommandCodeGoIngress(body []byte, protocol, path string) error {
	if len(body) == 0 || len(body) > commandcode.MaxRequestBytes || strings.HasSuffix(strings.TrimRight(path, "/"), "/compact") {
		return unsupportedCommandCodeFeature()
	}
	var fields commandCodeFields
	if json.Unmarshal(body, &fields) != nil || fields == nil {
		return unsupportedCommandCodeFeature()
	}
	switch protocol {
	case "chat":
		return commandCodeAllowedFields(fields, "model messages tools tool_choice max_tokens max_completion_tokens stream stream_options temperature reasoning_effort parallel_tool_calls n")
	case "responses":
		if err := commandCodeAllowedFields(fields, "model instructions input max_output_tokens temperature stream tools tool_choice parallel_tool_calls reasoning store"); err != nil {
			return err
		}
		if !commandCodeFalseOrAbsent(fields["store"]) {
			return unsupportedCommandCodeFeature()
		}
		if raw := fields["reasoning"]; commandCodePresent(raw) {
			reasoning, err := commandCodeObject(raw, "effort")
			if err != nil {
				return err
			}
			if effort, ok := commandCodeString(reasoning["effort"]); !ok || effort == "none" {
				return unsupportedCommandCodeFeature()
			}
		}
		names, err := commandCodeTools(fields["tools"], protocol)
		if err != nil {
			return err
		}
		if err := commandCodeResponsesInput(fields["input"], names); err != nil {
			return err
		}
		return commandCodeChoice(fields["tool_choice"], protocol, names)
	case "messages":
		if err := commandCodeAllowedFields(fields, "model max_tokens system messages tools stream temperature tool_choice output_config"); err != nil {
			return err
		}
		var limit int
		if json.Unmarshal(fields["max_tokens"], &limit) != nil || limit < 128 {
			return unsupportedCommandCodeFeature()
		}
		if raw := fields["output_config"]; commandCodePresent(raw) {
			config, err := commandCodeObject(raw, "effort")
			if err != nil {
				return err
			}
			if _, ok := commandCodeString(config["effort"]); !ok {
				return unsupportedCommandCodeFeature()
			}
		}
		names, err := commandCodeTools(fields["tools"], protocol)
		if err != nil {
			return err
		}
		if err := commandCodeChoice(fields["tool_choice"], protocol, names); err != nil {
			return err
		}
		if commandCodePresent(fields["system"]) {
			if err := commandCodeAnthropicContent(fields["system"], "system", false); err != nil {
				return err
			}
		}
		messages, err := commandCodeArray(fields["messages"])
		if err != nil || len(messages) == 0 {
			return unsupportedCommandCodeFeature()
		}
		for _, raw := range messages {
			message, err := commandCodeObject(raw, "role content")
			if err != nil {
				return err
			}
			role, _ := commandCodeString(message["role"])
			if role != "user" && role != "assistant" {
				return unsupportedCommandCodeFeature()
			}
			if err := commandCodeAnthropicContent(message["content"], role, false); err != nil {
				return err
			}
		}
		return nil
	default:
		return unsupportedCommandCodeFeature()
	}
}

func unsupportedCommandCodeFeature() error {
	return fmt.Errorf("command code Go cannot preserve these request options; use supported text, inline images and function tools, or choose the Provider API")
}

func commandCodeAllowedFields(fields commandCodeFields, allowed string) error {
	for key := range fields {
		if !slices.Contains(strings.Fields(allowed), key) {
			return unsupportedCommandCodeFeature()
		}
	}
	return nil
}

func commandCodeObject(raw []byte, allowed string) (commandCodeFields, error) {
	var fields commandCodeFields
	if json.Unmarshal(raw, &fields) != nil || fields == nil {
		return nil, unsupportedCommandCodeFeature()
	}
	return fields, commandCodeAllowedFields(fields, allowed)
}

func commandCodeArray(raw []byte) ([]json.RawMessage, error) {
	var values []json.RawMessage
	if json.Unmarshal(raw, &values) != nil || len(values) > 4096 {
		return nil, unsupportedCommandCodeFeature()
	}
	return values, nil
}

func commandCodeString(raw []byte) (string, bool) {
	var value string
	if !commandCodePresent(raw) || json.Unmarshal(raw, &value) != nil {
		return "", false
	}
	return value, true
}

func commandCodePresent(raw []byte) bool {
	return len(raw) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func commandCodeFalseOrAbsent(raw []byte) bool {
	if !commandCodePresent(raw) {
		return true
	}
	var flag bool
	return json.Unmarshal(raw, &flag) == nil && !flag
}

func commandCodeTools(raw []byte, protocol string) (map[string]bool, error) {
	names := make(map[string]bool)
	if !commandCodePresent(raw) {
		return names, nil
	}
	tools, err := commandCodeArray(raw)
	if err != nil || len(tools) > 256 {
		return nil, unsupportedCommandCodeFeature()
	}
	for _, rawTool := range tools {
		allowed := "type name description parameters strict"
		if protocol == "messages" {
			allowed = "name description input_schema"
		}
		tool, err := commandCodeObject(rawTool, allowed)
		if err != nil {
			return nil, err
		}
		name, ok := commandCodeString(tool["name"])
		if !ok || name == "" || names[name] {
			return nil, unsupportedCommandCodeFeature()
		}
		if protocol == "responses" {
			kind, _ := commandCodeString(tool["type"])
			if kind != "function" && kind != "custom" {
				return nil, unsupportedCommandCodeFeature()
			}
			if kind == "custom" {
				if err := commandCodeAllowedFields(tool, "type name description"); err != nil {
					return nil, err
				}
			}
			if !commandCodeFalseOrAbsent(tool["strict"]) {
				return nil, unsupportedCommandCodeFeature()
			}
		}
		names[name] = true
	}
	return names, nil
}

func commandCodeChoice(raw []byte, protocol string, names map[string]bool) error {
	if !commandCodePresent(raw) {
		return nil
	}
	if choice, ok := commandCodeString(raw); ok {
		if protocol != "responses" || (choice != "auto" && choice != "none" && choice != "required") {
			return unsupportedCommandCodeFeature()
		}
		if choice == "required" && len(names) == 0 {
			return unsupportedCommandCodeFeature()
		}
		return nil
	}
	choice, err := commandCodeObject(raw, "type name")
	if err != nil {
		return err
	}
	kind, _ := commandCodeString(choice["type"])
	if protocol == "messages" && (kind == "auto" || kind == "any" || kind == "none") {
		if kind == "any" && len(names) == 0 {
			return unsupportedCommandCodeFeature()
		}
		if commandCodePresent(choice["name"]) {
			return unsupportedCommandCodeFeature()
		}
		return nil
	}
	if (protocol == "responses" && kind != "function" && kind != "custom") || (protocol == "messages" && kind != "tool") {
		return unsupportedCommandCodeFeature()
	}
	name, _ := commandCodeString(choice["name"])
	if !names[name] {
		return unsupportedCommandCodeFeature()
	}
	return nil
}

func commandCodeResponsesInput(raw []byte, names map[string]bool) error {
	if _, ok := commandCodeString(raw); ok {
		return nil
	}
	items, err := commandCodeArray(raw)
	if err != nil || len(items) == 0 {
		return unsupportedCommandCodeFeature()
	}
	seenConversation := false
	for _, rawItem := range items {
		item, err := commandCodeObject(rawItem, "type role content id status call_id name arguments output input summary encrypted_content tools")
		if err != nil {
			return err
		}
		kind, _ := commandCodeString(item["type"])
		if status, ok := commandCodeString(item["status"]); ok && status != "completed" {
			return unsupportedCommandCodeFeature()
		}
		switch kind {
		case "", "message":
			if err := commandCodeAllowedFields(item, "type role content id status"); err != nil {
				return err
			}
			role, _ := commandCodeString(item["role"])
			switch role {
			case "user", "assistant":
				seenConversation = true
			case "system", "developer":
				if seenConversation {
					return unsupportedCommandCodeFeature()
				}
			default:
				return unsupportedCommandCodeFeature()
			}
			if err := commandCodeResponsesContent(item["content"], role); err != nil {
				return err
			}
		case "function_call", "custom_tool_call":
			if err := commandCodeAllowedFields(item, "type id status call_id name arguments input"); err != nil {
				return err
			}
			id, _ := commandCodeString(item["call_id"])
			name, _ := commandCodeString(item["name"])
			if id == "" || name == "" {
				return unsupportedCommandCodeFeature()
			}
			if kind == "function_call" {
				args, ok := commandCodeString(item["arguments"])
				if !ok || !commandCodeJSONObject([]byte(args)) || commandCodePresent(item["input"]) {
					return unsupportedCommandCodeFeature()
				}
			} else if _, ok := commandCodeString(item["input"]); !ok || commandCodePresent(item["arguments"]) {
				return unsupportedCommandCodeFeature()
			}
			seenConversation = true
		case "function_call_output", "custom_tool_call_output":
			if err := commandCodeAllowedFields(item, "type id status call_id output"); err != nil {
				return err
			}
			if id, _ := commandCodeString(item["call_id"]); id == "" {
				return unsupportedCommandCodeFeature()
			}
			if _, ok := commandCodeString(item["output"]); !ok {
				return unsupportedCommandCodeFeature()
			}
			seenConversation = true
		case "additional_tools":
			if err := commandCodeAllowedFields(item, "type tools"); err != nil {
				return err
			}
			additional, err := commandCodeTools(item["tools"], "responses")
			if err != nil {
				return err
			}
			for name := range additional {
				if names[name] {
					return unsupportedCommandCodeFeature()
				}
				names[name] = true
			}
		default:
			return unsupportedCommandCodeFeature()
		}
	}
	return nil
}

func commandCodeResponsesContent(raw []byte, role string) error {
	if _, ok := commandCodeString(raw); ok {
		return nil
	}
	parts, err := commandCodeArray(raw)
	if err != nil || len(parts) == 0 {
		return unsupportedCommandCodeFeature()
	}
	for _, rawPart := range parts {
		part, err := commandCodeObject(rawPart, "type text image_url detail annotations")
		if err != nil {
			return err
		}
		kind, _ := commandCodeString(part["type"])
		switch kind {
		case "input_text", "output_text", "text":
			if err := commandCodeAllowedFields(part, "type text annotations"); err != nil {
				return err
			}
			if _, ok := commandCodeString(part["text"]); !ok {
				return unsupportedCommandCodeFeature()
			}
			if annotations := part["annotations"]; commandCodePresent(annotations) {
				values, err := commandCodeArray(annotations)
				if err != nil || len(values) > 0 {
					return unsupportedCommandCodeFeature()
				}
			}
		case "input_image":
			if err := commandCodeAllowedFields(part, "type image_url detail"); err != nil {
				return err
			}
			uri, _ := commandCodeString(part["image_url"])
			if role != "user" || !commandCodeInlineImage(uri) {
				return unsupportedCommandCodeFeature()
			}
			if detail, ok := commandCodeString(part["detail"]); ok && detail != "auto" {
				return unsupportedCommandCodeFeature()
			}
		default:
			return unsupportedCommandCodeFeature()
		}
	}
	return nil
}

func commandCodeAnthropicContent(raw []byte, role string, nested bool) error {
	if _, ok := commandCodeString(raw); ok {
		return nil
	}
	blocks, err := commandCodeArray(raw)
	if err != nil || len(blocks) == 0 {
		return unsupportedCommandCodeFeature()
	}
	for _, rawBlock := range blocks {
		block, err := commandCodeObject(rawBlock, "type text source id name input tool_use_id content is_error")
		if err != nil {
			return err
		}
		kind, _ := commandCodeString(block["type"])
		switch kind {
		case "text":
			if err := commandCodeAllowedFields(block, "type text"); err != nil {
				return err
			}
			if _, ok := commandCodeString(block["text"]); !ok {
				return unsupportedCommandCodeFeature()
			}
		case "image":
			if err := commandCodeAllowedFields(block, "type source"); err != nil {
				return err
			}
			source, err := commandCodeObject(block["source"], "type media_type data")
			if err != nil {
				return err
			}
			typeName, _ := commandCodeString(source["type"])
			media, _ := commandCodeString(source["media_type"])
			data, _ := commandCodeString(source["data"])
			if role != "user" || typeName != "base64" || !commandCodeInlineImage("data:"+media+";base64,"+data) {
				return unsupportedCommandCodeFeature()
			}
		case "tool_use":
			if err := commandCodeAllowedFields(block, "type id name input"); err != nil {
				return err
			}
			id, _ := commandCodeString(block["id"])
			name, _ := commandCodeString(block["name"])
			if role != "assistant" || nested || id == "" || name == "" || !commandCodeJSONObject(block["input"]) {
				return unsupportedCommandCodeFeature()
			}
		case "tool_result":
			if err := commandCodeAllowedFields(block, "type tool_use_id content is_error"); err != nil {
				return err
			}
			id, _ := commandCodeString(block["tool_use_id"])
			if role != "user" || nested || id == "" {
				return unsupportedCommandCodeFeature()
			}
			if !commandCodeFalseOrAbsent(block["is_error"]) {
				return unsupportedCommandCodeFeature()
			}
			if err := commandCodeAnthropicContent(block["content"], role, true); err != nil {
				return err
			}
		default:
			return unsupportedCommandCodeFeature()
		}
	}
	return nil
}

func commandCodeJSONObject(raw []byte) bool {
	var fields commandCodeFields
	return json.Unmarshal(raw, &fields) == nil && fields != nil
}

func commandCodeInlineImage(uri string) bool {
	header, data, ok := strings.Cut(uri, ",")
	if !ok || !strings.HasPrefix(header, "data:image/") || !strings.HasSuffix(header, ";base64") {
		return false
	}
	decoded, err := base64.StdEncoding.DecodeString(data)
	return err == nil && len(decoded) > 0
}
