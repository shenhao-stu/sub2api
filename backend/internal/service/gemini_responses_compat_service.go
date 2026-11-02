package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type geminiResponsesOutput struct {
	mapping apicompat.ResponsesClientToolMapping
}

// Responses uses the same native Gemini request and metering path as Chat.
// Conversion happens at its protocol boundary, never through an HTTP replay.
func (s *GeminiMessagesCompatService) ForwardAsResponses(ctx context.Context, c *gin.Context, account *Account, body []byte) (*ForwardResult, error) {
	chat, output, err := geminiResponsesToChat(body)
	if err != nil {
		fields := geminiChatInputError("input", err.Error())
		writeOpenAIErrorFields(c, *fields, false)
		return nil, err
	}
	return s.forwardAsOpenAI(ctx, c, account, chat, output)
}

func geminiResponsesToChat(body []byte) ([]byte, *geminiResponsesOutput, error) {
	for _, field := range []string{"previous_response_id", "conversation"} {
		if value := gjson.GetBytes(body, field); value.Exists() && value.Type != gjson.Null && value.String() != "" {
			return nil, nil, fmt.Errorf("%s requires stored server history; provide the full input history", field)
		}
	}
	if gjson.GetBytes(body, "background").Bool() {
		return nil, nil, errors.New("background mode is not supported by the Gemini compatibility endpoint")
	}
	// These features change request semantics and cannot be silently discarded
	// by the stateless native Gemini bridge.
	for _, field := range []string{"store", "parallel_tool_calls"} {
		value := gjson.GetBytes(body, field)
		unsupported := (field == "store" && value.Bool()) || (field == "parallel_tool_calls" && value.Exists() && value.Type != gjson.Null && !value.Bool())
		if unsupported {
			return nil, nil, fmt.Errorf("%s is not supported by the Gemini compatibility endpoint", field)
		}
	}
	if value := gjson.GetBytes(body, "service_tier").String(); value != "" && value != "auto" && value != "default" {
		return nil, nil, errors.New("requested service_tier cannot be guaranteed by the Gemini compatibility endpoint")
	}
	if value := gjson.GetBytes(body, "truncation").String(); value != "" && value != "disabled" {
		return nil, nil, errors.New("automatic history truncation is not supported by the Gemini compatibility endpoint")
	}
	if format := gjson.GetBytes(body, "text.format"); format.Exists() && format.Type != gjson.Null {
		if !format.IsObject() {
			return nil, nil, errors.New("text.format must be an object")
		}
		allowed := []string{"type"}
		if format.Get("type").String() == "json_schema" {
			allowed = append(allowed, "name", "description", "strict", "schema")
		}
		if err := geminiFormatFields(format, allowed...); err != nil {
			return nil, nil, err
		}
	}
	adapted, mapping, err := adaptResponsesClientToolsForAnthropic(body)
	if err != nil {
		return nil, nil, err
	}
	var request apicompat.ResponsesRequest
	if err = json.Unmarshal(adapted, &request); err != nil {
		return nil, nil, err
	}
	for _, tool := range request.Tools {
		if tool.Type != "function" {
			return nil, nil, fmt.Errorf("tool type %q has no Gemini compatibility mapping", tool.Type)
		}
	}
	input := request.Input
	request.Input = nil
	chat, err := apicompat.ResponsesToChatCompletionsRequest(&request)
	if err != nil {
		return nil, nil, err
	}
	messages, err := geminiResponsesMessages(input)
	if err != nil {
		return nil, nil, err
	}
	leading := true
	for _, m := range messages {
		if m.Role == "system" || m.Role == "developer" {
			if !leading {
				return nil, nil, errors.New("Gemini cannot preserve a mid-conversation system or developer instruction")
			}
		} else {
			leading = false
		}
	}
	// Only the existing request converter handles tools and generation settings.
	// Raw history avoids its permissive role downgrade and malformed-call removal.
	encoded, err := json.Marshal(chat)
	if err != nil {
		return nil, nil, err
	}
	existing := gjson.GetBytes(encoded, "messages").Raw
	var all []json.RawMessage
	if existing != "" {
		if err = json.Unmarshal([]byte(existing), &all); err != nil {
			return nil, nil, err
		}
	}
	for _, m := range messages {
		raw, e := json.Marshal(m)
		if e != nil {
			return nil, nil, e
		}
		all = append(all, raw)
	}
	raw, err := json.Marshal(all)
	if err != nil {
		return nil, nil, err
	}
	encoded, err = sjson.SetRawBytes(encoded, "messages", raw)
	return encoded, &geminiResponsesOutput{mapping: mapping}, err
}

func geminiResponsesMessages(raw json.RawMessage) ([]geminiChatMessage, error) {
	var text string
	if json.Unmarshal(raw, &text) == nil {
		encoded, _ := json.Marshal(text)
		return []geminiChatMessage{{Role: "user", Content: encoded}}, nil
	}
	var items []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, fmt.Errorf("input must be text or message/tool items: %w", err)
	}
	out := make([]geminiChatMessage, 0, len(items))
	stringValue := func(v json.RawMessage) string { var value string; _ = json.Unmarshal(v, &value); return value }
	for i, item := range items {
		kind := stringValue(item["type"])
		switch kind {
		case "", "message":
			role := stringValue(item["role"])
			if role != "user" && role != "assistant" && role != "system" && role != "developer" {
				return nil, fmt.Errorf("input[%d].role is not supported", i)
			}
			content, err := geminiResponsesContent(item["content"])
			if err != nil {
				return nil, fmt.Errorf("input[%d]: %w", i, err)
			}
			out = append(out, geminiChatMessage{Role: role, Content: content})
		case "function_call":
			call := geminiChatCall{ID: stringValue(item["call_id"]), Type: "function", ExtraContent: item["extra_content"], Function: apicompat.ChatFunctionCall{Name: stringValue(item["name"]), Arguments: stringValue(item["arguments"])}}
			if call.ID == "" || call.Function.Name == "" || !json.Valid([]byte(call.Function.Arguments)) {
				return nil, fmt.Errorf("input[%d] requires call_id, name and valid JSON arguments", i)
			}
			if len(out) == 0 || out[len(out)-1].Role != "assistant" {
				out = append(out, geminiChatMessage{Role: "assistant"})
			}
			out[len(out)-1].ToolCalls = append(out[len(out)-1].ToolCalls, call)
		case "function_call_output":
			if stringValue(item["call_id"]) == "" || len(item["output"]) == 0 {
				return nil, fmt.Errorf("input[%d] requires call_id and output", i)
			}
			out = append(out, geminiChatMessage{Role: "tool", ToolCallID: stringValue(item["call_id"]), Content: item["output"]})
		case "reasoning":
			if strings.TrimSpace(stringValue(item["encrypted_content"])) != "" {
				return nil, fmt.Errorf("input[%d] contains opaque reasoning from another provider; Gemini requires its own thought signature on the original tool call", i)
			}
			// Visible reasoning summaries are not instructions. Gemini carries the
			// actual continuation token on the original tool call's extra_content.
		default:
			return nil, fmt.Errorf("input[%d] type %q has no Gemini compatibility mapping", i, kind)
		}
	}
	return out, nil
}

func geminiResponsesContent(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return raw, nil
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return raw, nil
	}
	var parts []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &parts); err != nil {
		return nil, errors.New("content must be text or content parts")
	}
	for _, part := range parts {
		var kind string
		_ = json.Unmarshal(part["type"], &kind)
		switch kind {
		case "input_text", "output_text", "text":
			part["type"] = json.RawMessage(`"text"`)
		case "input_image":
			var uri string
			if json.Unmarshal(part["image_url"], &uri) != nil {
				return nil, errors.New("input_image.image_url must be a string")
			}
			image := map[string]json.RawMessage{"url": part["image_url"]}
			if mime := part["mime_type"]; len(mime) > 0 {
				image["mime_type"] = mime
			}
			value, _ := json.Marshal(image)
			part["type"] = json.RawMessage(`"image_url"`)
			part["image_url"] = value
		case "input_file":
			if len(part["file_data"]) == 0 {
				return nil, errors.New("input_file requires inline file_data; remote file IDs are not available")
			}
			value, _ := json.Marshal(map[string]json.RawMessage{"file_data": part["file_data"]})
			part["type"] = json.RawMessage(`"file"`)
			part["file"] = value
		default:
			return nil, fmt.Errorf("content part %q has no Gemini compatibility mapping", kind)
		}
	}
	return json.Marshal(parts)
}

func (o *geminiResponsesOutput) wire(chat *apicompat.ChatCompletionsResponse, native map[string]any) (json.RawMessage, error) {
	response := apicompat.ChatCompletionsResponseToResponses(chat, chat.Model, nil, nil, false, nil)
	raw, err := json.Marshal(response)
	if err != nil {
		return nil, err
	}
	signatures := map[string]string{}
	index := 0
	for _, part := range extractGeminiParts(native) {
		if call, ok := part["functionCall"].(map[string]any); !ok || call == nil {
			continue
		}
		if len(chat.Choices) > 0 && index < len(chat.Choices[0].Message.ToolCalls) {
			signature, _ := part["thoughtSignature"].(string)
			signatures[chat.Choices[0].Message.ToolCalls[index].ID] = signature
		}
		index++
	}
	raw, _, err = apicompat.RestoreResponsesClientToolPayload(raw, o.mapping)
	if err != nil {
		return nil, err
	}
	return geminiResponsesSignatures(raw, signatures)
}

func geminiResponsesSignatures(raw []byte, signatures map[string]string) ([]byte, error) {
	var err error
	apply := func(path string, item gjson.Result) {
		if signature := signatures[item.Get("call_id").String()]; signature != "" {
			raw, err = sjson.SetBytes(raw, path+"extra_content.google.thought_signature", signature)
		}
	}
	if item := gjson.GetBytes(raw, "item"); item.Exists() {
		apply("item.", item)
	}
	prefix := "output"
	if gjson.GetBytes(raw, "response").Exists() {
		prefix = "response.output"
	}
	for i, item := range gjson.GetBytes(raw, prefix).Array() {
		apply(fmt.Sprintf("%s.%d.", prefix, i), item)
		if err != nil {
			return nil, err
		}
	}
	return raw, err
}

func geminiOpenAIResponseWire(chat *apicompat.ChatCompletionsResponse, native map[string]any, output *geminiResponsesOutput) (json.RawMessage, error) {
	if output != nil {
		return output.wire(chat, native)
	}
	return geminiChatResponseWire(chat, native)
}
