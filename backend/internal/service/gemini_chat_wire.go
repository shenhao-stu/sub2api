package service

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/tidwall/sjson"
)

func decodeGeminiChatObject(raw []byte, out *map[string]any) error {
	if !json.Valid(raw) {
		return fmt.Errorf("invalid Gemini JSON response")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	return decoder.Decode(out)
}

func geminiChatResponseWire(response *apicompat.ChatCompletionsResponse, native map[string]any) (json.RawMessage, error) {
	raw, err := json.Marshal(response)
	if err != nil {
		return nil, err
	}
	index := 0
	for _, part := range extractGeminiParts(native) {
		if call, ok := part["functionCall"].(map[string]any); !ok || call == nil {
			continue
		}
		signature, _ := part["thoughtSignature"].(string)
		if signature != "" {
			raw, err = sjson.SetBytes(raw, fmt.Sprintf("choices.0.message.tool_calls.%d.extra_content.google.thought_signature", index), signature)
			if err != nil {
				return nil, err
			}
		}
		index++
	}
	return raw, nil
}

func geminiChatChunkSSE(chunk apicompat.ChatCompletionsChunk, signatures map[string]string) (string, error) {
	raw, err := json.Marshal(chunk)
	if err != nil {
		return "", err
	}
	for i, choice := range chunk.Choices {
		for j, call := range choice.Delta.ToolCalls {
			if signature := signatures[call.ID]; signature != "" {
				raw, err = sjson.SetBytes(raw, fmt.Sprintf("choices.%d.delta.tool_calls.%d.extra_content.google.thought_signature", i, j), signature)
				if err != nil {
					return "", err
				}
			}
		}
	}
	return "data: " + string(raw) + "\n\n", nil
}

func geminiChatToolConfig(req *apicompat.ChatCompletionsRequest) ([]byte, *openAIClientErrorFields) {
	choice := req.ToolChoice
	if len(choice) == 0 {
		choice = req.FunctionCall
	}
	if len(choice) == 0 || bytes.Equal(bytes.TrimSpace(choice), []byte("null")) {
		return nil, nil
	}
	var mode string
	var names []string
	var text string
	if json.Unmarshal(choice, &text) == nil {
		switch text {
		case "auto":
			mode = "AUTO"
		case "none":
			mode = "NONE"
		case "required":
			mode = "ANY"
		default:
			return nil, geminiChatInputError("tool_choice", "Use auto, none, required or a declared function name.")
		}
	} else {
		var value struct {
			Type     string `json:"type"`
			Name     string `json:"name"`
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		}
		if json.Unmarshal(choice, &value) != nil || (value.Type != "" && value.Type != "function") {
			return nil, geminiChatInputError("tool_choice", "Invalid function choice.")
		}
		name := value.Function.Name
		if name == "" {
			name = value.Name
		}
		declared := false
		for _, tool := range req.Tools {
			if tool.Type == "function" && tool.Function != nil && tool.Function.Name == name {
				declared = true
			}
		}
		for _, function := range req.Functions {
			if function.Name == name {
				declared = true
			}
		}
		if name == "" || !declared {
			return nil, geminiChatInputError("tool_choice", "The selected function must be declared in tools or functions.")
		}
		mode = "ANY"
		names = []string{name}
	}
	config := map[string]any{"mode": mode}
	if len(names) > 0 {
		config["allowedFunctionNames"] = names
	}
	encoded, _ := json.Marshal(map[string]any{"functionCallingConfig": config})
	return encoded, nil
}
