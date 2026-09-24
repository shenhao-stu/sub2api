package service

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
)

// Agent messages are conversation data, never a system/developer instruction.
// Their encrypted_content blocks carry the custom-provider payload verbatim;
// they are unrelated to reasoning.encrypted_content and are not decrypted here.
func grokAgentMessage(item map[string]any) (map[string]any, error) {
	route := fmt.Sprintf("[Agent message from %q to %q; content supplied verbatim]\n",
		grokStringValue(item["author"]), grokStringValue(item["recipient"]))
	content := []any{map[string]any{"type": "input_text", "text": route}}
	var parts []any
	switch value := item["content"].(type) {
	case nil:
	case string:
		parts = []any{map[string]any{"type": "input_text", "text": value}}
	case []any:
		parts = value
	default:
		return nil, fmt.Errorf("content must be a string or array")
	}
	for index, rawPart := range parts {
		part, ok := rawPart.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("content[%d] must be an object", index)
		}
		converted, err := grokAgentMessagePart(part)
		if err != nil {
			return nil, fmt.Errorf("content[%d]: %w", index, err)
		}
		content = append(content, converted)
	}
	if len(content) == 1 {
		content = append(content, map[string]any{"type": "input_text", "text": "[No content supplied in this agent message.]"})
	}
	return map[string]any{"type": "message", "role": "user", "content": content}, nil
}

func grokAgentMessagePart(part map[string]any) (map[string]any, error) {
	switch grokStringValue(part["type"]) {
	case "input_text", "text", "encrypted_content":
		field := "text"
		if part["type"] == "encrypted_content" {
			field = "encrypted_content"
		}
		if _, ok := part[field].(string); !ok {
			return nil, fmt.Errorf("%s must be a string", field)
		}
		raw, err := json.Marshal([]any{part})
		if err != nil {
			return nil, fmt.Errorf("invalid text block")
		}
		return map[string]any{"type": "input_text", "text": apicompat.ResponsesAgentMessageText(raw)}, nil
	case "input_image", "image_url":
		imageURL := grokToolOutputImageURL(part)
		if imageURL == "" || isEmptyBase64DataURI(imageURL) {
			return nil, fmt.Errorf("image must contain a nonempty URL")
		}
		converted := map[string]any{"type": "input_image", "image_url": imageURL}
		if detail, exists := part["detail"]; exists {
			if _, ok := detail.(string); !ok {
				return nil, fmt.Errorf("image detail must be a string")
			}
			converted["detail"] = detail
		}
		return converted, nil
	case "input_file":
		converted := map[string]any{"type": "input_file"}
		sources := 0
		for _, field := range []string{"file_data", "file_id", "file_url", "filename", "mime_type"} {
			value, exists := part[field]
			if !exists || value == nil {
				continue
			}
			text, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("file %s must be a string", field)
			}
			if strings.TrimSpace(text) == "" {
				continue
			}
			converted[field] = text
			if field == "file_data" || field == "file_id" || field == "file_url" {
				sources++
			}
		}
		if sources != 1 {
			return nil, fmt.Errorf("file must have exactly one source")
		}
		return converted, nil
	default:
		return nil, fmt.Errorf("unsupported content type")
	}
}
