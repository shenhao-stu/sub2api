package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/tidwall/gjson"
)

// Keep unknown model capabilities unchanged. These upstream models explicitly
// reject the last non-empty model turn: https://firebase.google.com/docs/ai-logic/chat.
func geminiRejectsAssistantPrefill(upstreamModel string) bool {
	name := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(upstreamModel)), "models/")
	return name == "gemini-4-argon" || name == "gemini-3.8-flash"
}

func geminiChatFinalTurnError(upstreamModel string, messages []apicompat.ChatMessage, nativeBody []byte) *openAIClientErrorFields {
	if !geminiRejectsAssistantPrefill(upstreamModel) {
		return nil
	}
	contents := gjson.GetBytes(nativeBody, "contents").Array()
	for i := len(contents) - 1; i >= 0; i-- {
		if !geminiTurnHasContent(contents[i]) {
			continue
		}
		if contents[i].Get("role").String() != "model" {
			return nil
		}
		fields := &openAIClientErrorFields{
			Type: "invalid_request_error", Code: "unsupported_conversation_turn", Param: "messages", StatusCode: http.StatusBadRequest,
			Message: "The final message does not produce a supported Gemini user or tool-result turn. Provide non-empty user content, inline images, or correctly paired tool results.",
		}
		if len(messages) > 0 {
			last := len(messages) - 1
			fields.Param = fmt.Sprintf("messages[%d]", last)
			if geminiChatAssistantText(messages[last]) {
				fields.Code = "unsupported_assistant_prefill"
				fields.Param += ".content"
				fields.Message = "This Gemini model does not support assistant prefill. End messages with a non-empty user message or the results of the preceding tool calls."
			}
		}
		return fields
	}
	return nil
}

func geminiTurnHasContent(content gjson.Result) bool {
	for _, part := range content.Get("parts").Array() {
		if strings.TrimSpace(part.Get("text").String()) != "" || part.Get("functionCall").Exists() ||
			part.Get("functionResponse").Exists() || part.Get("inlineData").Exists() || part.Get("fileData").Exists() {
			return true
		}
	}
	return false
}

func geminiChatAssistantText(message apicompat.ChatMessage) bool {
	if message.Role != "assistant" || len(message.ToolCalls) != 0 || message.FunctionCall != nil {
		return false
	}
	var text string
	if json.Unmarshal(message.Content, &text) == nil {
		return strings.TrimSpace(text) != ""
	}
	for _, part := range gjson.ParseBytes(message.Content).Array() {
		if part.Get("type").String() == "text" && strings.TrimSpace(part.Get("text").String()) != "" {
			return true
		}
	}
	return false
}
