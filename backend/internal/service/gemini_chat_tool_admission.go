package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// ValidateGeminiChatToolArguments rejects ambiguous tool history before a usage
// receipt is claimed. Google's Struct decoder rejects repeated object keys;
// choosing a value here could change what the client's tool actually executed.
func (s *OpenAIGatewayService) ValidateGeminiChatToolArguments(c *gin.Context, account *Account, body []byte) error {
	if account == nil {
		return nil
	}
	model := resolveOpenAIForwardModel(account, gjson.GetBytes(body, "model").String(), "")
	model = model[strings.LastIndex(model, "/")+1:]
	if !strings.HasPrefix(strings.ToLower(model), "gemini-") {
		return nil
	}
	reject := func(path string) error {
		message := "Gemini requires unique JSON object keys in tool arguments. Correct the tool-call history or start a new conversation."
		writeOpenAIErrorFields(c, openAIClientErrorFields{
			Type: "invalid_request_error", Code: "duplicate_tool_argument", Param: path,
			Message: message, StatusCode: http.StatusBadRequest,
		}, false)
		return errors.New(message)
	}
	for i, message := range gjson.GetBytes(body, "messages").Array() {
		if message.Get("role").String() != "assistant" {
			continue
		}
		for j, call := range message.Get("tool_calls").Array() {
			args := call.Get("function.arguments")
			if args.Type == gjson.String && duplicateToolArgumentKeys(args.String()) {
				return reject(fmt.Sprintf("messages[%d].tool_calls[%d].function.arguments", i, j))
			}
		}
	}
	// The Chat endpoint also accepts the existing Responses-shaped compatibility
	// input. Validate its tool history without rewriting either representation.
	for i, item := range gjson.GetBytes(body, "input").Array() {
		args := item.Get("arguments")
		if item.Get("type").String() == "function_call" && args.Type == gjson.String && duplicateToolArgumentKeys(args.String()) {
			return reject(fmt.Sprintf("input[%d].arguments", i))
		}
	}
	return nil
}

func duplicateToolArgumentKeys(raw string) bool {
	if !json.Valid([]byte(raw)) {
		return false // Leave unrelated syntax errors to the existing validator.
	}
	d := json.NewDecoder(strings.NewReader(raw))
	d.UseNumber()
	var scan func() bool
	scan = func() bool {
		token, err := d.Token()
		if err != nil {
			return false
		}
		switch token {
		case json.Delim('{'):
			keys := make(map[string]struct{})
			for d.More() {
				key, _ := d.Token() // json.Valid established the object grammar.
				name := key.(string)
				if _, exists := keys[name]; exists {
					return true
				}
				keys[name] = struct{}{}
				if scan() {
					return true
				}
			}
			_, _ = d.Token()
		case json.Delim('['):
			for d.More() {
				if scan() {
					return true
				}
			}
			_, _ = d.Token()
		}
		return false
	}
	return scan()
}
