package service

import (
	"bytes"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// xAI accepts speaker labels only for the user role. A legacy function result's
// name identifies the invocation, so never discard it as a speaker label.
func normalizeGrokChatMessageNames(body []byte) ([]byte, error) {
	messages := gjson.GetBytes(body, "messages")
	if !messages.IsArray() {
		return body, nil
	}
	var rebuilt bytes.Buffer
	cursor := 0
	var editErr error
	messages.ForEach(func(_, message gjson.Result) bool {
		switch message.Get("role").Str {
		case "assistant", "system", "developer", "tool":
		default:
			return true
		}
		if !message.Get("name").Exists() {
			return true
		}
		modified, err := sjson.Delete(message.Raw, "name")
		if err != nil {
			editErr = err
			return false
		}
		rebuilt.Write(body[cursor:message.Index])
		rebuilt.WriteString(modified)
		cursor = message.Index + len(message.Raw)
		return true
	})
	if editErr != nil || cursor == 0 {
		return body, editErr
	}
	rebuilt.Write(body[cursor:])
	return rebuilt.Bytes(), nil
}
