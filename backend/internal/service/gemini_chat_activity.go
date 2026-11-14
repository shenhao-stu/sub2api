package service

import (
	"bytes"
	"net/http"
	"strconv"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/tidwall/gjson"
)

func geminiChatNativeUnavailable(payload []byte) bool {
	event, ok := openAIGenerationFailureObject(payload)
	if !ok {
		return false
	}
	owner := event
	switch string(event["type"]) {
	case `"error"`:
		if event["response"] != nil {
			return false
		}
	case `"response.failed"`:
		if event["error"] != nil {
			return false
		}
		owner, ok = openAIGenerationFailureObject(event["response"])
		if !ok || !bytes.Equal(owner["status"], []byte(`"failed"`)) {
			return false
		}
	default:
		return false
	}
	detail, ok := openAIGenerationFailureObject(owner["error"])
	if !ok || !bytes.Equal(detail["code"], []byte(`503`)) || !bytes.Equal(detail["status"], []byte(`"UNAVAILABLE"`)) {
		return false
	}
	if kind := detail["type"]; kind != nil && !bytes.Equal(kind, []byte(`"server_error"`)) && !bytes.Equal(kind, []byte(`"upstream_error"`)) {
		return false
	}
	for _, path := range []string{"status", "status_code", "error.status_code", "response.status_code", "response.error.status_code"} {
		if raw := gjson.GetBytes(payload, path); raw.Exists() && (raw.Type != gjson.Number || raw.Raw != "503") {
			return false
		}
	}
	return true
}

// A created lifecycle alone is not progress. Later known events may release
// transport headers without releasing the buffered semantic prefix.
func geminiResponsesActivity(event *apicompat.ResponsesStreamEvent, started bool) (bool, bool) {
	if event.Response != nil && (event.Response.Error != nil || event.Response.Status == "failed" || event.Response.Status == "completed" || event.Response.Status == "incomplete") {
		return started, false
	}
	switch event.Type {
	case "response.created", "response.in_progress":
		if event.Response == nil || event.Response.ID == "" || (event.Response.Status != "" && event.Response.Status != "queued" && event.Response.Status != "in_progress") {
			return started, false
		}
		return true, started && event.Type == "response.in_progress"
	case "response.output_item.added", "response.output_item.done":
		return started, started && event.Item != nil && event.Item.ID != "" && (event.Item.Type == "reasoning" || event.Item.Type == "message")
	case "response.content_part.added", "response.content_part.done":
		return started, started && event.ItemID != "" && event.Part != nil && event.Part.Type == "output_text"
	case "response.reasoning_summary_part.added", "response.reasoning_summary_part.done":
		return started, started && event.ItemID != "" && event.Part != nil && event.Part.Type == "summary_text"
	default:
		return started, false
	}
}

// Once activity committed HTTP 200, an explicit upstream failure status must
// travel in the SSE error object. Configured passthrough rules still take priority.
func geminiActivityFailureStatus(payload []byte, message string) int {
	for _, path := range openAIStreamErrorStatusPaths {
		value := gjson.GetBytes(payload, path)
		if value.Type != gjson.Number {
			continue
		}
		status, err := strconv.Atoi(value.Raw)
		if err == nil && status >= http.StatusBadRequest && status <= 599 {
			return status
		}
	}
	return openAIStreamFailedEventSemanticStatus(payload, message)
}
