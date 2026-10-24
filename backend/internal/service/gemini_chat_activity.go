package service

import (
	"net/http"
	"strconv"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/tidwall/gjson"
)

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
