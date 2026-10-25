package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// These terminal codes describe an attempt that reached generation. Missing
// usage is still unknown, but is not permission to dispatch a second attempt.
// Match structural identifiers, never message text or generic server errors.
func openAIGenerationFailureFields(payload []byte) (openAIClientErrorFields, bool) {
	var empty openAIClientErrorFields
	doc, ok := receiptObject(payload)
	if !ok {
		return empty, false
	}
	var kind string
	if json.Unmarshal(doc["type"], &kind) != nil || (kind != "response.failed" && kind != "error") {
		return empty, false
	}
	owner := doc
	if raw, exists := doc["response"]; exists {
		if _, conflicting := doc["error"]; conflicting {
			return empty, false
		}
		owner, ok = receiptObject(raw)
		if !ok {
			return empty, false
		}
		if raw, exists := owner["status"]; exists {
			var status string
			if json.Unmarshal(raw, &status) != nil || status != "failed" {
				return empty, false
			}
		}
	}
	fields, ok := receiptObject(owner["error"])
	var code string
	if !ok || json.Unmarshal(fields["code"], &code) != nil {
		return empty, false
	}
	message := ""
	switch code {
	case "upstream_malformed_response":
		message = "The upstream returned a malformed response."
	case "upstream_malformed_function_call":
		message = "The upstream returned a malformed function call."
	default:
		return empty, false
	}
	if raw, exists := fields["type"]; exists {
		var errorType string
		if json.Unmarshal(raw, &errorType) != nil || (errorType != "server_error" && errorType != "upstream_error" && errorType != "api_error") {
			return empty, false
		}
	}
	// Conflicting status keeps the existing auth, quota and transient policy.
	for _, path := range openAIStreamErrorStatusPaths {
		value := gjson.GetBytes(payload, path)
		if value.Exists() && (value.Type != gjson.Number || value.Raw != "502") {
			return empty, false
		}
	}
	return openAIClientErrorFields{Type: "upstream_error", Code: code, Message: message, StatusCode: http.StatusBadGateway}, true
}

func normalizeOpenAIGenerationFailure(payload []byte) []byte {
	fields, ok := openAIGenerationFailureFields(payload)
	if !ok {
		return payload
	}
	path := "error"
	if gjson.GetBytes(payload, "response").Exists() {
		path = "response.error"
	}
	updated, err := sjson.SetBytes(payload, path, fields)
	if err != nil {
		return payload
	}
	return updated
}

// Only public fixed fields cross this boundary; usage retains the existing
// observed-meter contract, including the distinction between absent and zero.
func writeOpenAIGenerationFailure(c *gin.Context, payload []byte, usage ...*OpenAIUsage) error {
	fields, ok := openAIGenerationFailureFields(payload)
	if !ok {
		return fmt.Errorf("unrecognized generation failure")
	}
	stream := StopOpenAICompactSSEKeepaliveCommitted(c) || (c.Writer.Written() && strings.HasPrefix(c.Writer.Header().Get("Content-Type"), "text/event-stream"))
	MarkResponseCommitted(c)
	if capture := chatReceiptFromContext(c.Request.Context()); capture != nil {
		capture.status = fields.StatusCode
	}
	body := openAIMeteredErrorMetadata(c, openAIErrorBodyWithUsage(gin.H{"error": fields}, usage...))
	if stream {
		MarkOpsStreamErrorValue(c, OpsStreamError{ErrType: fields.Type, Code: fields.Code, Message: fields.Message, IntendedStatus: fields.StatusCode, CountTowardsSLA: true})
		path := strings.TrimRight(c.Request.URL.Path, "/")
		if strings.HasSuffix(path, "/responses") || strings.Contains(path, "/responses/") {
			_, _ = c.Writer.WriteString(buildOpenAIResponseFailedSSE(gjson.GetBytes(payload, "response.id").String(), observedUpstreamResponseModel(c), gjson.GetBytes(payload, "response.created_at").Int(), payload, fields.Message, usage...))
		} else {
			encoded, _ := json.Marshal(body)
			_, _ = fmt.Fprintf(c.Writer, "data: %s\n\n", encoded)
		}
		c.Writer.Flush()
	} else {
		c.JSON(fields.StatusCode, body)
	}
	return fmt.Errorf("upstream generation failed: %s", fields.Code)
}
