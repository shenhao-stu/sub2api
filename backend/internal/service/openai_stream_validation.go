package service

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// xAI may return HTTP 200 followed by a bare error with code:null. Match its
// exact validation message; broad words such as "argument" also occur in
// transient errors and must not suppress account failover.
func isOpenAIStreamInvalidArguments(payload []byte, message string) bool {
	code := openAIStreamFailedEventErrorCode(payload)
	return code == "invalid_argument" || code == "invalid_arguments" ||
		strings.EqualFold(strings.TrimSpace(message), "Invalid arguments passed to the model.")
}

func normalizeOpenAIStreamValidation(payload []byte) []byte {
	if !isOpenAIStreamInvalidArguments(payload, extractOpenAISSEErrorMessage(payload)) {
		return payload
	}
	path := ""
	if gjson.GetBytes(payload, "response.error").Exists() {
		path = "response.error."
	} else if gjson.GetBytes(payload, "error").Exists() {
		path = "error."
	}
	updated, err := sjson.SetBytes(payload, path+"code", "invalid_argument")
	if err != nil {
		return payload
	}
	// The top-level type is the SSE event discriminator, not the error type.
	if path != "" {
		updated, _ = sjson.SetBytes(updated, path+"type", "invalid_request_error")
	}
	return updated
}

func writeOpenAIStreamValidation(c *gin.Context, payload []byte, message string, usage ...*OpenAIUsage) error {
	message = sanitizeUpstreamErrorMessage(message)
	errorBody := gin.H{"type": "invalid_request_error", "code": "invalid_argument", "message": message}
	for _, path := range []string{"response.error.param", "error.param", "param"} {
		if param := gjson.GetBytes(payload, path); param.Type == gjson.String && param.String() != "" {
			errorBody["param"] = param.String()
			break
		}
	}
	body := openAIMeteredErrorMetadata(c, openAIErrorBodyWithUsage(gin.H{"error": errorBody}, usage...))
	committed := StopOpenAICompactSSEKeepaliveCommitted(c)
	MarkResponseCommitted(c)
	if committed || c.Writer.Written() {
		source, _ := marshalOpenAIUpstreamJSON(gin.H{"error": errorBody})
		_, _ = c.Writer.WriteString(buildOpenAIResponseFailedSSE("", observedUpstreamResponseModel(c), source, message, usage...))
		c.Writer.Flush()
	} else {
		c.Header("Content-Type", "application/json; charset=utf-8")
		c.JSON(http.StatusBadRequest, body)
	}
	return fmt.Errorf("upstream invalid argument: %s", message)
}
