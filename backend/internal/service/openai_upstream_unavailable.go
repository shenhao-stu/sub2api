package service

import (
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
)

// WriteOpenAIUpstreamUnavailableError preserves an explicit upstream-unavailable
// response after failover is exhausted. It changes only the final response,
// without making the error a conversation guard or changing retry policy.
func WriteOpenAIUpstreamUnavailableError(c *gin.Context, status int, body []byte, streamStarted bool) bool {
	if c == nil || c.GetBool(clientErrorSemanticOutputKey) || status != http.StatusServiceUnavailable || len(body) > 64<<10 {
		return false
	}
	var doc, errorFields map[string]json.RawMessage
	var errorType string
	if json.Unmarshal(body, &doc) != nil || json.Unmarshal(doc["error"], &errorFields) != nil ||
		json.Unmarshal(errorFields["type"], &errorType) != nil || errorType != "upstream_unavailable" {
		return false
	}
	// Select the exact envelope before the shared decoder's case-insensitive
	// struct matching can pick an unrelated sibling such as "ERROR".
	canonicalBody := append([]byte(`{"error":`), doc["error"]...)
	canonicalBody = append(canonicalBody, '}')
	return writeOpenAIErrorFields(c, boundedOpenAIClientError(canonicalBody, status), streamStarted)
}
