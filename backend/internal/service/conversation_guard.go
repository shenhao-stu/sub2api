package service

import (
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
)

const conversationGuardReason GatewayFailureReason = "conversation_guard"

// A pinned conversation's admission guard is terminal for this request. It
// does not describe the health of the aggregator account or its other users.
func isConversationGuardError(status int, body []byte) bool {
	if len(body) > 64<<10 || (status != 403 && status != 429 && status != 503) {
		return false
	}
	var doc struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &doc) != nil {
		return false
	}
	switch doc.Error.Code {
	case "conversation_account_unavailable", "conversation_account_restricted", "conversation_model_disabled":
		return status == http.StatusForbidden
	case "conversation_quota_exhausted", "conversation_rate_limited":
		return status == http.StatusTooManyRequests
	case "conversation_route_unavailable", "conversation_account_paused":
		return status == http.StatusServiceUnavailable
	default:
		return false
	}
}

// WriteConversationGuardError preserves the pre-admission contract without
// requiring a broad 403/429/503 passthrough rule. Public fields remain bounded
// and redacted, and a committed heartbeat retains its logical error status.
func WriteConversationGuardError(c *gin.Context, status int, body []byte, streamStarted bool) bool {
	if c == nil || c.GetBool(clientErrorSemanticOutputKey) || !isConversationGuardError(status, body) {
		return false
	}
	return writeOpenAIErrorFields(c, boundedOpenAIClientError(body, status), streamStarted)
}
