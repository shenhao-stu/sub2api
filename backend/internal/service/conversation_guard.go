package service

import (
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
)

const conversationGuardReason GatewayFailureReason = "conversation_guard"

// A pinned conversation or account route guard is terminal for this request.
// It does not describe the health of the aggregator account or its other users.
func isConversationGuardError(status int, body []byte) bool {
	if len(body) > 64<<10 || (status != 403 && status != 429 && status != 503) {
		return false
	}
	var doc struct {
		Error struct {
			Code json.RawMessage `json:"code"`
			Type json.RawMessage `json:"type"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &doc) != nil {
		return false
	}
	// A present code must be a string. In particular, null is not an omitted
	// identifier and must not allow type to override an explicit code field.
	code := ""
	if len(doc.Error.Code) != 0 {
		var value *string
		if json.Unmarshal(doc.Error.Code, &value) != nil || value == nil {
			return false
		}
		code = *value
	}
	// C2A's proxy guard uses the OpenAI error.type field without error.code.
	// Only this exact producer contract is a fallback; explicit codes keep priority.
	if code == "" && status == http.StatusServiceUnavailable {
		var errorType string
		return json.Unmarshal(doc.Error.Type, &errorType) == nil && errorType == "proxy_unavailable"
	}
	switch code {
	case "conversation_account_unavailable", "conversation_account_restricted", "conversation_model_disabled":
		return status == http.StatusForbidden
	case "conversation_quota_exhausted", "conversation_rate_limited":
		return status == http.StatusTooManyRequests
	case "conversation_route_unavailable", "conversation_account_paused", "proxy_unavailable":
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
	fields := boundedOpenAIClientError(body, status)
	if fields.Code == "proxy_unavailable" || (fields.Code == "" && fields.Type == "proxy_unavailable") {
		// This pre-admission guard has one public cause. Do not expose transport
		// diagnostics or origin identifiers from either producer envelope shape.
		fields = openAIClientErrorFields{
			Type: "proxy_unavailable", Code: "proxy_unavailable", StatusCode: status,
			Message: "账号出口暂不可达，已暂停调度并自动复检；恢复后可继续原会话。",
		}
	}
	return writeOpenAIErrorFields(c, fields, streamStarted)
}
