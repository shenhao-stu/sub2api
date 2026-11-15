package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

const anthropicIncompleteMessageCode = "upstream_malformed_response"

func anthropicIncompleteMessageUsage(body []byte) *ClaudeUsage {
	message, ok := receiptObject(body)
	if !ok || string(message["type"]) != `"message"` || string(message["role"]) != `"assistant"` ||
		message["error"] != nil || string(bytes.TrimSpace(message["stop_reason"])) != "null" {
		return nil
	}

	var id, model string
	if json.Unmarshal(message["id"], &id) != nil || !strings.HasPrefix(id, "msg_") || len(id) <= len("msg_") ||
		json.Unmarshal(message["model"], &model) != nil || !strings.HasPrefix(model, "claude-") {
		return nil
	}

	var content []json.RawMessage
	if json.Unmarshal(message["content"], &content) != nil || content == nil || len(content) != 0 {
		return nil
	}
	if _, ok := receiptObject(message["usage"]); !ok {
		return nil
	}
	var usage ClaudeUsage
	if json.Unmarshal(message["usage"], &usage) != nil || !usage.hasObservedTokens() {
		return nil
	}
	return &usage
}

// A pool can return HTTP 502 with only the provider's message_start object.
// Inference began, but the usage in that object is provisional. Stop failover
// without returning a ForwardResult that could be settled as final usage.
func (s *GatewayService) handleAnthropicIncompleteMessage(c *gin.Context, account *Account, resp *http.Response) bool {
	if !account.IsPoolMode() || resp.StatusCode != http.StatusBadGateway || resp.Body == nil {
		return false
	}

	source := resp.Body
	const limit = 8 << 10
	body, err := awaitAnthropicBody(c, source, func() ([]byte, error) {
		return io.ReadAll(io.LimitReader(source, limit))
	})
	resp.Body = &anthropicErrorPrefixBody{prefix: bytes.NewReader(body), source: source, readErr: err}
	if err != nil || len(body) >= limit {
		return false
	}
	usage := anthropicIncompleteMessageUsage(body)
	if usage == nil {
		return false
	}

	_ = resp.Body.Close()
	const message = "The upstream started this response but did not complete it."
	detail := fmt.Sprintf("final_usage=false input=%d output=%d cache_read=%d cache_creation=%d",
		usage.InputTokens, usage.OutputTokens, usage.CacheReadInputTokens, usage.CacheCreationInputTokens)
	setOpsUpstreamError(c, resp.StatusCode, anthropicIncompleteMessageCode, detail)
	appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
		AccountID: account.ID, Platform: account.Platform, UpstreamStatusCode: resp.StatusCode,
		UpstreamRequestID: UpstreamRequestIDFromHeaders(account, resp.Header),
		Kind:              "incomplete_message_start", Message: anthropicIncompleteMessageCode, Detail: detail, Passthrough: true,
	})
	WriteAnthropicErrorResponse(c, resp.StatusCode, gin.H{
		"type": "error", "error": gin.H{"type": "upstream_error", "code": anthropicIncompleteMessageCode,
			"message": message, "status_code": resp.StatusCode},
	})
	MarkResponseCommitted(c)
	return true
}
