package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// An explicit provider policy refusal is request-scoped. Switching accounts
// cannot repair it and must not circumvent a conversation or device block.
func isContentRefusal(status int, body []byte) bool {
	if status != http.StatusForbidden || len(body) > 64<<10 {
		return false
	}
	var doc struct {
		Error struct {
			Type string `json:"type"`
			Code string `json:"code"`
		} `json:"error"`
	}
	return json.Unmarshal(body, &doc) == nil && (doc.Error.Type == "content_refusal" || doc.Error.Code == "content_policy_violation")
}

func (s *GatewayService) handleContentRefusalResponse(c *gin.Context, account *Account, resp *http.Response, model, upstreamModel string, stream bool, start time.Time) (*ForwardResult, error, bool) {
	if resp == nil || resp.Body == nil || resp.StatusCode != http.StatusForbidden {
		return nil, nil, false
	}
	body, readErr := s.readUpstreamErrorBody(resp, c)
	_ = resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(body))
	if readErr != nil || !isContentRefusal(resp.StatusCode, body) {
		return nil, nil, false
	}
	fields := boundedOpenAIClientError(body, resp.StatusCode)
	setOpsUpstreamError(c, resp.StatusCode, fields.Message, "")
	appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
		AccountID: account.ID, Platform: account.Platform,
		UpstreamStatusCode: resp.StatusCode, UpstreamRequestID: resp.Header.Get("x-request-id"),
		Kind: "request_rejection", Message: fields.Message,
	})
	MarkResponseCommitted(c)
	payload := gin.H{"type": "error", "error": fields}
	usage := parseClaudeUsageFromResponseBody(body)
	var result *ForwardResult
	if usage.hasObservedTokens() {
		// A refusal can follow real metering. Preserve that usage for the existing
		// partial-result settlement path and for the downstream relay.
		payload["usage"] = gin.H{
			"input_tokens": usage.InputTokens, "output_tokens": usage.OutputTokens,
			"cache_creation_input_tokens": usage.CacheCreationInputTokens, "cache_read_input_tokens": usage.CacheReadInputTokens,
			"cache_creation": gin.H{"ephemeral_5m_input_tokens": usage.CacheCreation5mTokens, "ephemeral_1h_input_tokens": usage.CacheCreation1hTokens},
		}
		result = &ForwardResult{RequestID: resp.Header.Get("x-request-id"), UpstreamHeaders: resp.Header,
			Usage: *usage, Model: model, UpstreamModel: upstreamModel, Stream: stream, Duration: time.Since(start)}
	}
	WriteAnthropicErrorResponse(c, resp.StatusCode, payload)
	return result, fmt.Errorf("upstream content refusal: %s", fields.Message), true
}
