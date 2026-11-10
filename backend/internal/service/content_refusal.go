package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
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
	return json.Unmarshal(body, &doc) == nil && (doc.Error.Type == "content_refusal" || doc.Error.Code == "content_policy_violation" || doc.Error.Code == "policy_blocked")
}

func (s *GatewayService) handleContentRefusalResponse(c *gin.Context, account *Account, resp *http.Response, model, upstreamModel string, stream bool, start time.Time) (*ForwardResult, error, bool) {
	guard := s.trustedAnthropicRefusal(c, account)
	if resp == nil || resp.Body == nil || (resp.StatusCode != http.StatusForbidden &&
		(resp.StatusCode < http.StatusBadRequest || guard == nil)) {
		return nil, nil, false
	}
	var body []byte
	var readErr error
	if guard == nil {
		body, readErr = s.readUpstreamErrorBody(resp, c)
		_ = resp.Body.Close()
		resp.Body = io.NopCloser(bytes.NewReader(body))
	} else {
		original := resp.Body
		body, readErr = awaitAnthropicBody(c, original, func() ([]byte, error) {
			return io.ReadAll(io.LimitReader(original, (64<<10)+1))
		})
		resp.Body = &anthropicErrorPrefixBody{prefix: bytes.NewReader(body), source: original, readErr: readErr}
	}
	if readErr != nil {
		return nil, nil, false
	}
	if guard.observe(body) {
		defer resp.Body.Close()
		fields := openAIClientErrorFields{Type: "policy_blocked", Code: "refusal_guard", Message: "Anthropic refused this request under its usage policy. Do not retry this request.", StatusCode: http.StatusBadRequest}
		result, err := s.handleRequestRejectionBody(c, account, resp, body, parseClaudeUsageFromResponseBody(body), model, upstreamModel, stream, start, fields)
		return result, err, true
	}
	if !isContentRefusal(resp.StatusCode, body) {
		return nil, nil, false
	}
	defer resp.Body.Close()
	result, err := s.handleContentRefusalBody(c, account, resp, body, parseClaudeUsageFromResponseBody(body), model, upstreamModel, stream, start)
	return result, err, true
}

func (s *GatewayService) handleContentRefusalBody(c *gin.Context, account *Account, resp *http.Response, body []byte, usage *ClaudeUsage, model, upstreamModel string, stream bool, start time.Time) (*ForwardResult, error) {
	return s.handleRequestRejectionBody(c, account, resp, body, usage, model, upstreamModel, stream, start, boundedOpenAIClientError(body, http.StatusForbidden))
}

func (s *GatewayService) handleRequestRejectionBody(c *gin.Context, account *Account, resp *http.Response, body []byte, usage *ClaudeUsage, model, upstreamModel string, stream bool, start time.Time, fields openAIClientErrorFields) (*ForwardResult, error) {
	restorePolicyCacheCreation(usage)
	status := fields.StatusCode
	// Preserve restricted diagnostics under the existing administrator setting.
	// Client redaction must not erase the evidence used to correlate incidents.
	detail := ""
	if s.cfg != nil && s.cfg.Gateway.LogUpstreamErrorBody {
		limit := s.cfg.Gateway.LogUpstreamErrorBodyMaxBytes
		if limit <= 0 {
			limit = 2048
		}
		detail = truncateString(string(body), limit)
	}
	upstreamRequestID := resp.Header.Get("x-request-id")
	if upstreamRequestID == "" {
		var envelope struct {
			Error struct {
				RequestID string `json:"request_id"`
			} `json:"error"`
		}
		if json.Unmarshal(body, &envelope) == nil && clientErrorIdentifier.MatchString(envelope.Error.RequestID) {
			upstreamRequestID = envelope.Error.RequestID
		}
	}
	setOpsUpstreamError(c, resp.StatusCode, fields.Message, detail)
	appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
		AccountID: account.ID, Platform: account.Platform,
		UpstreamStatusCode: resp.StatusCode, UpstreamRequestID: upstreamRequestID,
		Kind: "request_rejection", Message: fields.Message, Detail: detail,
	})
	MarkResponseCommitted(c)
	payload := gin.H{"type": "error", "error": fields}
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
	path := ""
	if c.Request != nil && c.Request.URL != nil {
		path = strings.TrimRight(c.Request.URL.Path, "/")
	}
	if strings.HasSuffix(path, "/chat/completions") || strings.HasSuffix(path, "/responses") {
		var meter *OpenAIUsage
		if usage.hasObservedTokens() {
			converted := policyClaudeUsageToOpenAIUsage(usage)
			meter = &converted
		}
		writeOpenAIErrorFields(c, fields, false, meter)
	} else {
		WriteAnthropicErrorResponse(c, status, payload)
	}
	return result, fmt.Errorf("upstream request rejection: %s", fields.Message)
}
