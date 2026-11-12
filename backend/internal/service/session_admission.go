package service

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

const sessionAdmissionBodyLimit = 64 << 10
const sessionAdmissionRejection = "This group only accepts sessions first seen here."
const sessionPolicyBlockRejection = "当前会话因内容审核违规已被拉黑，请使用 /new 开启新会话，或联系管理员解除。"
const nativeSessionBusy = "session_busy"
const nativeIngressBusy = "Relay capacity is busy; retry later."

// Match request admission errors separately from provider-wide 429 responses.
func sessionAdmissionRejectionMessage(status int, body []byte) string {
	if status != http.StatusTooManyRequests || len(body) >= sessionAdmissionBodyLimit {
		return ""
	}
	envelope, ok := receiptObject(body)
	if !ok {
		return ""
	}
	if raw, exists := envelope["type"]; exists {
		var kind string
		if json.Unmarshal(raw, &kind) != nil || kind != "error" {
			return ""
		}
	}
	fields, ok := receiptObject(envelope["error"])
	if !ok {
		return ""
	}
	for _, object := range []map[string]json.RawMessage{envelope, fields} {
		for _, name := range []string{"status", "status_code"} {
			if raw, exists := object[name]; exists {
				var declared int
				if json.Unmarshal(raw, &declared) != nil || declared != status {
					return ""
				}
			}
		}
	}
	var code string
	if raw, exists := fields["code"]; exists {
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, &code) != nil {
			return ""
		}
	}
	var kind, message string
	if json.Unmarshal(fields["type"], &kind) != nil || json.Unmarshal(fields["message"], &message) != nil {
		return ""
	}
	switch {
	case kind == "upstream_error" && code == "" && (message == sessionAdmissionRejection || message == sessionPolicyBlockRejection):
		return message
	case kind == "rate_limit_error" && code == "upstream_rate_limit" && message == nativeSessionBusy:
		return message
	case kind == "overloaded_error" && code == "native_ingress_busy" && message == nativeIngressBusy && envelope["type"] != nil:
		return message
	}
	return ""
}

func (s *GatewayService) handleSessionAdmissionResponse(c *gin.Context, account *Account, resp *http.Response, model, upstreamModel string, stream bool, start time.Time) (*ForwardResult, error, bool) {
	if account == nil || account.Type != AccountTypeAPIKey || resp == nil || resp.Body == nil || resp.StatusCode != http.StatusTooManyRequests {
		return nil, nil, false
	}
	source := resp.Body
	body, readErr := awaitAnthropicBody(c, source, func() ([]byte, error) { return io.ReadAll(io.LimitReader(source, sessionAdmissionBodyLimit)) })
	resp.Body = &anthropicErrorPrefixBody{prefix: bytes.NewReader(body), source: source, readErr: readErr}
	message := sessionAdmissionRejectionMessage(resp.StatusCode, body)
	if readErr != nil || message == "" {
		return nil, nil, false
	}
	defer resp.Body.Close()
	fields := openAIClientErrorFields{Type: "invalid_request_error", Code: "upstream_session_not_accepted", Message: "This session is not accepted by the upstream group. Please start a new session.", StatusCode: http.StatusBadRequest}
	switch message {
	case sessionPolicyBlockRejection:
		fields = openAIClientErrorFields{Type: "permission_error", Code: "upstream_session_policy_blocked", Message: "The upstream blocked this conversation under its Usage Policy. In Claude Code, use /new to start a new conversation. Contact support if you believe this was a mistake.", StatusCode: http.StatusForbidden}
	case nativeSessionBusy:
		fields = openAIClientErrorFields{Type: "rate_limit_error", Code: "conversation_rate_limited", Message: "This conversation already has a request in progress. Wait for it to finish before retrying.", StatusCode: http.StatusTooManyRequests}
	case nativeIngressBusy:
		fields = openAIClientErrorFields{Type: "overloaded_error", Code: "native_ingress_busy", Message: "The model relay is temporarily at capacity. Please retry shortly.", StatusCode: http.StatusTooManyRequests}
	}
	if fields.StatusCode == http.StatusTooManyRequests && !c.Writer.Written() && resp.Header.Get("Retry-After") == "1" {
		c.Header("Retry-After", "1")
	}
	result, err := s.handleRequestRejectionBody(c, account, resp, body, parseClaudeUsageFromResponseBody(body), model, upstreamModel, stream, start, fields)
	return result, err, true
}
