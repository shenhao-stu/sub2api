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

// A session admission policy is not transient capacity. Match its complete
// contract; ordinary 429 responses retain their existing retry behavior.
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
	if raw, exists := fields["code"]; exists {
		var code string
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, &code) != nil || code != "" {
			return ""
		}
	}
	var kind, message string
	if json.Unmarshal(fields["type"], &kind) != nil || kind != "upstream_error" || json.Unmarshal(fields["message"], &message) != nil {
		return ""
	}
	if message == sessionAdmissionRejection || message == sessionPolicyBlockRejection {
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
	if message == sessionPolicyBlockRejection {
		fields = openAIClientErrorFields{Type: "permission_error", Code: "upstream_session_policy_blocked", Message: "The upstream blocked this conversation under its Usage Policy. In Claude Code, use /new to start a new conversation. Contact support if you believe this was a mistake.", StatusCode: http.StatusForbidden}
	}
	result, err := s.handleRequestRejectionBody(c, account, resp, body, parseClaudeUsageFromResponseBody(body), model, upstreamModel, stream, start, fields)
	return result, err, true
}
