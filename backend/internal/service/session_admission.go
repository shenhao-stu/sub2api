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

// A session admission policy is not transient capacity. Match its complete
// contract; ordinary 429 responses retain their existing retry behavior.
func isSessionAdmissionRejection(status int, body []byte) bool {
	if status != http.StatusTooManyRequests || len(body) >= sessionAdmissionBodyLimit {
		return false
	}
	envelope, ok := receiptObject(body)
	if !ok {
		return false
	}
	if raw, exists := envelope["type"]; exists {
		var kind string
		if json.Unmarshal(raw, &kind) != nil || kind != "error" {
			return false
		}
	}
	fields, ok := receiptObject(envelope["error"])
	if !ok {
		return false
	}
	for _, object := range []map[string]json.RawMessage{envelope, fields} {
		for _, name := range []string{"status", "status_code"} {
			if raw, exists := object[name]; exists {
				var declared int
				if json.Unmarshal(raw, &declared) != nil || declared != status {
					return false
				}
			}
		}
	}
	if raw, exists := fields["code"]; exists {
		var code string
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, &code) != nil || code != "" {
			return false
		}
	}
	var kind, message string
	return json.Unmarshal(fields["type"], &kind) == nil && kind == "upstream_error" &&
		json.Unmarshal(fields["message"], &message) == nil && message == sessionAdmissionRejection
}

func (s *GatewayService) handleSessionAdmissionResponse(c *gin.Context, account *Account, resp *http.Response, model, upstreamModel string, stream bool, start time.Time) (*ForwardResult, error, bool) {
	if account == nil || account.Type != AccountTypeAPIKey || resp == nil || resp.Body == nil || resp.StatusCode != http.StatusTooManyRequests {
		return nil, nil, false
	}
	source := resp.Body
	body, readErr := awaitAnthropicBody(c, source, func() ([]byte, error) { return io.ReadAll(io.LimitReader(source, sessionAdmissionBodyLimit)) })
	resp.Body = &anthropicErrorPrefixBody{prefix: bytes.NewReader(body), source: source, readErr: readErr}
	if readErr != nil || !isSessionAdmissionRejection(resp.StatusCode, body) {
		return nil, nil, false
	}
	defer resp.Body.Close()
	fields := openAIClientErrorFields{Type: "invalid_request_error", Code: "upstream_session_not_accepted", Message: "This session is not accepted by the upstream group. Please start a new session.", StatusCode: http.StatusBadRequest}
	result, err := s.handleRequestRejectionBody(c, account, resp, body, parseClaudeUsageFromResponseBody(body), model, upstreamModel, stream, start, fields)
	return result, err, true
}
