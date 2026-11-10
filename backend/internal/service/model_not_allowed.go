package service

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const modelNotAllowedBodyLimit = 64 << 10
const modelNotAllowedMessage = "This model is prohibited from being used."

// A provider may label a model permission denial as 429. Only the explicit,
// complete error contract is terminal; ordinary capacity limits retain retry.
func isModelNotAllowedError(status int, body []byte) bool {
	if status != http.StatusTooManyRequests || len(body) >= modelNotAllowedBodyLimit {
		return false
	}
	envelope, ok := receiptObject(body)
	if !ok {
		return false
	}
	fields, ok := receiptObject(envelope["error"])
	if !ok {
		return false
	}
	for _, object := range []map[string]json.RawMessage{envelope, fields} {
		if raw, exists := object["status_code"]; exists {
			var declared int
			if json.Unmarshal(raw, &declared) != nil || declared != status {
				return false
			}
		}
	}
	var code, message string
	if raw, exists := fields["code"]; exists {
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, &code) != nil {
			return false
		}
		if code != "" {
			return code == "model_not_allowed"
		}
	}
	return json.Unmarshal(fields["message"], &message) == nil && strings.EqualFold(strings.TrimSpace(message), modelNotAllowedMessage)
}

func modelNotAllowedFields() openAIClientErrorFields {
	return openAIClientErrorFields{Type: "permission_error", Code: "model_not_allowed", Message: modelNotAllowedMessage, StatusCode: http.StatusForbidden}
}

func (s *GatewayService) handleModelNotAllowedResponse(c *gin.Context, account *Account, resp *http.Response, model, upstreamModel string, stream bool, start time.Time) (*ForwardResult, error, bool) {
	if resp == nil || resp.Body == nil || resp.StatusCode != http.StatusTooManyRequests {
		return nil, nil, false
	}
	source := resp.Body
	body, readErr := awaitAnthropicBody(c, source, func() ([]byte, error) {
		return io.ReadAll(io.LimitReader(source, modelNotAllowedBodyLimit))
	})
	// A nonmatching response must keep its bytes, read failure and original Close.
	resp.Body = &anthropicErrorPrefixBody{prefix: bytes.NewReader(body), source: source, readErr: readErr}
	if readErr != nil || !isModelNotAllowedError(resp.StatusCode, body) {
		return nil, nil, false
	}
	defer resp.Body.Close()
	result, err := s.handleRequestRejectionBody(c, account, resp, body, parseClaudeUsageFromResponseBody(body), model, upstreamModel, stream, start, modelNotAllowedFields())
	return result, err, true
}
