package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Wei-Shaw/sub2api/internal/util/logredact"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

var (
	clientErrorIdentifier = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.-]{0,95}$`)
	clientErrorParam      = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(?:\[[0-9]{1,9}\]|\.[A-Za-z_][A-Za-z0-9_]*)*$`)
	clientErrorCredential = regexp.MustCompile(`(?i)\b(?:Bearer\s+[A-Za-z0-9._~+/=-]+|sk-[A-Za-z0-9_-]+)`)
)

type openAIClientErrorFields struct {
	Type       string `json:"type"`
	Code       string `json:"code,omitempty"`
	Param      string `json:"param,omitempty"`
	Message    string `json:"message"`
	StatusCode int    `json:"status_code"`
}

const clientErrorSemanticOutputKey = "openai_client_error_semantic_output"

// MarkOpenAIClientErrorSemanticOutput prevents a late error from starting a
// second Responses lifecycle after the forwarder has already written output.
func MarkOpenAIClientErrorSemanticOutput(c *gin.Context) {
	if c != nil {
		c.Set(clientErrorSemanticOutputKey, true)
	}
}

func boundedOpenAIClientError(body []byte, status int) openAIClientErrorFields {
	result := openAIClientErrorFields{
		Type: openAIUpstreamClientErrorFallbackType, Message: openAIUpstreamClientErrorFallbackMessage, StatusCode: status,
	}
	var doc struct {
		Error map[string]json.RawMessage `json:"error"`
	}
	if len(body) > 64<<10 || json.Unmarshal(body, &doc) != nil {
		return result
	}
	field := func(key string) string {
		var value string
		_ = json.Unmarshal(doc.Error[key], &value)
		return strings.TrimSpace(value)
	}
	if value := field("type"); clientErrorIdentifier.MatchString(value) && !clientErrorCredential.MatchString(value) && redactClientErrorOrigins(value) == value {
		result.Type = value
	}
	if value := field("code"); clientErrorIdentifier.MatchString(value) && !clientErrorCredential.MatchString(value) && redactClientErrorOrigins(value) == value {
		result.Code = value
	}
	if value := field("param"); len(value) <= 512 && clientErrorParam.MatchString(value) && redactClientErrorOrigins(value) == value {
		result.Param = value
	}
	message := field("message")
	// Error messages must not become a second JSON envelope or an HTML document.
	if message == "" || strings.HasPrefix(message, "{") || strings.HasPrefix(message, "[") || strings.HasPrefix(message, "<") {
		return result
	}
	message = redactClientErrorOrigins(message)
	message = clientErrorCredential.ReplaceAllString(message, "[REDACTED]")
	message = logredact.RedactText(sanitizeUpstreamErrorMessage(message), "api_key", "apikey", "authorization", "cookie", "set-cookie")
	message = strings.Map(func(r rune) rune {
		if r < 32 || r == 127 {
			return ' '
		}
		return r
	}, message)
	if len(message) > 2048 {
		message = message[:2048]
		for !utf8.ValidString(message) {
			message = message[:len(message)-1]
		}
	}
	result.Message = message
	return result
}

// WriteOpenAIClientErrorPassthrough preserves only a rule-enabled OpenAI 400/422
// error object. Other rules retain their existing message/status precedence.
func WriteOpenAIClientErrorPassthrough(c *gin.Context, rules *ErrorPassthroughService, upstreamStatus int, body []byte, streamStarted bool) bool {
	if c == nil || rules == nil || c.GetBool(clientErrorSemanticOutputKey) ||
		(upstreamStatus != http.StatusBadRequest && upstreamStatus != http.StatusUnprocessableEntity) {
		return false
	}
	rule := rules.MatchRule(PlatformOpenAI, upstreamStatus, body)
	if rule == nil || !rule.PassthroughBody {
		return false
	}
	status := upstreamStatus
	if !rule.PassthroughCode && rule.ResponseCode != nil {
		status = *rule.ResponseCode
	}
	if rule.SkipMonitoring {
		c.Set(OpsSkipPassthroughKey, true)
	}
	fields := boundedOpenAIClientError(body, status)
	return writeOpenAIErrorFields(c, fields, streamStarted)
}

func writeOpenAIErrorFields(c *gin.Context, fields openAIClientErrorFields, streamStarted bool) bool {
	status := fields.StatusCode
	streamStarted = StopOpenAICompactSSEKeepaliveCommitted(c) || streamStarted ||
		(c.Writer.Written() && strings.HasPrefix(c.Writer.Header().Get("Content-Type"), "text/event-stream"))
	MarkResponseCommitted(c)
	path := ""
	if c.Request != nil && c.Request.URL != nil {
		path = strings.TrimRight(c.Request.URL.Path, "/")
	}
	payload := gin.H{"error": fields}
	if strings.HasSuffix(path, "/messages") {
		payload["type"] = "error"
	}
	if streamStarted || c.Writer.Written() {
		MarkOpsStreamErrorValue(c, OpsStreamError{
			ErrType: fields.Type, Code: fields.Code, Message: fields.Message, IntendedStatus: status,
			NonStream: !streamStarted,
		})
	}
	if streamStarted {
		event := ""
		switch {
		case strings.HasSuffix(path, "/messages"):
			event = "event: error\n"
		case strings.HasSuffix(path, "/responses") || strings.Contains(path, "/responses/"):
			event = "event: response.failed\n"
			response := gin.H{
				"id": "resp_" + strings.ReplaceAll(uuid.NewString(), "-", ""), "object": "response",
				"created_at": time.Now().Unix(), "status": "in_progress", "output": []any{}, "error": nil, "usage": nil,
			}
			// SDK accumulators require a lifecycle before a first terminal, even
			// when only heartbeat comments preceded this HTTP rejection.
			created, _ := json.Marshal(gin.H{"type": "response.created", "sequence_number": 0, "response": response})
			if _, err := fmt.Fprintf(c.Writer, "event: response.created\ndata: %s\n\n", created); err != nil {
				_ = c.Error(err)
				return true
			}
			response["status"], response["error"] = "failed", fields
			delete(response, "usage")
			payload = gin.H{"type": "response.failed", "sequence_number": 1, "response": response}
		}
		encoded, _ := json.Marshal(payload)
		if _, err := fmt.Fprintf(c.Writer, "%sdata: %s\n\n", event, encoded); err != nil {
			_ = c.Error(err)
		}
		c.Writer.Flush()
		return true
	}
	if c.Writer.Written() {
		// The HTTP status is already committed; retain it on the wire and put
		// the intended status in the error object without writing new headers.
		encoded, _ := json.Marshal(payload)
		_, _ = c.Writer.Write(encoded)
	} else {
		c.JSON(status, payload)
	}
	return true
}
