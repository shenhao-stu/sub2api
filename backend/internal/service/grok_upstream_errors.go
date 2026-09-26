package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/util/logredact"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

var grokDiagnosticCredentialPattern = regexp.MustCompile(`(?i)\b(?:Bearer\s+[A-Za-z0-9._~+/=-]+|xai-[A-Za-z0-9_-]+)`)

func sanitizeGrokUpstreamDiagnostic(message string) string {
	message = grokDiagnosticCredentialPattern.ReplaceAllString(message, "[REDACTED]")
	return logredact.RedactText(sanitizeUpstreamErrorMessage(message), "api_key", "apikey", "authorization", "cookie", "set-cookie")
}

func extractGrokUpstreamErrorMessage(body []byte) string {
	for _, candidate := range grokStructuredErrorMessageCandidates(body) {
		if message := sanitizeGrokUpstreamDiagnostic(candidate); message != "" {
			return truncateString(message, 2048)
		}
	}
	return ""
}

// An unattributed 403, even with generic text or an edge HTML page, does not
// establish an account failure and must not quarantine or rotate the pool.
func isGrokOpaqueForbidden(status int, body []byte) bool {
	if status != http.StatusForbidden || isGrokContentPolicyRejection(status, body) ||
		grokAccountAccessMessage(extractGrokUpstreamErrorMessage(body)) || isGrokSpendingLimitError(body) {
		return false
	}
	var payload any
	if json.Unmarshal(body, &payload) == nil && grokStructuredAccountAccessMarker(payload) {
		return false
	}
	return classifyGrokUpstreamFailure(status, body, "").Class == GrokFailureNone
}

func (s *OpenAIGatewayService) grokUpstreamErrorDetail(body []byte) string {
	if s == nil || s.cfg == nil || !s.cfg.Gateway.LogUpstreamErrorBody {
		return ""
	}
	maxBytes := s.cfg.Gateway.LogUpstreamErrorBodyMaxBytes
	if maxBytes <= 0 {
		maxBytes = 2048
	}
	detail, _ := sanitizeErrorBodyForStorage(sanitizeGrokUpstreamDiagnostic(string(body)), maxBytes)
	return detail
}

// isGrokContentPolicyRejection identifies request-scoped safety refusals from
// xAI. These failures are caused by the prompt or media, so retrying another
// OAuth account cannot change the outcome and would incorrectly drain a pool.
// Keep this matcher deliberately narrow: account entitlement and suspension
// messages may mention policy but must retain the normal account failover path.
func isGrokContentPolicyRejection(statusCode int, responseBody []byte) bool {
	if statusCode != http.StatusForbidden || len(responseBody) == 0 {
		return false
	}
	if grokAccountAccessMessage(string(responseBody)) {
		return false
	}

	var payload any
	if json.Unmarshal(responseBody, &payload) == nil {
		if grokStructuredAccountAccessMarker(payload) {
			return false
		}
		if grokStructuredContentPolicyMarker(payload) {
			return true
		}
		for _, message := range grokStructuredErrorMessageCandidates(responseBody) {
			if strings.EqualFold(strings.TrimSpace(message), "I can't help with that request.") {
				return true
			}
		}
	}

	return grokContentPolicyMessage(string(responseBody))
}

func grokStructuredAccountAccessMarker(value any) bool {
	switch node := value.(type) {
	case map[string]any:
		for key, child := range node {
			normalizedKey := normalizeGrokErrorMarker(key)
			switch normalizedKey {
			case "code", "error_code", "type", "category", "reason":
				if marker, ok := child.(string); ok && isGrokAccountAccessCode(marker) {
					return true
				}
			}
			if grokStructuredAccountAccessMarker(child) {
				return true
			}
		}
	case []any:
		for _, child := range node {
			if grokStructuredAccountAccessMarker(child) {
				return true
			}
		}
	}
	return false
}

func grokStructuredContentPolicyMarker(value any) bool {
	switch node := value.(type) {
	case map[string]any:
		for key, child := range node {
			normalizedKey := normalizeGrokErrorMarker(key)
			switch normalizedKey {
			case "code", "error_code", "type", "category", "reason":
				if marker, ok := child.(string); ok && isGrokContentPolicyCode(marker) {
					return true
				}
			}
			if grokStructuredContentPolicyMarker(child) {
				return true
			}
		}
	case []any:
		for _, child := range node {
			if grokStructuredContentPolicyMarker(child) {
				return true
			}
		}
	}
	return false
}

func normalizeGrokErrorMarker(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, "-", "_")
	value = strings.ReplaceAll(value, " ", "_")
	return value
}

func isGrokContentPolicyCode(value string) bool {
	switch normalizeGrokErrorMarker(value) {
	case "content_filter",
		"content_policy",
		"content_policy_violation",
		"content_moderation",
		"cyber_policy",
		"new_sensitive":
		return true
	default:
		return false
	}
}

func isGrokAccountAccessCode(value string) bool {
	switch normalizeGrokErrorMarker(value) {
	case "account_suspended",
		"account_disabled",
		"user_suspended",
		"user_disabled",
		"subscription_required",
		"entitlement_required",
		"not_entitled",
		"plan_required",
		"invalid_api_key",
		"api_key_expired",
		"api_key_revoked",
		"invalid_token",
		"token_expired",
		"authentication_error":
		// permission-denied is omitted: xAI reuses it for both entitlement
		// refusals and request-scoped safety blocks, so the message decides.
		return true
	default:
		return false
	}
}

func grokAccountAccessMessage(value string) bool {
	lower := strings.ToLower(strings.TrimSpace(value))
	for _, phrase := range []string{
		"account suspended",
		"account has been suspended",
		"account disabled",
		"account has been disabled",
		"user suspended",
		"user has been suspended",
		"subscription required",
		"entitlement required",
		"entitlement denied",
		"not entitled",
		"invalid api key",
		"invalid api_key",
		"invalid token",
		"token expired",
		"expired token",
		"invalid credentials",
		"authentication failed",
	} {
		if strings.Contains(lower, phrase) {
			return true
		}
	}
	return false
}

func grokContentPolicyMessage(value string) bool {
	lower := strings.ToLower(strings.TrimSpace(value))
	if lower == "" {
		return false
	}

	// xAI's media safety responses use these exact phrases. They are specific
	// enough not to classify a generic account-policy or entitlement message.
	for _, phrase := range []string{
		"the moderation feature is not available",
		"image is sensitive",
		"text is sensitive",
		"prohibited content",
		"forbidden content",
		"content policy violation",
		"content policy rejection",
		"content policy rejected",
		"content moderation rejection",
		"content moderation rejected",
		"content moderation blocked",
		"request blocked by content moderation",
		"request rejected by content moderation",
		"request blocked by policy",
		"request rejected by policy",
		"request violates policy",
		"prompt violates content policy",
		"prompt violates policy",
		"input violates content policy",
		"input violates policy",
		"violates usage guidelines",
	} {
		if strings.Contains(lower, phrase) {
			return true
		}
	}

	return false
}

func grokContentPolicyClientMessage(responseBody []byte) string {
	message := extractGrokUpstreamErrorMessage(responseBody)
	if message == "" {
		return "Request blocked by upstream content policy"
	}
	return message
}

// Preserve the request-scoped verdict through every client protocol, including
// a heartbeat that has already committed HTTP 200. No usage is invented here.
func writeGrokContentPolicyError(c *gin.Context, message string) {
	writeRequestAdmissionError(c, http.StatusForbidden, "invalid_request_error", "content_policy_violation", message)
}

func writeRequestAdmissionError(c *gin.Context, status int, errorType, code, message string) {
	compactCommitted := StopOpenAICompactSSEKeepaliveCommitted(c)
	StopOpenAIImagesJSONKeepaliveCommitted(c)
	path := strings.TrimRight(c.Request.URL.Path, "/")
	messageBody := gin.H{"error": gin.H{
		"type": errorType, "code": code, "message": message,
	}}
	if strings.HasSuffix(path, "/messages") {
		messageBody["type"] = "error"
	}
	MarkResponseCommitted(c)
	if compactCommitted || (c.Writer.Written() && strings.HasPrefix(c.Writer.Header().Get("Content-Type"), "text/event-stream")) {
		if !strings.HasSuffix(path, "/messages") && !strings.HasSuffix(path, "/chat/completions") {
			writeOpenAICompactSSEFailureMessage(c, status, code, message)
			return
		}
		MarkOpsStreamError(c, code, message, status)
		event := ""
		if strings.HasSuffix(path, "/messages") {
			event = "event: error\n"
		}
		payload, _ := json.Marshal(messageBody)
		_, _ = fmt.Fprintf(c.Writer, "%sdata: %s\n\n", event, payload)
		c.Writer.Flush()
		return
	}
	c.JSON(status, messageBody)
}

// shouldFailoverGrokUpstreamError is the body-aware counterpart of the
// status-only failover helper. Grok content refusals must stay on the current
// account and be returned to the caller instead of consuming the account pool.
// Free-usage / empty-output / billing bodies also failover even when the HTTP
// status alone would not (e.g. 400 with free-usage-exhausted).
func (s *OpenAIGatewayService) shouldFailoverGrokUpstreamError(statusCode int, responseBody []byte) bool {
	if isGrokContentPolicyRejection(statusCode, responseBody) || isGrokOpaqueForbidden(statusCode, responseBody) || isGrokUnknownInputItemTypeError(statusCode, responseBody) {
		return false
	}
	// A 422 emitted by xAI's ModelInput decoder is account/runtime compatibility,
	// not quota exhaustion. Another account may run a different upstream build,
	// so fail over without applying an account cooldown.
	if isGrokDecoderCompatibilityError(statusCode, responseBody) {
		return true
	}
	decision := classifyGrokUpstreamFailure(statusCode, responseBody, "")
	switch decision.Class {
	case GrokFailureFreeUsage, GrokFailureEmptyUpstream, GrokFailureBilling, GrokFailureModelCapacity, GrokFailureCompatibility:
		return decision.ShouldFailover
	}
	return s.shouldFailoverUpstreamError(statusCode)
}

func isGrokUnknownInputItemTypeError(statusCode int, responseBody []byte) bool {
	if statusCode != http.StatusUnprocessableEntity {
		return false
	}
	for _, candidate := range grokStructuredErrorMessageCandidates(responseBody) {
		message := strings.ToLower(candidate)
		inputPath := strings.Contains(message, "input[") || strings.Contains(message, "input.")
		decoder := strings.Contains(message, "decode") || strings.Contains(message, "deserializ")
		if inputPath && decoder && strings.Contains(message, "unknown item type") {
			return true
		}
	}
	return false
}

func isGrokDecoderCompatibilityError(statusCode int, responseBody []byte) bool {
	if statusCode != http.StatusUnprocessableEntity || len(responseBody) == 0 {
		return false
	}
	for _, candidate := range grokStructuredErrorMessageCandidates(responseBody) {
		message := strings.ToLower(candidate)
		decoderSignal := strings.Contains(message, "untagged enum") ||
			strings.Contains(message, "decode") ||
			strings.Contains(message, "deserialize") ||
			strings.Contains(message, "deserializ") ||
			strings.Contains(message, "decoder")
		inputSignal := strings.Contains(message, "modelinput") ||
			strings.Contains(message, "model input") ||
			strings.Contains(message, "input[") ||
			strings.Contains(message, "input.")
		messageContentSignal := (strings.Contains(message, "messages[") ||
			strings.Contains(message, "messages.")) &&
			strings.Contains(message, "content") &&
			strings.Contains(message, "did not match any variant")
		if decoderSignal && (inputSignal || messageContentSignal) {
			return true
		}
	}
	return false
}

func grokStructuredErrorMessageCandidates(body []byte) []string {
	candidates := make([]string, 0, 6)
	appendCandidate := func(result gjson.Result) {
		if !result.Exists() {
			return
		}
		value := strings.TrimSpace(result.String())
		if value != "" {
			candidates = append(candidates, value)
		}
	}
	appendCandidate(gjson.GetBytes(body, "error.message"))
	appendCandidate(gjson.GetBytes(body, "error.error"))
	errorNode := gjson.GetBytes(body, "error")
	if errorNode.Type == gjson.String {
		appendCandidate(errorNode)
	}
	appendCandidate(gjson.GetBytes(body, "message"))
	appendCandidate(gjson.GetBytes(body, "detail"))
	if !json.Valid(body) {
		if plaintext := strings.TrimSpace(string(body)); plaintext != "" {
			candidates = append(candidates, plaintext)
		}
	}
	return candidates
}

// applyGrokForbiddenPolicy applies an administrator's existing temporary
// unschedulable rules to a non-content 403. It reports true only when a rule
// matched; unmatched responses retain the legacy entitlement cooldown.
func (s *OpenAIGatewayService) applyGrokForbiddenPolicy(ctx context.Context, account *Account, responseBody []byte) bool {
	if account == nil || !account.IsTempUnschedulableEnabled() {
		return false
	}

	matches := matchTempUnschedulableRules(account, http.StatusForbidden, responseBody)
	if len(matches) == 0 {
		return false
	}

	match := matches[0]
	// Reuse the central policy implementation when it has a repository. This
	// preserves the existing reason/cache format and avoids duplicating writes.
	if s != nil && s.rateLimitService != nil && s.rateLimitService.accountRepo != nil {
		stateCtx, cancel := openAIAccountStateContext(ctx)
		handled := s.rateLimitService.tryTempUnschedulable(
			stateCtx,
			account,
			http.StatusForbidden,
			responseBody,
		)
		cancel()
		if handled {
			return true
		}
	}

	// A partially constructed service (for example a unit-test gateway) still
	// honors the configured duration instead of silently falling back to 30m.
	cooldown := time.Duration(match.rule.DurationMinutes) * time.Minute
	if cooldown > 0 {
		s.tempUnscheduleGrok(ctx, account, cooldown, "grok configured forbidden rule")
	}
	return true
}
