package service

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	GeminiPaymentRequiredReason  GatewayFailureReason = "gemini_prepayment_depleted"
	GeminiPaymentRequiredMessage                      = "Upstream prepaid credits are depleted; please contact the administrator to replenish the provider balance"
	geminiPaymentBodyLimit                            = 8192
)

// Only a complete provider rejection proves that switching cannot repeat a
// generation. HTTP 402 alone does not prove account-wide wallet exhaustion.
func geminiPaymentDepleted(body []byte) bool {
	envelope, ok := geminiUniqueErrorObject(body)
	if !ok || len(envelope) != 1 {
		return false
	}
	failure, ok := geminiUniqueErrorObject(envelope["error"])
	if !ok {
		return false
	}
	for key := range failure {
		switch key {
		case "code", "status", "message", "details":
		default:
			return false
		}
	}
	// No live raw details were retained. Unknown typed details may narrow the
	// rejection scope; do not infer account-wide depletion from them.
	if details, exists := failure["details"]; exists {
		var values []json.RawMessage
		if json.Unmarshal(details, &values) != nil || len(values) != 0 {
			return false
		}
	}
	var code int
	var status, message string
	if json.Unmarshal(failure["code"], &code) != nil || code != http.StatusPaymentRequired ||
		json.Unmarshal(failure["status"], &status) != nil || status != "RESOURCE_EXHAUSTED" ||
		json.Unmarshal(failure["message"], &message) != nil {
		return false
	}
	// The remainder is a provider billing/help link, never returned to customers.
	message = strings.TrimSpace(message)
	return message == "Your prepayment credits are depleted." ||
		strings.HasPrefix(message, "Your prepayment credits are depleted. Please go to AI Studio ")
}

func geminiUniqueErrorObject(raw []byte) (map[string]json.RawMessage, bool) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, false
	}
	result := map[string]json.RawMessage{}
	for decoder.More() {
		token, err = decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok {
			return nil, false
		}
		if _, exists := result[key]; exists {
			return nil, false
		}
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return nil, false
		}
		result[key] = value
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, false
	}
	if _, err = decoder.Token(); err != io.EOF {
		return nil, false
	}
	return result, true
}

// Inspect before policy helpers consume/rebuild the body and lose transport
// completeness. Unknown/truncated responses keep the existing no-replay path.
func (s *GeminiMessagesCompatService) geminiPaymentFailover(ctx context.Context, c *gin.Context, account *Account, resp *http.Response, model string) *UpstreamFailoverError {
	if resp == nil || resp.StatusCode != http.StatusPaymentRequired || resp.Body == nil || account == nil || account.Platform != PlatformGemini {
		return nil
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, geminiPaymentBodyLimit+1))
	_ = resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(body))
	if readErr != nil || len(body) > geminiPaymentBodyLimit ||
		(resp.ContentLength > 0 && resp.ContentLength != int64(len(body))) || !geminiPaymentDepleted(body) {
		return nil
	}
	if account.ShouldHandleErrorCode(resp.StatusCode) {
		policy := ErrorPolicyNone
		if account.IsPoolMode() && !account.IsCustomErrorCodesEnabled() {
			policy = ErrorPolicySkipped
		}
		if s.rateLimitService != nil {
			stateCtx, cancel := openAIAccountStateContext(ctx)
			policy = s.rateLimitService.CheckErrorPolicy(stateCtx, account, resp.StatusCode, body, model)
			cancel()
		}
		if policy == ErrorPolicyNone || policy == ErrorPolicyMatched {
			s.persistGeminiPaymentCooldown(ctx, account, resp.Header)
		}
	}
	requestID := resp.Header.Get("x-goog-request-id")
	if requestID == "" {
		requestID = resp.Header.Get("x-request-id")
	}
	appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
		Platform: account.Platform, AccountID: account.ID, AccountName: account.Name,
		ProxyID: opsUpstreamProxyID(account), ProxyName: opsUpstreamProxyName(account),
		UpstreamStatusCode: resp.StatusCode, UpstreamRequestID: requestID,
		Kind: "failover", Message: GeminiPaymentRequiredMessage,
	})
	next := NextAccountRetry
	// Explicit cached content belongs to its original credential. Never migrate
	// the request to a different account simply because that wallet is depleted.
	if geminiCachedContentBoundFromContext(ctx) {
		next = NextAccountStop
	}
	return &UpstreamFailoverError{
		StatusCode: http.StatusPaymentRequired, ResponseBody: body,
		ExcludeFromRetry: true, Scope: GatewayFailureScopeAccount,
		Reason: GeminiPaymentRequiredReason, NextAccountAction: next,
		ClientStatusCode: http.StatusPaymentRequired, ClientMessage: GeminiPaymentRequiredMessage,
	}
}

func (s *GeminiMessagesCompatService) persistGeminiPaymentCooldown(ctx context.Context, account *Account, headers http.Header) {
	repo, ok := s.accountRepo.(accountRateLimitExtender)
	if !ok {
		slog.Warn("gemini_payment_cooldown_unavailable", "account_id", account.ID)
		return
	}
	stateCtx, cancel := openAIAccountStateContext(ctx)
	defer cancel()
	// This is a recoverable local billing backoff, not an official reset time.
	until := upstreamQuotaResetAt(time.Now(), headers, upstreamQuotaCooldown, true)
	if err := repo.SetRateLimitedIfLater(stateCtx, account.ID, until); err != nil {
		slog.Warn("gemini_payment_cooldown_failed", "account_id", account.ID, "error", err)
		return
	}
	if s.rateLimitService != nil {
		current, err := s.accountRepo.GetByID(stateCtx, account.ID)
		if err == nil && current != nil && current.RateLimitResetAt != nil {
			s.rateLimitService.notifyAccountSchedulingBlocked(current, *current.RateLimitResetAt, "gemini_prepayment_depleted")
		}
	}
	slog.Warn("gemini_payment_cooldown", "account_id", account.ID, "reset_at", until.UTC())
}
