package service

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

const AnthropicPoolWindowCooldownReason GatewayFailureReason = "anthropic_pool_window_cooldown"

const anthropicPoolWindowMessage = "号池账号均被上游限流，稍后再试"

// The vm2api public pool envelope is distinct from an individual provider 429.
// Metered or otherwise extended errors stay on the ordinary forwarding path.
func isAnthropicPoolWindowRejection(body []byte) bool {
	root, ok := receiptObject(body)
	if !ok || !onlyPoolWindowFields(root, "type", "error", "status_code", "status") {
		return false
	}
	if kind, present := root["type"]; present && string(kind) != `"error"` {
		return false
	}
	failure, ok := receiptObject(root["error"])
	if !ok || !onlyPoolWindowFields(failure, "type", "code", "message", "status_code", "status") {
		return false
	}
	for _, status := range []string{string(root["status_code"]), string(failure["status_code"]), string(root["status"]), string(failure["status"])} {
		if status != "" && status != "429" {
			return false
		}
	}
	return string(failure["type"]) == `"rate_limit_error"` &&
		string(failure["code"]) == `"upstream_rate_limit"` &&
		string(failure["message"]) == `"`+anthropicPoolWindowMessage+`"`
}

func onlyPoolWindowFields(object map[string]json.RawMessage, allowed ...string) bool {
	for field := range object {
		found := false
		for _, name := range allowed {
			if field == name {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (s *GatewayService) anthropicPoolWindowRejection(c *gin.Context, account *Account, resp *http.Response, attempt int) *UpstreamFailoverError {
	if account == nil || resp == nil || account.Platform != PlatformAnthropic || account.Type != AccountTypeAPIKey ||
		account.IsCustomErrorCodesEnabled() || resp.StatusCode != http.StatusTooManyRequests || resp.Body == nil {
		return nil
	}
	source := resp.Body
	const maxBody = 8192
	body, err := awaitAnthropicBody(c, source, func() ([]byte, error) {
		return io.ReadAll(io.LimitReader(source, maxBody))
	})
	resp.Body = &anthropicErrorPrefixBody{prefix: bytes.NewReader(body), source: source, readErr: err}
	quotaGuard := IsAnthropicQuotaGuardRejection(resp.StatusCode, body)
	poolWindow := account.IsPoolMode() && isAnthropicPoolWindowRejection(body)
	if err != nil || len(body) >= maxBody || (!quotaGuard && !poolWindow) ||
		len(matchTempUnschedulableRules(account, resp.StatusCode, body)) > 0 {
		return nil
	}
	_ = resp.Body.Close()
	reason := AnthropicPoolWindowCooldownReason
	message := anthropicPoolWindowMessage
	if quotaGuard {
		reason = GatewayFailureReason(AnthropicQuotaGuardCode)
		message = "Request exceeds this account's remaining quota budget; no model dispatch"
	}
	appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
		Platform: account.Platform, AccountID: account.ID, AccountName: account.Name,
		UpstreamStatusCode: resp.StatusCode, UpstreamRequestID: firstNonEmpty(UpstreamRequestIDFromHeaders(account, resp.Header), strings.TrimSpace(resp.Header.Get("X-Request-Id"))),
		Passthrough: true, Kind: string(reason), Message: message,
	})
	return &UpstreamFailoverError{
		StatusCode: resp.StatusCode, ResponseBody: body, ResponseHeaders: resp.Header.Clone(),
		Reason: reason, ExcludeFromRetry: true, FirstAttemptQuotaGuard: quotaGuard && attempt == 1,
	}
}
