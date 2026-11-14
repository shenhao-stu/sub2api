package service

import (
	"bytes"
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
	if !ok || root["usage"] != nil || root["message"] != nil {
		return false
	}
	if kind, present := root["type"]; present && string(kind) != `"error"` {
		return false
	}
	failure, ok := receiptObject(root["error"])
	if !ok || failure["usage"] != nil || failure["details"] != nil {
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

func (s *GatewayService) anthropicPoolWindowRejection(c *gin.Context, account *Account, resp *http.Response) *UpstreamFailoverError {
	if account == nil || resp == nil || account.Platform != PlatformAnthropic || account.Type != AccountTypeAPIKey ||
		!account.IsPoolMode() || account.IsCustomErrorCodesEnabled() || resp.StatusCode != http.StatusTooManyRequests || resp.Body == nil {
		return nil
	}
	source := resp.Body
	const maxBody = 8192
	body, err := awaitAnthropicBody(c, source, func() ([]byte, error) {
		return io.ReadAll(io.LimitReader(source, maxBody))
	})
	resp.Body = &anthropicErrorPrefixBody{prefix: bytes.NewReader(body), source: source, readErr: err}
	if err != nil || len(body) >= maxBody || !isAnthropicPoolWindowRejection(body) {
		return nil
	}
	_ = resp.Body.Close()
	appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
		Platform: account.Platform, AccountID: account.ID, AccountName: account.Name,
		UpstreamStatusCode: resp.StatusCode, UpstreamRequestID: firstNonEmpty(UpstreamRequestIDFromHeaders(account, resp.Header), strings.TrimSpace(resp.Header.Get("X-Request-Id"))),
		Passthrough: true, Kind: string(AnthropicPoolWindowCooldownReason), Message: anthropicPoolWindowMessage,
	})
	return &UpstreamFailoverError{
		StatusCode: resp.StatusCode, ResponseBody: body, ResponseHeaders: resp.Header.Clone(),
		Reason: AnthropicPoolWindowCooldownReason, ExcludeFromRetry: true,
	}
}
