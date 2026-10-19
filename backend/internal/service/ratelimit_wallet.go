package service

import (
	"net/http"
	"strings"

	"github.com/tidwall/gjson"
)

// A gateway pool can rotate its internal accounts, but cannot replenish the
// credential's wallet. Only an explicit billing rejection overrides pool mode.
func isAPIKeyWalletExhausted(account *Account, status int, body []byte) bool {
	return account != nil && account.Type == AccountTypeAPIKey && IsUpstreamWalletExhausted(status, body)
}

// IsUpstreamWalletExhausted recognizes structured wallet rejections, excluding
// ordinary authorization failures and request-specific quota guards.
func IsUpstreamWalletExhausted(status int, body []byte) bool {
	if (status != http.StatusPaymentRequired && status != http.StatusForbidden && status != http.StatusTooManyRequests) ||
		!gjson.ValidBytes(body) {
		return false
	}
	if IsUpstreamQuotaExhausted(http.StatusTooManyRequests, body) &&
		isWalletBalanceMessage(gjson.GetBytes(body, "error.message").String()) {
		return true
	}
	if status != http.StatusForbidden {
		return false
	}
	if gjson.GetBytes(body, "code").String() == "INSUFFICIENT_BALANCE" &&
		isWalletBalanceMessage(gjson.GetBytes(body, "message").String()) {
		return true
	}
	errorNode := gjson.GetBytes(body, "error")
	if !isWalletBalanceMessage(errorNode.Get("message").String()) {
		return false
	}
	return errorNode.Get("type").String() == "billing_error" ||
		errorNode.Get("code").String() == "bad_response_status_code"
}

func isWalletBalanceMessage(message string) bool {
	switch strings.ToLower(strings.TrimSpace(message)) {
	case "insufficient account balance", "insufficient balance":
		return true
	default:
		return false
	}
}
