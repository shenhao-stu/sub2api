package service

import (
	"net/http"
	"strings"

	"github.com/tidwall/gjson"
)

// A gateway pool can rotate its internal accounts, but cannot replenish the
// credential's wallet. Only an explicit billing rejection overrides pool mode.
func isAPIKeyWalletExhausted(account *Account, status int, body []byte) bool {
	if account == nil || account.Type != AccountTypeAPIKey ||
		(status != http.StatusPaymentRequired && status != http.StatusTooManyRequests) {
		return false
	}
	return IsUpstreamQuotaExhausted(http.StatusTooManyRequests, body) &&
		strings.EqualFold(strings.TrimSpace(gjson.GetBytes(body, "error.message").String()), "Insufficient account balance")
}
