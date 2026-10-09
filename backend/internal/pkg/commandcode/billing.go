package commandcode

import (
	"encoding/json"
	"net/http"
	"strings"
)

const maxBillingBody = 1 << 20
const CreditExhaustedCode = "commandcode_credit_exhausted"

func IsCreditExhausted(status int, body []byte) bool {
	if status != http.StatusBadRequest || len(body) > maxBillingBody {
		return false
	}
	var payload struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &payload) != nil {
		return false
	}
	return payload.Error.Code == CreditExhaustedCode ||
		(payload.Error.Code == "BAD_REQUEST" && strings.HasPrefix(strings.ToLower(strings.TrimSpace(payload.Error.Message)),
			"you have insufficient credits to make this request."))
}

func (c *Client) applyCLIHeaders(req *http.Request) {
	req.Header.Set("x-cli-environment", "production")
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}
	if c.Version != "" {
		req.Header.Set("x-command-code-version", c.Version)
	}
}
