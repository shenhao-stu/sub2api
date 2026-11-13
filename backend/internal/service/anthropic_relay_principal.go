package service

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// A private native relay needs a server-authenticated tenant even when Getoken
// shares one transit key. Never trust a client header or account override.
func applyAnthropicRelayPrincipal(ctx context.Context, c *gin.Context, account *Account, headers http.Header) error {
	const name = "X-Native-Relay-Principal"
	for key := range headers {
		if strings.EqualFold(key, name) {
			delete(headers, key)
		}
	}
	if account == nil || account.Platform != PlatformAnthropic || account.Type != AccountTypeAPIKey {
		return nil
	}
	enabled, _ := account.Extra["anthropic_relay_session_scope"].(bool)
	if !enabled {
		return nil
	}
	var key *APIKey
	if c != nil {
		value, _ := c.Get("api_key")
		key, _ = value.(*APIKey)
	}
	if key == nil {
		return errors.New("native relay requires an authenticated API key owner")
	}
	principal := openAIRelayPrincipal(ctx, key.UserID, key.ID)
	if principal == "" {
		return errors.New("native relay requires an authenticated API key owner")
	}
	headers.Set(name, principal)
	return nil
}
