package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const anthropicRelayPrincipalHeader = "X-Native-Relay-Principal"

func usesAnthropicRelayPrincipal(account *Account) bool {
	if account == nil || account.Platform != PlatformAnthropic || account.Type != AccountTypeAPIKey {
		return false
	}
	enabled, _ := account.Extra["anthropic_relay_session_scope"].(bool)
	return enabled
}

// Account tests authenticate as a private, single-use maintenance tenant.
// They have no customer API key and must not share a customer's session scope.
func setAnthropicRelayProbePrincipal(headers http.Header) {
	for key := range headers {
		if strings.EqualFold(key, anthropicRelayPrincipalHeader) || strings.EqualFold(key, "X-Request-Id") {
			delete(headers, key)
		}
	}
	ref := uuid.NewString()
	headers.Set("X-Request-Id", ref)
	headers.Set(anthropicRelayPrincipalHeader, fmt.Sprintf("%x", sha256.Sum256([]byte("anthropic-relay-probe:"+ref))))
}

// A private native relay needs a server-authenticated tenant even when Getoken
// shares one transit key. Never trust a client header or account override.
func applyAnthropicRelayPrincipal(ctx context.Context, c *gin.Context, account *Account, headers http.Header) error {
	for key := range headers {
		if strings.EqualFold(key, anthropicRelayPrincipalHeader) {
			delete(headers, key)
		}
	}
	if !usesAnthropicRelayPrincipal(account) {
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
	// A shared transit key without a verified tenant would collapse every
	// downstream customer into one conversation namespace.
	requireTag, _ := account.Extra["anthropic_relay_require_affinity"].(bool)
	if requireTag && len(AccountAffinityTagsFromContext(ctx)) != 1 {
		return errors.New("native relay requires verified transit identity")
	}
	principal := openAIRelayPrincipal(ctx, key.UserID, key.ID)
	if principal == "" {
		return errors.New("native relay requires an authenticated API key owner")
	}
	headers.Set(anthropicRelayPrincipalHeader, principal)
	return nil
}
