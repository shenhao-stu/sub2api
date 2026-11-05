package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func openAIRelaySessionScopeEnabled(account *Account) bool {
	if account == nil || account.Platform != PlatformOpenAI || account.Type != AccountTypeAPIKey {
		return false
	}
	enabled, _ := account.Extra["openai_relay_session_scope"].(bool)
	return enabled
}

// Only authenticated context tags participate; a client-supplied identity header
// is not a tenant. Sorting a copy keeps concurrent requests independent.
func openAIRelayPrincipal(ctx context.Context, userID, apiKeyID int64) string {
	if userID <= 0 || apiKeyID <= 0 {
		return ""
	}
	tags := slices.Clone(AccountAffinityTagsFromContext(ctx))
	slices.Sort(tags)
	encoded, _ := json.Marshal([]any{userID, apiKeyID, tags})
	return fmt.Sprintf("%x", sha256.Sum256(encoded))
}

func openAIRelayOwnerKey(responseID, principal string) string {
	return "relay-scope-v1:" + principal + ":" + responseID
}

func scopedOpenAIRelayValue(principal, raw string) string {
	return deriveStableUUIDv4("sub2api:relay-session:v1:" + principal + ":" + strings.TrimSpace(raw))
}

var openAIRelaySessionHeaders = []string{
	"x-session-id", "session-id", "session_id", "x-conversation-id", "conversation_id", "conversation-id",
}

var openAIRelaySessionPaths = []string{
	"session_id", "conversation_id", "prompt_cache_key",
	"client_metadata.session_id", "client_metadata.session-id",
	"client_metadata.conversation_id", "client_metadata.conversation-id",
	"client_metadata.thread_id", "client_metadata.thread-id",
}

func projectOpenAIRelaySession(body []byte, inbound http.Header, principal, fallback string) ([]byte, http.Header, error) {
	headers := make(http.Header)
	canonical := ""
	for _, name := range openAIRelaySessionHeaders {
		if raw := strings.TrimSpace(inbound.Get(name)); raw != "" {
			value := scopedOpenAIRelayValue(principal, raw)
			headers.Set(name, value)
			if canonical == "" {
				canonical = value
			}
		}
	}
	for _, path := range openAIRelaySessionPaths {
		raw := gjson.GetBytes(body, path)
		if raw.Type != gjson.String || strings.TrimSpace(raw.Str) == "" {
			continue
		}
		value := scopedOpenAIRelayValue(principal, raw.Str)
		var err error
		body, err = sjson.SetBytes(body, path, value)
		if err != nil {
			return nil, nil, err
		}
		if canonical == "" {
			canonical = value
		}
	}
	// A request with no session hint must not fall back to a shared upstream key
	// and identical first prompt. Full-history requests remain stateless; response
	// continuations retain their provider-issued ID and ownership binding.
	if canonical == "" {
		canonical = scopedOpenAIRelayValue(principal, fallback)
	}
	headers.Set("session-id", canonical)
	headers.Set("session_id", canonical)
	return body, headers, nil
}

func (s *OpenAIGatewayService) prepareOpenAIRelaySession(ctx context.Context, c *gin.Context, account *Account, body []byte) ([]byte, http.Header, error) {
	if !openAIRelaySessionScopeEnabled(account) {
		return body, nil, nil
	}
	var key *APIKey
	if c != nil {
		value, _ := c.Get("api_key")
		key, _ = value.(*APIKey)
	}
	principal := ""
	if key != nil {
		principal = openAIRelayPrincipal(ctx, key.UserID, key.ID)
	}
	if principal == "" || c.Request == nil {
		return nil, nil, errors.New("relay session scope requires an authenticated API key owner")
	}
	if responseID := strings.TrimSpace(gjson.GetBytes(body, "previous_response_id").String()); responseID != "" {
		userID, keyID, found, err := s.getOpenAIWSStateStore().GetHTTPResponseOwner(ctx, getOpenAIGroupIDFromContext(c), openAIRelayOwnerKey(responseID, principal))
		if err != nil && !errors.Is(err, ErrStickySessionNotFound) {
			return nil, nil, err
		}
		if !found || userID != key.UserID || keyID != key.ID {
			message := "previous_response_id is not available for this user"
			writeOpenAIResponsesFallbackError(c, http.StatusBadRequest, "invalid_request_error", message)
			return nil, nil, errors.New(message)
		}
	}
	const fallbackKey = "openai_relay_session_fallback"
	fallback := c.GetString(fallbackKey)
	if fallback == "" {
		fallback = uuid.NewString()
		c.Set(fallbackKey, fallback)
	}
	return projectOpenAIRelaySession(body, c.Request.Header, principal, fallback)
}

func applyOpenAIRelaySessionHeaders(target, scoped http.Header) {
	for name, values := range scoped {
		target[name] = slices.Clone(values)
	}
}
