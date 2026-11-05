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

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"go.uber.org/zap"
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
	"thread-id", "x-codex-parent-thread-id", "x-codex-image-turn-id",
}

var openAIRelaySessionPaths = []string{
	"session_id", "conversation_id", "prompt_cache_key", "thread_id", "thread-id", "parent_thread_id", "parent-thread-id",
	"client_metadata.session_id", "client_metadata.session-id",
	"client_metadata.conversation_id", "client_metadata.conversation-id",
	"client_metadata.thread_id", "client_metadata.thread-id",
	"client_metadata.parent_thread_id", "client_metadata.parent-thread-id",
}

// Correlation IDs are tenant-scoped; unrelated metadata and opaque state are
// preserved. Never rebuild malformed client metadata or invent client signals.
func projectOpenAIRelayMetadata(raw, principal string) (string, error) {
	if !gjson.Valid(raw) || !gjson.Parse(raw).IsObject() {
		return "", errors.New("x-codex-turn-metadata must be a JSON object")
	}
	for _, field := range []string{"session_id", "session-id", "conversation_id", "conversation-id", "thread_id", "thread-id", "parent_thread_id", "parent-thread-id", "turn_id"} {
		value := gjson.Get(raw, field)
		if !value.Exists() || value.Type == gjson.Null {
			continue
		}
		if value.Type != gjson.String {
			return "", fmt.Errorf("turn metadata %s must be a string", field)
		}
		if strings.TrimSpace(value.Str) == "" {
			continue
		}
		var err error
		raw, err = sjson.Set(raw, field, scopedOpenAIRelayValue(principal, value.Str))
		if err != nil {
			return "", err
		}
	}
	return raw, nil
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
	if raw := inbound.Get("x-codex-turn-metadata"); raw != "" {
		projected, err := projectOpenAIRelayMetadata(raw, principal)
		if err != nil {
			return nil, nil, err
		}
		headers.Set("x-codex-turn-metadata", projected)
	}
	const metadataPath = "client_metadata.x-codex-turn-metadata"
	if raw := gjson.GetBytes(body, metadataPath); raw.Exists() && raw.Type != gjson.Null {
		metadata := raw.Raw
		if raw.Type == gjson.String {
			metadata = raw.Str
		}
		projected, err := projectOpenAIRelayMetadata(metadata, principal)
		if err != nil {
			return nil, nil, err
		}
		if raw.Type == gjson.String {
			body, err = sjson.SetBytes(body, metadataPath, projected)
		} else {
			body, err = sjson.SetRawBytes(body, metadataPath, []byte(projected))
		}
		if err != nil {
			return nil, nil, err
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
	state := extractOpenAICodexTurnState(c.Request.Header)
	if state != "" {
		ownerKey := openAIRelayTurnStateKey(principal, account.ID, state)
		userID, keyID, found, err := s.getOpenAIWSStateStore().GetHTTPResponseOwner(ctx, getOpenAIGroupIDFromContext(c), ownerKey)
		if err != nil && !errors.Is(err, ErrStickySessionNotFound) {
			return nil, nil, err
		}
		if !found || userID != key.UserID || keyID != key.ID {
			message := "x-codex-turn-state is not available for this user and upstream account"
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
	projected, headers, err := projectOpenAIRelaySession(body, c.Request.Header, principal, fallback)
	if err != nil {
		writeOpenAIResponsesFallbackError(c, http.StatusBadRequest, "invalid_request_error", err.Error())
		return nil, nil, err
	}
	if state != "" {
		headers.Set(openAICodexTurnStateHeader, state)
	}
	return projected, headers, nil
}

func applyOpenAIRelaySessionHeaders(target, scoped http.Header) {
	for name, values := range scoped {
		target[name] = slices.Clone(values)
	}
}

func openAIRelayTurnStateKey(principal string, accountID int64, state string) string {
	return fmt.Sprintf("relay-turn-v1:%s:%d:%x", principal, accountID, sha256.Sum256([]byte(state)))
}

// Record only state committed to this authenticated caller, using the existing
// bounded Redis ownership store so restarts cannot erase the isolation boundary.
func (s *OpenAIGatewayService) bindOpenAIRelayTurnState(c *gin.Context, account *Account, state string) {
	if !openAIRelaySessionScopeEnabled(account) || state == "" || c == nil || c.Request == nil {
		return
	}
	value, _ := c.Get("api_key")
	key, _ := value.(*APIKey)
	if key == nil {
		return
	}
	principal := openAIRelayPrincipal(c.Request.Context(), key.UserID, key.ID)
	if principal == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(c.Request.Context()), openAIWSStateStoreRedisTimeout)
	defer cancel()
	ownerKey := openAIRelayTurnStateKey(principal, account.ID, state)
	if err := s.BindOpenAIHTTPResponseOwner(ctx, getOpenAIGroupIDFromContext(c), ownerKey, key.UserID, key.ID); err != nil {
		logger.L().Warn("openai.relay_turn_state_owner_failed", zap.Int64("account_id", account.ID), zap.Error(err))
	}
}
