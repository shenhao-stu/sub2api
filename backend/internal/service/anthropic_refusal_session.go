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
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	gocache "github.com/patrickmn/go-cache"
	"github.com/tidwall/gjson"
)

const anthropicRefusalSessionContextKey = "anthropic_refusal_session"
const pendingAnthropicRefusalTTL = 24 * time.Hour

var errAnthropicSessionAdmission = errors.New("anthropic session admission denied")

// Positive marks cover this process during a shared storage write or a failed acknowledgement.
var pendingAnthropicRefusalMarks = gocache.New(pendingAnthropicRefusalTTL, time.Hour)

type anthropicRefusalSession struct {
	store      CyberSessionBlockStore
	journalDir string
	key        string
	ctx        context.Context
	marked     bool
}

func anthropicRefusalFromContext(c *gin.Context) *anthropicRefusalSession {
	if c == nil {
		return nil
	}
	value, _ := c.Get(anthropicRefusalSessionContextKey)
	guard, _ := value.(*anthropicRefusalSession)
	return guard
}

func anthropicRefusalSessionKey(ctx context.Context, key *APIKey, metadata string) (string, bool) {
	if key == nil || len(AccountAffinityTagsFromContext(ctx)) != 1 {
		return "", false
	}
	principal := openAIRelayPrincipal(ctx, key.UserID, key.ID)
	parsed := ParseMetadataUserID(metadata)
	if principal == "" || parsed == nil {
		return "", false
	}
	session, err := uuid.Parse(parsed.SessionID)
	if err != nil || session == uuid.Nil || !strings.EqualFold(session.String(), parsed.SessionID) {
		return "", false
	}
	return fmt.Sprintf("anthropic-refusal:v1:%s:%x", principal, sha256.Sum256([]byte(session.String()))), true
}

// AdmitAnthropicRefusalSession runs before the protected Messages route waits
// for quota or selects an account. A shared transit key is not a tenant by
// itself: AccountAffinity must already have authenticated the caller tag.
func (s *GatewayService) AdmitAnthropicRefusalSession(ctx context.Context, c *gin.Context, key *APIKey, parsed *ParsedRequest) error {
	if s.cfg == nil || key == nil || parsed == nil || !slices.Contains(s.cfg.Gateway.AnthropicRefusalRelayKeyIDs, key.ID) {
		return nil
	}
	// Ordinary Messages requests may omit metadata. Without a verified
	// session we have no safe scope to quarantine at this layer.
	blockKey, ok := anthropicRefusalSessionKey(ctx, key, parsed.MetadataUserID)
	if !ok {
		return nil
	}
	store, ok := s.cache.(CyberSessionBlockStore)
	if !ok || store == nil {
		return rejectAnthropicSessionAdmission(c, http.StatusServiceUnavailable, "session_protection_unavailable", "Session protection is temporarily unavailable.")
	}
	c.Set(anthropicRefusalSessionContextKey, &anthropicRefusalSession{store: store, journalDir: s.cfg.Gateway.AnthropicRefusalJournalDir, key: blockKey, ctx: ctx})
	return checkAnthropicRefusalSession(ctx, c)
}

func checkAnthropicRefusalSession(ctx context.Context, c *gin.Context) error {
	guard := anthropicRefusalFromContext(c)
	if guard == nil {
		return nil
	}
	if _, ok := pendingAnthropicRefusalMarks.Get(guard.key); ok {
		return rejectQuarantinedAnthropicSession(c)
	}
	marked, journalErr := readAnthropicRefusalMarker(guard.journalDir, guard.key)
	if marked {
		return rejectQuarantinedAnthropicSession(c)
	}
	readCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	hit, redisErr := guard.store.FindCyberSessionBlocked(readCtx, []string{guard.key})
	if hit != "" {
		return rejectQuarantinedAnthropicSession(c)
	}
	if journalErr != nil || redisErr != nil {
		return rejectAnthropicSessionAdmission(c, http.StatusServiceUnavailable, "session_protection_unavailable", "Session protection is temporarily unavailable.")
	}
	return nil
}

func rejectQuarantinedAnthropicSession(c *gin.Context) error {
	return rejectAnthropicSessionAdmission(c, http.StatusBadRequest, "session_quarantined", "This session was rejected by the upstream policy. Start a new conversation without the rejected history.")
}

func rejectAnthropicSessionAdmission(c *gin.Context, status int, code, message string) error {
	if status == http.StatusBadRequest {
		MarkOpsClientBusinessLimited(c, OpsClientBusinessLimitedReasonLocalPolicyDenied)
	}
	kind := "invalid_request_error"
	if status == http.StatusServiceUnavailable {
		kind = "api_error"
	}
	MarkResponseCommitted(c)
	WriteAnthropicErrorResponse(c, status, gin.H{"type": "error", "error": gin.H{
		"type": kind, "code": code, "message": message,
	}})
	return errAnthropicSessionAdmission
}

func (s *GatewayService) trustedAnthropicRefusal(c *gin.Context, account *Account) *anthropicRefusalSession {
	if s.cfg == nil || account == nil || !account.IsAnthropicAPIKeyPassthroughEnabled() ||
		!slices.Contains(s.cfg.Gateway.AnthropicRefusalSourceAccountIDs, account.ID) {
		return nil
	}
	return anthropicRefusalFromContext(c)
}

// Only structural upstream decisions are observed. Local cached/device
// refusals, ordinary capacity errors and assistant prose never mark a session.
func isOfficialAnthropicSessionRefusal(payload []byte) bool {
	if len(payload) == 0 || len(payload) > 64<<10 {
		return false
	}
	// Avoid constructing a JSON field map for each ordinary content token.
	switch gjson.GetBytes(payload, "type").String() {
	case "message", "message_delta", "error":
	default:
		return false
	}
	obj, ok := receiptObject(payload)
	if !ok {
		return false
	}
	var kind string
	if json.Unmarshal(obj["type"], &kind) != nil {
		return false
	}
	var stop string
	switch kind {
	case "message":
		var role string
		return json.Unmarshal(obj["role"], &role) == nil && role == "assistant" &&
			json.Unmarshal(obj["stop_reason"], &stop) == nil && stop == "refusal"
	case "message_delta":
		delta, ok := receiptObject(obj["delta"])
		return ok && json.Unmarshal(delta["stop_reason"], &stop) == nil && stop == "refusal"
	case "error":
		failure, ok := receiptObject(obj["error"])
		if !ok {
			return false
		}
		var errType, code, source string
		details, ok := receiptObject(failure["details"])
		return ok && json.Unmarshal(failure["type"], &errType) == nil && errType == "policy_blocked" &&
			json.Unmarshal(failure["code"], &code) == nil && code == "refusal_guard" &&
			json.Unmarshal(details["blocked_by"], &source) == nil && source == "upstream" &&
			string(details["retryable"]) == "false"
	default:
		return false
	}
}

func (g *anthropicRefusalSession) observe(payload []byte) bool {
	if g == nil || !isOfficialAnthropicSessionRefusal(payload) {
		return false
	}
	if !g.marked {
		g.marked = true
		pendingAnthropicRefusalMarks.Set(g.key, true, pendingAnthropicRefusalTTL)
		journalErr := recordAnthropicRefusalMarker(g.journalDir, g.key)
		writeCtx, cancel := context.WithTimeout(context.WithoutCancel(g.ctx), time.Second)
		defer cancel()
		redisErr := g.store.SetCyberSessionBlocked(writeCtx, CyberSessionBlock{Keys: []string{g.key}}, 0)
		if journalErr != nil || redisErr != nil {
			logger.LegacyPrintf("service.gateway", "Anthropic refusal storage degraded (journal_ack=%t, redis_ack=%t)", journalErr == nil, redisErr == nil)
		}
		if journalErr == nil || redisErr == nil {
			pendingAnthropicRefusalMarks.Delete(g.key)
		}
	}
	return true
}
