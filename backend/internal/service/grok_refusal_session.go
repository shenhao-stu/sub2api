package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/pkg/xai"
	"github.com/gin-gonic/gin"
	gocache "github.com/patrickmn/go-cache"
	"github.com/tidwall/gjson"
)

// ErrGrokWSContentRefusal ends a turn; only a separately observed meter can settle it.
var errGrokRequestScoped = errors.New("Grok request-scoped decision")
var ErrGrokWSContentRefusal = fmt.Errorf("%w: upstream content policy refusal", errGrokRequestScoped)

func grokRefusalForwardError(c *gin.Context, err error) error {
	if guard := grokRefusalGuardFromContext(c); guard != nil && guard.marked {
		return errors.Join(errGrokRequestScoped, err)
	}
	return err
}

const grokRefusalSessionContextKey = "grok_refusal_session"
const grokRefusalSessionTTL = 24 * time.Hour
const grokRefusalBlockedMessage = "This Grok conversation was rejected by the upstream content policy. Start a new conversation without the rejected history."

// Redis is authoritative. Retain only positive marks from failed writes so
// this process still protects the session when storage becomes readable again.
var interruptedGrokRefusalWrites = gocache.New(grokRefusalSessionTTL, time.Hour)

type grokRefusalSession struct {
	store                CyberSessionBlockStore
	scope                string
	explicit             string
	previous             string
	responseID           string
	refusalDelta         string
	refusalDeltaOverflow bool
	body                 []byte
	accountID            int64
	marked               bool
}

func grokRefusalGuardFromContext(c *gin.Context) *grokRefusalSession {
	if c == nil {
		return nil
	}
	value, _ := c.Get(grokRefusalSessionContextKey)
	guard, _ := value.(*grokRefusalSession)
	return guard
}

// Only a real conversation signal can stand in for history. Prompt-cache,
// device, IP, model and account identifiers are all shared across sessions.
func (s *OpenAIGatewayService) CheckGrokRefusalSession(ctx context.Context, c *gin.Context, account *Account, body []byte) error {
	if c == nil || s.cfg == nil || !s.cfg.Gateway.Grok.RefusalSessionIsolationEnabled {
		return nil
	}
	if prior := grokRefusalGuardFromContext(c); prior != nil {
		if account != nil && account.ID == prior.accountID {
			return nil
		}
		c.Set(grokRefusalSessionContextKey, (*grokRefusalSession)(nil))
		if o := upstreamResponseModelObserverFromContext(c); o != nil {
			o.grokRefusal = nil
		}
	}
	guard, fields := s.grokRefusalAdmission(ctx, c, account, body, "")
	if fields != nil {
		if fields.StatusCode == http.StatusForbidden {
			MarkOpsClientBusinessLimited(c, OpsClientBusinessLimitedReasonLocalPolicyDenied)
		}
		writeOpenAIErrorFields(c, *fields, false)
		return fmt.Errorf("%w: %s", errGrokRequestScoped, fields.Type)
	}
	if guard != nil {
		c.Set(grokRefusalSessionContextKey, guard)
		if o := upstreamResponseModelObserverFromContext(c); o != nil {
			o.grokRefusal = guard
		}
	}
	return nil
}

func (s *OpenAIGatewayService) grokRefusalAdmission(ctx context.Context, c *gin.Context, account *Account, body []byte, originalParent string) (*grokRefusalSession, *openAIClientErrorFields) {
	if c == nil || s.cfg == nil || !s.cfg.Gateway.Grok.RefusalSessionIsolationEnabled {
		return nil, nil
	}
	if account == nil || !account.IsGrok() || !xai.IsOfficialBaseURL(account.GetGrokBaseURL()) {
		return nil, nil
	}
	key, _ := c.Get("api_key")
	apiKey, _ := key.(*APIKey)
	store := s.cyberSessionBlockStore()
	if apiKey == nil || store == nil {
		return nil, nil
	}
	principal := openAIRelayPrincipal(ctx, apiKey.UserID, apiKey.ID)
	if principal == "" {
		return nil, nil
	}
	if slices.Contains(s.cfg.Gateway.Grok.RefusalSessionRelayKeyIDs, apiKey.ID) {
		// These key IDs identify authenticated internal gateways that overwrite this header.
		tag, verified := AffinityTagFromUserID("getoken:", c.GetHeader(AccountAffinityHeader))
		if !verified {
			return nil, nil
		} // A shared relay identity alone never owns a session.
		principal = fmt.Sprintf("%s:%x", principal, sha256.Sum256([]byte(tag)))
	}
	scope := "grok-refusal:v1:" + principal
	session := firstNonEmpty(extractClaudeCodeSessionID(c, body), explicitOpenAIHeaderSessionID(c), c.GetHeader(grokConversationIDHeader))
	guard := &grokRefusalSession{store: store, scope: scope, body: body, accountID: account.ID}
	previous := grokPreviousResponseSessionSeed(body)
	if previous == "" && ClassifyOpenAIPreviousResponseIDKind(originalParent) == OpenAIPreviousResponseIDKindResponseID {
		previous = "grok-prev-resp:" + originalParent
	}
	guard.previous = guard.sessionKey(previous)
	if session != "" {
		guard.explicit = guard.sessionKey(session)
	}
	readCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	blocked, err := guard.blocked(readCtx)
	if err != nil {
		logger.LegacyPrintf("service.grok", "refusal session lookup failed: %v", err)
		return nil, &openAIClientErrorFields{Type: "grok_session_protection_unavailable", Code: "conversation_route_unavailable", Message: "Grok session protection is temporarily unavailable.", StatusCode: http.StatusServiceUnavailable}
	}
	if blocked {
		return nil, &openAIClientErrorFields{Type: "grok_session_isolated", Code: "content_policy_violation", Message: grokRefusalBlockedMessage, StatusCode: http.StatusForbidden}
	}
	return guard, nil
}

func (g *grokRefusalSession) sessionKey(seed string) string {
	if seed == "" {
		return ""
	}
	return fmt.Sprintf("%s:s:%x", g.scope, sha256.Sum256([]byte(seed)))
}

func canonicalGrokHistoryItem(raw string) string {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	var v any
	if err := decoder.Decode(&v); err != nil {
		return raw
	}
	b, err := json.Marshal(v)
	if err != nil {
		return raw
	}
	return string(b)
}

// Walk exact cumulative digests only after a user turn. A shared system prefix cannot block another branch.
func (g *grokRefusalSession) history(visit func(string) bool) {
	root := gjson.ParseBytes(g.body)
	sequence := root.Get("messages")
	if !sequence.IsArray() {
		sequence = root.Get("input")
		if sequence.Type == gjson.String && sequence.Str != "" {
			b, _ := json.Marshal([]map[string]string{{"role": "user", "content": sequence.Str}})
			sequence = gjson.ParseBytes(b)
		}
	}
	if !sequence.IsArray() {
		return
	}
	h := sha256.New()
	fmt.Fprint(h, g.scope)
	for _, field := range []string{"system", "instructions"} {
		if v := root.Get(field); v.Exists() {
			fmt.Fprintf(h, "|%s=%s", field, canonicalGrokHistoryItem(v.Raw))
		}
	}
	seenUser := false
	sequence.ForEach(func(_, item gjson.Result) bool {
		encoded := canonicalGrokHistoryItem(item.Raw)
		fmt.Fprintf(h, "|%d:%s", len(encoded), encoded)
		if item.Get("role").Str == "user" || item.Type == gjson.String || item.Get("type").Str == "input_text" {
			seenUser = true
		}
		return !seenUser || visit(g.scope+":h:"+hex.EncodeToString(h.Sum(nil)))
	})
}

func (g *grokRefusalSession) blocked(ctx context.Context) (bool, error) {
	var identities []string
	for _, k := range []string{g.explicit, g.previous} {
		if k != "" {
			if _, ok := interruptedGrokRefusalWrites.Get(k); ok {
				return true, nil
			}
			identities = append(identities, k)
		}
	}
	if len(identities) != 0 {
		hit, err := g.store.FindCyberSessionBlocked(ctx, identities)
		if err != nil || hit != "" {
			return hit != "", err
		}
	}
	active, err := g.store.IsCyberSessionScopeActive(ctx, g.scope)
	_, local := interruptedGrokRefusalWrites.Get(g.scope)
	if err != nil || (!active && !local) {
		return false, err
	}
	keys := make([]string, 0, 128)
	var hit string
	flush := func() {
		if len(keys) > 0 {
			hit, err = g.store.FindCyberSessionBlocked(ctx, keys)
			keys = keys[:0]
		}
	}
	g.history(func(key string) bool {
		if local {
			if _, ok := interruptedGrokRefusalWrites.Get(key); ok {
				hit = key
				return false
			}
		}
		keys = append(keys, key)
		if len(keys) == cap(keys) {
			flush()
		}
		return hit == "" && err == nil
	})
	if hit == "" && err == nil {
		flush()
	}
	return hit != "", err
}

func (g *grokRefusalSession) mark() {
	if g.marked {
		return
	}
	var keys []string
	for _, k := range []string{g.explicit, g.previous, g.responseID} {
		if k != "" {
			keys = append(keys, k)
		}
	}
	var fullHistory string
	g.history(func(k string) bool { fullHistory = k; return true })
	if fullHistory != "" {
		keys = append(keys, fullHistory)
	}
	if len(keys) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := g.store.SetCyberSessionBlocked(ctx, g.scope, keys, grokRefusalSessionTTL); err != nil {
		for _, k := range keys {
			interruptedGrokRefusalWrites.SetDefault(k, true)
		}
		interruptedGrokRefusalWrites.SetDefault(g.scope, true)
		logger.LegacyPrintf("service.grok", "refusal session retained locally after Redis write failed: %v", err)
	}
	g.marked = true
}

// Read only provider error/refusal fields, never ordinary assistant output,
// tool results, prompt echoes or citations.
func (g *grokRefusalSession) observeEvent(body []byte, event string) {
	if g.marked {
		return
	}
	var response gjson.Result
	switch event {
	case "response.created", "response.completed", "response.failed":
		response = gjson.GetBytes(body, "response")
	case "":
		if gjson.GetBytes(body, "object").Str == "response" {
			response = gjson.ParseBytes(body)
			event = "response." + response.Get("status").Str
		}
	}
	if id := response.Get("id").Str; ClassifyOpenAIPreviousResponseIDKind(id) == OpenAIPreviousResponseIDKindResponseID {
		g.responseID = g.sessionKey("grok-prev-resp:" + id)
	}
	switch event {
	case "response.refusal.done":
		if isGrokFixedContentRefusalMessage(gjson.GetBytes(body, "refusal").Str) {
			g.mark()
		}
	case "response.failed", "error":
		e := response.Get("error")
		if !e.Exists() {
			e = gjson.GetBytes(body, "error")
		}
		if e.IsObject() {
			if isGrokContentPolicyRejection(403, []byte(`{"error":`+e.Raw+`}`)) {
				g.mark()
			}
		} else if isGrokContentPolicyRejection(403, body) {
			g.mark()
		}
	case "", "chat.completion", "chat.completion.chunk":
		first := gjson.GetBytes(body, "choices.0")
		if first.Get("index").Int() != 0 {
			return
		}
		if delta := first.Get("delta.refusal").Str; delta != "" && !g.refusalDeltaOverflow {
			if len(delta) <= 64-len(g.refusalDelta) {
				g.refusalDelta += delta
			} else {
				g.refusalDeltaOverflow = true
			}
		}
		finish := first.Get("finish_reason").Str
		if finish == "content_filter" || isGrokFixedContentRefusalMessage(first.Get("message.refusal").Str) ||
			(finish == "stop" && !g.refusalDeltaOverflow && isGrokFixedContentRefusalMessage(g.refusalDelta)) {
			g.mark()
		}
	case "response.completed":
		response.Get("output").ForEach(func(_, item gjson.Result) bool {
			item.Get("content").ForEach(func(_, part gjson.Result) bool {
				if part.Get("type").Str == "refusal" && isGrokFixedContentRefusalMessage(part.Get("refusal").Str) {
					g.mark()
				}
				return !g.marked
			})
			return !g.marked
		})
	}
}
