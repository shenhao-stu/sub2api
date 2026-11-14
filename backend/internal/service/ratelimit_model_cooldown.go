package service

import (
	"context"
	"encoding/json"
	"log/slog"
	"math"
	"net/http"
	"regexp"
	"strings"
	"time"
)

type accountModelRateLimitExtender interface {
	SetModelRateLimitIfLater(context.Context, int64, string, time.Time, ...string) error
}

// A pool can exhaust one model while its other models remain usable.
func upstreamModelCooldown(body []byte, model string) time.Duration {
	var response struct {
		Error struct {
			Code    string `json:"code"`
			Model   string `json:"model"`
			Seconds int64  `json:"reset_seconds"`
		} `json:"error"`
	}
	if model == "" || json.Unmarshal(body, &response) != nil ||
		response.Error.Code != "model_cooldown" || response.Error.Model != model ||
		response.Error.Seconds <= 0 || response.Error.Seconds > 7*24*60*60 {
		return 0
	}
	return time.Duration(response.Error.Seconds) * time.Second
}

// CPA can pass through Google's bare model-exhaustion response when the
// last attempted credential fails before CPA can report a pool cooldown.
func isCPAUltraResourceExhausted(account *Account, model string, body []byte) bool {
	if !strings.EqualFold(UpstreamRequestIDHeaderName(account), "X-CPA-Request-ID") ||
		!strings.HasPrefix(model, "ultra/gemini-") {
		return false
	}
	var response struct {
		Error struct {
			Code    int               `json:"code"`
			Status  string            `json:"status"`
			Message string            `json:"message"`
			Details []json.RawMessage `json:"details"`
		} `json:"error"`
	}
	return json.Unmarshal(body, &response) == nil &&
		response.Error.Code == http.StatusTooManyRequests &&
		response.Error.Status == "RESOURCE_EXHAUSTED" &&
		response.Error.Message == "Resource has been exhausted (e.g. check quota)." &&
		len(response.Error.Details) == 0
}

var cpaRequestID = regexp.MustCompile(`^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$`)

const AnthropicPoolModelCooldownReason GatewayFailureReason = "anthropic_pool_model_cooldown"

func cpaAnthropicModelCooldown(account *Account, model string, headers http.Header, body []byte, now time.Time) time.Duration {
	if account == nil || account.Platform != PlatformAnthropic || !account.IsPoolMode() ||
		!strings.EqualFold(UpstreamRequestIDHeaderName(account), "X-CPA-Request-ID") ||
		!cpaRequestID.MatchString(UpstreamRequestIDFromHeaders(account, headers)) || model == "" {
		return 0
	}
	var envelope struct {
		Type  string `json:"type"`
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	prefix := "All credentials for model " + model + " are cooling down"
	if json.Unmarshal(body, &envelope) != nil || envelope.Type != "error" ||
		envelope.Error.Type != "rate_limit_error" ||
		!(envelope.Error.Message == prefix || strings.HasPrefix(envelope.Error.Message, prefix+" ")) {
		return 0
	}
	delay, ok := parseRetryAfterDelay(now, headers)
	if !ok || delay <= 0 {
		return 0
	}
	return delay
}

func (e *UpstreamFailoverError) AnthropicPoolRetryAfter() int {
	if e == nil || e.StatusCode != http.StatusTooManyRequests ||
		(e.Reason != AnthropicPoolModelCooldownReason && e.Reason != AnthropicPoolWindowCooldownReason) {
		return 0
	}
	delay, ok := parseRetryAfterDelay(time.Now(), e.ResponseHeaders)
	if !ok || delay <= 0 {
		return 0
	}
	return int(math.Ceil(delay.Seconds()))
}

// canonicalModel has already been mapped once at the dispatch boundary.
func (s *RateLimitService) persistUpstreamModelCooldown(ctx context.Context, account *Account, status int, headers http.Header, body []byte, canonicalModel string) bool {
	if status != http.StatusTooManyRequests || account == nil || account.Type != AccountTypeAPIKey ||
		!account.ShouldHandleErrorCode(status) {
		return false
	}
	model := strings.TrimSpace(canonicalModel)
	delay := upstreamModelCooldown(body, model)
	if delay == 0 {
		delay = cpaAnthropicModelCooldown(account, model, headers, body, time.Now())
	}
	if delay == 0 && isCPAUltraResourceExhausted(account, model, body) {
		delay, _ = s.get429FallbackCooldown(ctx, account)
		if retryAfter, ok := parseRetryAfterDelay(time.Now(), headers); ok && retryAfter > delay {
			delay = retryAfter
		}
	}
	if delay == 0 {
		return false
	}
	repo, ok := s.accountRepo.(accountModelRateLimitExtender)
	if !ok {
		slog.Warn("upstream_model_cooldown_unavailable", "account_id", account.ID, "model", model)
		return true
	}
	stateCtx, cancel := openAIAccountStateContext(ctx)
	defer cancel()
	until := time.Now().Add(delay)
	if err := repo.SetModelRateLimitIfLater(stateCtx, account.ID, model, until, "upstream_model_cooldown"); err != nil {
		slog.Warn("upstream_model_cooldown_failed", "account_id", account.ID, "model", model, "error", err)
		return true
	}
	slog.Info("upstream_model_cooldown", "account_id", account.ID, "model", model, "reset_at", until.UTC())
	return true
}
