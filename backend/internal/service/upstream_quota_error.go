package service

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	upstreamQuotaCooldown         = 10 * time.Minute
	upstreamQuotaRetryAfterMax    = 7 * 24 * time.Hour
	UpstreamQuotaExhaustedMessage = "Upstream account quota is exhausted, please contact administrator"
)

// IsUpstreamQuotaExhausted classifies a provider response, never a customer balance check.
func IsUpstreamQuotaExhausted(statusCode int, body []byte) bool {
	if statusCode != http.StatusTooManyRequests {
		return false
	}
	var payload struct {
		Error *struct {
			Code string `json:"code"`
			Type string `json:"type"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &payload) != nil || payload.Error == nil {
		return false
	}
	return strings.TrimSpace(payload.Error.Code) == "insufficient_quota" ||
		strings.TrimSpace(payload.Error.Type) == "insufficient_quota"
}

func upstreamQuotaResetAt(now time.Time, headers http.Header, fallback time.Duration, enabled bool) time.Time {
	var delay time.Duration
	raw := strings.TrimSpace(headers.Get("Retry-After"))
	if seconds, err := strconv.ParseUint(raw, 10, 64); err == nil {
		if seconds <= uint64(upstreamQuotaRetryAfterMax/time.Second) {
			delay = time.Duration(seconds) * time.Second
		}
	} else if at, err := http.ParseTime(raw); err == nil {
		if remaining := at.Sub(now); remaining <= upstreamQuotaRetryAfterMax {
			delay = remaining
		}
	}
	if enabled {
		if fallback > delay {
			delay = fallback
		}
		if upstreamQuotaCooldown > delay {
			delay = upstreamQuotaCooldown
		}
	}
	if delay <= 0 {
		return time.Time{}
	}
	return now.Add(delay)
}

type accountRateLimitExtender interface {
	SetRateLimitedIfLater(ctx context.Context, id int64, resetAt time.Time) error
}

func (s *RateLimitService) handleUpstreamQuotaExhausted(ctx context.Context, account *Account, headers http.Header, body []byte) bool {
	if !IsUpstreamQuotaExhausted(http.StatusTooManyRequests, body) {
		return false
	}
	stateCtx, cancel := openAIAccountStateContext(ctx)
	defer cancel()
	cooldown, enabled := s.get429FallbackCooldown(stateCtx, account)
	resetAt := upstreamQuotaResetAt(time.Now(), headers, cooldown, enabled)
	if resetAt.IsZero() {
		return true
	}
	// An unconditional write could shorten a newer response's real reset window.
	repo, ok := s.accountRepo.(accountRateLimitExtender)
	if !ok {
		slog.Warn("upstream_quota_cooldown_unavailable", "account_id", account.ID)
		return true
	}
	if err := repo.SetRateLimitedIfLater(stateCtx, account.ID, resetAt); err != nil {
		slog.Warn("upstream_quota_cooldown_failed", "account_id", account.ID, "error", err)
		return true
	}
	authoritative, err := s.accountRepo.GetByID(stateCtx, account.ID)
	if err != nil || authoritative == nil || authoritative.RateLimitResetAt == nil {
		slog.Warn("upstream_quota_cooldown_reload_failed", "account_id", account.ID, "error", err)
		return true
	}
	s.notifyAccountSchedulingBlocked(authoritative, *authoritative.RateLimitResetAt, "upstream_insufficient_quota")
	slog.Warn("upstream_quota_exhausted", "account_id", account.ID, "platform", account.Platform,
		"reset_at", authoritative.RateLimitResetAt.UTC())
	return true
}
