package service

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var vertexRetryDuration = regexp.MustCompile(`^[0-9]+(?:\.[0-9]{1,9})?s$`)

// The caller supplies the actual dispatched model, already mapped exactly once.
// This is request/model capacity backoff, never an assertion of depleted quota.
func vertexModelCooldown(account *Account, status int, headers http.Header, body []byte, model string, now time.Time) (time.Duration, string) {
	if status != http.StatusTooManyRequests || account == nil || account.Platform != PlatformGemini ||
		!account.IsVertexServiceAccount() || strings.TrimSpace(model) == "" || !account.ShouldHandleErrorCode(status) ||
		(account.IsPoolMode() && !account.IsCustomErrorCodesEnabled()) {
		return 0, ""
	}
	raw := json.RawMessage(body)
	if strings.HasPrefix(strings.TrimSpace(string(body)), "[") {
		var items []json.RawMessage
		if json.Unmarshal(body, &items) != nil || len(items) != 1 {
			return 0, ""
		}
		raw = items[0]
	}
	var envelope map[string]json.RawMessage
	if json.Unmarshal(raw, &envelope) != nil || len(envelope) != 1 {
		return 0, ""
	}
	var failure struct {
		Code    int    `json:"code"`
		Status  string `json:"status"`
		Message string `json:"message"`
		Details []struct {
			Type       string `json:"@type"`
			RetryDelay string `json:"retryDelay"`
		} `json:"details"`
	}
	if json.Unmarshal(envelope["error"], &failure) != nil || failure.Code != 429 || failure.Status != "RESOURCE_EXHAUSTED" {
		return 0, ""
	}
	message := strings.ToLower(failure.Message)
	if !strings.Contains(message, "resource exhausted") || !strings.Contains(message, "try again later") ||
		strings.Contains(message, "quota") || strings.Contains(message, "provisioned throughput") {
		return 0, ""
	}
	delay := time.Duration(0)
	for _, detail := range failure.Details {
		// QuotaFailure / ErrorInfo may describe project-wide scope: keep the existing policy.
		if detail.Type != geminiRetryInfoTypeURL {
			return 0, ""
		}
		if vertexRetryDuration.MatchString(detail.RetryDelay) {
			if value, err := time.ParseDuration(detail.RetryDelay); err == nil && value > delay {
				delay = value
			}
		}
	}
	if value := vertexRetryAfter(headers.Get("Retry-After"), now); value > delay {
		delay = value
	}
	if delay > 0 {
		return min(delay, geminiRetryInfoMaxDelay), "vertex_model_retry_hint"
	}
	return geminiVertexFallbackCooldown, "vertex_shared_capacity_backoff"
}

func vertexRetryAfter(raw string, now time.Time) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	if seconds, err := strconv.ParseUint(raw, 10, 32); err == nil {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(raw); err == nil && at.After(now) {
		return at.Sub(now)
	}
	return 0
}

func (s *GeminiMessagesCompatService) persistVertexModelCooldown(ctx context.Context, account *Account, status int, headers http.Header, body []byte, model string) bool {
	now := time.Now()
	delay, reason := vertexModelCooldown(account, status, headers, body, model, now)
	if delay == 0 {
		return false
	}
	repo, ok := s.accountRepo.(accountModelRateLimitExtender)
	if !ok {
		slog.Warn("vertex_model_cooldown_unavailable", "account_id", account.ID, "model", model)
		return true
	}
	stateCtx, cancel := openAIAccountStateContext(ctx)
	defer cancel()
	// Ceil to whole seconds: storage uses RFC3339, and a fractional hint must not be shortened.
	until := now.Add(delay).Truncate(time.Second).Add(time.Second)
	if err := repo.SetModelRateLimitIfLater(stateCtx, account.ID, strings.TrimSpace(model), until, reason); err != nil {
		slog.Warn("vertex_model_cooldown_failed", "account_id", account.ID, "model", model, "error", err)
		return true
	}
	slog.Info("vertex_model_cooldown", "account_id", account.ID, "model", model, "reason", reason, "reset_at", until.UTC())
	return true
}
