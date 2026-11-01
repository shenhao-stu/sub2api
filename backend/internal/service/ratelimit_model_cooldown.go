package service

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
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

// canonicalModel has already been mapped once at the dispatch boundary.
func (s *RateLimitService) persistUpstreamModelCooldown(ctx context.Context, account *Account, status int, body []byte, canonicalModel string) bool {
	if status != http.StatusTooManyRequests || account == nil || account.Type != AccountTypeAPIKey ||
		!account.ShouldHandleErrorCode(status) {
		return false
	}
	model := strings.TrimSpace(canonicalModel)
	delay := upstreamModelCooldown(body, model)
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
