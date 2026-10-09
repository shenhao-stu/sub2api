package service

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"time"
)

const (
	alphaSearchUnavailableUntil  = "alpha_search_unavailable_until"
	alphaSearchUnavailableOrigin = "alpha_search_unavailable_origin"
	alphaSearchRecheckInterval   = time.Hour
)

func alphaSearchOrigin(account *Account) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(account.GetOpenAIBaseURL())))
}

func (a *Account) alphaSearchUnavailable(now time.Time) bool {
	if a == nil || a.Type != AccountTypeAPIKey || a.GetExtraString(alphaSearchUnavailableOrigin) != alphaSearchOrigin(a) {
		return false
	}
	until, err := time.Parse(time.RFC3339, a.GetExtraString(alphaSearchUnavailableUntil))
	return err == nil && now.Before(until)
}

func (s *OpenAIGatewayService) rememberUnsupportedAlphaSearch(ctx context.Context, account *Account) {
	if s.accountRepo == nil || account.IsShadow() {
		return
	}
	updates := map[string]any{
		alphaSearchUnavailableUntil:  time.Now().Add(alphaSearchRecheckInterval).UTC().Format(time.RFC3339),
		alphaSearchUnavailableOrigin: alphaSearchOrigin(account),
	}
	if err := s.accountRepo.UpdateExtra(ctx, account.ID, updates); err != nil {
		slog.Warn("alpha_search_capability_update_failed", "account_id", account.ID, "error", err)
	}
}
