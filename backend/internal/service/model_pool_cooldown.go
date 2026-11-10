package service

import (
	"context"
	"time"
)

// Diagnose the persistent pool only after selection failed. Never infer a
// cooldown for an empty pool, a healthy alternative, or an unknown failure.
func modelPoolRetryAfter(ctx context.Context, accounts []*Account, model string, now time.Time) int {
	if len(accounts) == 0 {
		return 0
	}
	var earliest time.Time
	for _, account := range accounts {
		if account == nil {
			return 0
		}
		reset := now
		if account.RateLimitResetAt != nil && account.RateLimitResetAt.After(reset) {
			reset = *account.RateLimitResetAt
		}
		for _, key := range account.modelRateLimitKeysForRequest(ctx, model) {
			if at := account.modelRateLimitResetAt(key); at != nil && at.After(reset) {
				reset = *at
			}
		}
		if !reset.After(now) {
			return 0
		}
		if earliest.IsZero() || reset.Before(earliest) {
			earliest = reset
		}
	}
	remaining := earliest.Sub(now)
	return int((remaining + time.Second - 1) / time.Second)
}
