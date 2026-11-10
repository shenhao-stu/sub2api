package service

import "time"

// Diagnose the persistent pool only after selection failed. Never infer a
// cooldown for an empty pool, a healthy alternative, or an unknown failure.
func modelPoolRetryAfter(accounts []*Account, now time.Time) int {
	if len(accounts) == 0 {
		return 0
	}
	var earliest time.Time
	for _, account := range accounts {
		if account == nil || account.RateLimitResetAt == nil || !account.RateLimitResetAt.After(now) {
			return 0
		}
		if earliest.IsZero() || account.RateLimitResetAt.Before(earliest) {
			earliest = *account.RateLimitResetAt
		}
	}
	remaining := earliest.Sub(now)
	return int((remaining + time.Second - 1) / time.Second)
}
