package service

import (
	"context"
	"math"
	"strings"
	"time"
)

// Spending-limit is recoverable at the end of the observed billing period.
// When no billing snapshot is available, use a short probe rather than
// fabricating a 24h boundary from the error arrival time.
const grokSpendingLimitProbeCooldown = 10 * time.Minute

func grokSpendingLimitResetAt(account *Account, now time.Time) time.Time {
	probeAt := now.Add(grokSpendingLimitProbeCooldown)
	if account == nil {
		return probeAt
	}
	billing, err := grokBillingSnapshotFromExtra(account.Extra)
	if err != nil || billing == nil {
		return probeAt
	}
	var resetAt time.Time
	if billing.PeriodType == "weekly" {
		resetAt = grokExhaustedBillingWindowReset(billing.UsagePercent, billing.PeriodStart, billing.PeriodEnd,
			billing.WeeklyUpdatedAt, billing.WeeklyStatusCode, now)
	}
	monthlyReset := grokExhaustedBillingWindowReset(billing.UsedPercent, billing.BillingPeriodStart, billing.BillingPeriodEnd,
		billing.MonthlyUpdatedAt, billing.MonthlyStatusCode, now)
	if monthlyReset.After(resetAt) {
		resetAt = monthlyReset
	}
	if resetAt.IsZero() {
		return probeAt
	}
	return resetAt
}

// A calendar boundary alone does not identify which quota rejected the request.
func grokExhaustedBillingWindowReset(percent *float64, start, end, observed string, status int, now time.Time) time.Time {
	if percent == nil || math.IsNaN(*percent) || math.IsInf(*percent, 0) || *percent < 100 || status < 200 || status >= 300 {
		return time.Time{}
	}
	windowStart, startErr := time.Parse(time.RFC3339, strings.TrimSpace(start))
	windowEnd, endErr := time.Parse(time.RFC3339, strings.TrimSpace(end))
	observedAt, observedErr := time.Parse(time.RFC3339, strings.TrimSpace(observed))
	if startErr != nil || endErr != nil || observedErr != nil || !windowEnd.After(now) ||
		windowStart.After(observedAt) || observedAt.After(now) {
		return time.Time{}
	}
	return windowEnd
}

// clearGrokNeedsReauthExtra drops the soft reauth flag after successful refresh
// or reauth. Best-effort; never fails the request path.
func clearGrokNeedsReauthExtra(ctx context.Context, repo AccountRepository, accountID int64) {
	if repo == nil || accountID <= 0 {
		return
	}
	stateCtx, cancel := openAIAccountStateContext(ctx)
	defer cancel()
	_ = repo.UpdateExtra(stateCtx, accountID, map[string]any{
		"grok_needs_reauth":        false,
		"grok_needs_reauth_reason": "",
		"grok_needs_reauth_at":     "",
	})
}

func accountGrokNeedsReauth(account *Account) bool {
	if account == nil {
		return false
	}
	if account.Status == StatusError {
		msg := strings.ToLower(account.ErrorMessage)
		if strings.Contains(msg, "spending limit") || strings.Contains(msg, "reauthorize") {
			return true
		}
	}
	if v, ok := account.Extra["grok_needs_reauth"].(bool); ok && v {
		return true
	}
	if s, ok := account.Extra["grok_needs_reauth"].(string); ok {
		return strings.EqualFold(s, "true") || s == "1"
	}
	return false
}
