package service

import (
	"context"
	"strings"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/xai"
)

const (
	GrokQuotaWaitUsageLimitReached = "usage_limit_reached"
	grokQuotaWaitMaximum           = 32 * 24 * time.Hour
)

func isGrokUsageLimitReachedText(message string) bool {
	low := strings.ToLower(strings.TrimSpace(message))
	if low == "usage_limit_reached" || strings.HasPrefix(low, "usage_limit_reached ") {
		return true
	}
	if strings.Contains(low, "not reached") || strings.Contains(low, "not exceeded") || strings.Contains(low, "not exhausted") {
		return false
	}
	return strings.Contains(low, "usage limit") && (strings.Contains(low, "reached") || strings.Contains(low, "exceeded") || strings.Contains(low, "exhausted"))
}

// GrokQuotaCooldownResult deliberately separates an observed upstream reset
// from a local retry-probe boundary. Neither operation enables an admin hold.
type GrokQuotaCooldownResult struct {
	AccountID            int64      `json:"account_id"`
	Source               string     `json:"source"`
	RetryAt              time.Time  `json:"retry_at"`
	OfficialResetAt      *time.Time `json:"official_reset_at"`
	ManualSchedulable    bool       `json:"manual_schedulable"`
	EffectiveSchedulable bool       `json:"effective_schedulable"`
}

type grokQuotaWaitDecision struct {
	Source          string
	RetryAt         time.Time
	OfficialResetAt *time.Time
}

func grokQuotaWaitPlan(account *Account, now time.Time) grokQuotaWaitDecision {
	plan := grokQuotaWaitDecision{Source: "retry_probe", RetryAt: now.Add(grokSpendingLimitProbeCooldown)}
	if account == nil {
		return plan
	}
	billing, err := grokBillingSnapshotFromExtra(account.Extra)
	if err != nil || billing == nil {
		return plan
	}
	var reset time.Time
	if billing.PeriodType == "weekly" {
		reset = grokExhaustedBillingWindowReset(grokTextBillingPercent(billing), billing.PeriodStart, billing.PeriodEnd, billing.WeeklyUpdatedAt, billing.WeeklyStatusCode, now)
	}
	if reset.After(now.Add(grokQuotaWaitMaximum)) {
		reset = time.Time{}
	}
	monthly := grokExhaustedBillingWindowReset(billing.UsedPercent, billing.BillingPeriodStart, billing.BillingPeriodEnd, billing.MonthlyUpdatedAt, billing.MonthlyStatusCode, now)
	if monthly.After(now.Add(grokQuotaWaitMaximum)) {
		monthly = time.Time{}
	}
	if monthly.After(reset) {
		reset = monthly
	}
	if reset.After(now) && !reset.After(now.Add(grokQuotaWaitMaximum)) {
		plan.Source = "official_billing_reset"
		plan.RetryAt = reset
		plan.OfficialResetAt = &reset
	}
	return plan
}

// Product-bearing billing is scoped: an exhausted Imagine/media window says
// nothing about text. Preserve legacy aggregate-only observations, while never
// using an unrelated or unknown explicit product as text quota evidence.
func grokTextBillingPercent(billing *xai.BillingSummary) *float64 {
	if billing == nil {
		return nil
	}
	if len(billing.ProductUsage) == 0 {
		return billing.UsagePercent
	}
	var percent *float64
	for _, p := range billing.ProductUsage {
		switch strings.ToLower(strings.TrimSpace(p.Product)) {
		case "grokbuild", "grok", "grokchat", "groktext", "grokresponses":
			if p.UsagePercent != nil && (percent == nil || *p.UsagePercent > *percent) {
				percent = p.UsagePercent
			}
		}
	}
	return percent
}

// SetQuotaCooldown arms only the existing native temporal scheduling gate.
// A caller-supplied usage_limit_reached is an explicit administrator observation,
// not a claimed reset timestamp. No HTTP/model call or account enabling occurs.
func (s *GrokQuotaService) SetQuotaCooldown(ctx context.Context, accountID int64, observedCode string) (*GrokQuotaCooldownResult, error) {
	if accountID <= 0 || s == nil || s.accountRepo == nil {
		return nil, infraerrors.BadRequest("GROK_QUOTA_COOLDOWN_INVALID_ACCOUNT", "Grok OAuth account is required")
	}
	if observedCode != "" && observedCode != GrokQuotaWaitUsageLimitReached {
		return nil, infraerrors.BadRequest("GROK_QUOTA_COOLDOWN_INVALID_EVIDENCE", "Unsupported quota observation code")
	}
	a, err := s.accountRepo.GetByID(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if a == nil || !a.IsGrokOAuth() || !a.IsActive() {
		return nil, infraerrors.BadRequest("GROK_QUOTA_COOLDOWN_INACTIVE_ACCOUNT", "An active Grok OAuth account is required; administrator holds are not changed")
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	plan := grokQuotaWaitPlan(a, now)
	if plan.OfficialResetAt == nil && observedCode == "" {
		return nil, infraerrors.BadRequest("GROK_QUOTA_COOLDOWN_EVIDENCE_REQUIRED", "An exhausted text billing window or observed usage_limit_reached is required")
	}
	if !plan.RetryAt.After(now) || plan.RetryAt.After(now.Add(grokQuotaWaitMaximum)) {
		return nil, infraerrors.BadRequest("GROK_QUOTA_COOLDOWN_INVALID_BOUNDARY", "Quota cooldown boundary is outside its allowed range")
	}
	// PostgreSQL timestamptz stores microseconds. Ceil finer upstream boundaries
	// instead of admitting early or mistaking a precision roundtrip for a race.
	if rounded := plan.RetryAt.Truncate(time.Microsecond); rounded.Before(plan.RetryAt) {
		plan.RetryAt = rounded.Add(time.Microsecond)
	}
	extending, ok := s.accountRepo.(grokRateLimitExtendingRepository)
	if !ok {
		return nil, infraerrors.New(503, "GROK_QUOTA_COOLDOWN_ATOMIC_UNAVAILABLE", "Atomic quota cooldown persistence is unavailable")
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if err = extending.SetRateLimitedIfLater(ctx, accountID, plan.RetryAt); err != nil {
		return nil, err
	}
	latest, err := s.accountRepo.GetByID(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if latest == nil || latest.RateLimitResetAt == nil || latest.RateLimitResetAt.Before(plan.RetryAt) {
		return nil, infraerrors.New(409, "GROK_QUOTA_COOLDOWN_STATE_CHANGED", "Quota cooldown changed concurrently; inspect current state")
	}
	actual := *latest.RateLimitResetAt
	if actual.After(plan.RetryAt) {
		plan.Source = "existing_longer_cooldown"
	}
	return &GrokQuotaCooldownResult{AccountID: accountID, Source: plan.Source, RetryAt: actual, OfficialResetAt: plan.OfficialResetAt, ManualSchedulable: latest.Schedulable, EffectiveSchedulable: latest.IsSchedulable()}, nil
}
