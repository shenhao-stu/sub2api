//go:build unit

package service

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/xai"
	"github.com/stretchr/testify/require"
)

func quotaWaitFixture(now time.Time, products []xai.BillingProductSummary) *Account {
	return &Account{ID: 900001, Platform: PlatformGrok, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true,
		Extra: map[string]any{grokBillingExtraKey: &xai.BillingSummary{PeriodType: "weekly", UsagePercent: floatPtr(100),
			PeriodStart: now.Add(-time.Hour).Format(time.RFC3339), PeriodEnd: now.Add(72 * time.Hour).Format(time.RFC3339),
			WeeklyStatusCode: 200, WeeklyUpdatedAt: now.Format(time.RFC3339), ProductUsage: products}}}
}

func TestGrokQuotaWaitUsesTextScopeNotImageReset(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	for _, tt := range []struct {
		name     string
		products []xai.BillingProductSummary
		official bool
	}{
		{"build", []xai.BillingProductSummary{{Product: "GrokBuild", UsagePercent: floatPtr(100)}}, true},
		{"image", []xai.BillingProductSummary{{Product: "GrokImagine", UsagePercent: floatPtr(100)}}, false},
		{"mixed_only_image_exhausted", []xai.BillingProductSummary{{Product: "GrokBuild", UsagePercent: floatPtr(30)}, {Product: "GrokImagine", UsagePercent: floatPtr(100)}}, false},
		{"mixed_build_exhausted", []xai.BillingProductSummary{{Product: "GrokBuild", UsagePercent: floatPtr(100)}, {Product: "GrokImagine", UsagePercent: floatPtr(20)}}, true},
		{"unknown_product", []xai.BillingProductSummary{{Product: "FutureUnknown", UsagePercent: floatPtr(100)}}, false},
		{"legacy_unscoped", nil, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := quotaWaitFixture(now, tt.products)
			p := grokQuotaWaitPlan(a, now)
			require.Equal(t, tt.official, p.OfficialResetAt != nil)
			if tt.official {
				require.Equal(t, now.Add(72*time.Hour), p.RetryAt)
			} else {
				require.Equal(t, now.Add(grokSpendingLimitProbeCooldown), p.RetryAt)
				require.Equal(t, "retry_probe", p.Source)
			}
		})
	}
}

func TestGrokQuotaWaitRejectsInvalidWindowAndBounds(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	for _, mutate := range []func(*xai.BillingSummary){
		func(b *xai.BillingSummary) { b.PeriodEnd = now.Add(-time.Second).Format(time.RFC3339) },
		func(b *xai.BillingSummary) { b.PeriodEnd = now.Add(40 * 24 * time.Hour).Format(time.RFC3339) },
		func(b *xai.BillingSummary) { b.WeeklyUpdatedAt = now.Add(time.Minute).Format(time.RFC3339) },
		func(b *xai.BillingSummary) { b.WeeklyUpdatedAt = "" },
		func(b *xai.BillingSummary) { b.WeeklyStatusCode = 503 },
	} {
		a := quotaWaitFixture(now, []xai.BillingProductSummary{{Product: "GrokBuild", UsagePercent: floatPtr(100)}})
		mutate(a.Extra[grokBillingExtraKey].(*xai.BillingSummary))
		p := grokQuotaWaitPlan(a, now)
		require.Nil(t, p.OfficialResetAt)
		require.Equal(t, now.Add(grokSpendingLimitProbeCooldown), p.RetryAt)
	}
	p := grokQuotaWaitPlan(&Account{}, now)
	require.Nil(t, p.OfficialResetAt)
	require.Equal(t, now.Add(grokSpendingLimitProbeCooldown), p.RetryAt)
	// One invalid longer window must not mask another valid observed boundary.
	a := quotaWaitFixture(now, nil)
	billing := a.Extra[grokBillingExtraKey].(*xai.BillingSummary)
	billing.UsedPercent = floatPtr(100)
	billing.BillingPeriodStart = now.Add(-time.Hour).Format(time.RFC3339)
	billing.BillingPeriodEnd = now.Add(40 * 24 * time.Hour).Format(time.RFC3339)
	billing.MonthlyUpdatedAt = now.Format(time.RFC3339)
	billing.MonthlyStatusCode = 200
	require.Equal(t, now.Add(72*time.Hour), grokQuotaWaitPlan(a, now).RetryAt)
}

type quotaWaitRepo struct {
	AccountRepository
	account         *Account
	setCalls        int
	concurrentUntil *time.Time
	writeErr        error
}

func (r *quotaWaitRepo) GetByID(context.Context, int64) (*Account, error) {
	copy := *r.account
	return &copy, nil
}
func (r *quotaWaitRepo) SetRateLimited(context.Context, int64, time.Time) error {
	panic("must use atomic extend")
}
func (r *quotaWaitRepo) SetRateLimitedIfLater(_ context.Context, _ int64, until time.Time) error {
	r.setCalls++
	if r.writeErr != nil {
		return r.writeErr
	}
	if r.concurrentUntil != nil {
		r.account.RateLimitResetAt = r.concurrentUntil
		r.account.Schedulable = false
	}
	if r.account.RateLimitResetAt == nil || until.After(*r.account.RateLimitResetAt) {
		r.account.RateLimitResetAt = &until
		now := time.Now()
		r.account.RateLimitedAt = &now
	}
	return nil
}

func TestGrokQuotaWaitSetterPreservesManualOffAndLongerConcurrentHold(t *testing.T) {
	a := quotaWaitFixture(time.Now().UTC(), nil)
	a.Schedulable = false
	longer := time.Now().Add(90 * time.Hour)
	r := &quotaWaitRepo{account: a, concurrentUntil: &longer}
	s := &GrokQuotaService{accountRepo: r}
	result, err := s.SetQuotaCooldown(context.Background(), a.ID, GrokQuotaWaitUsageLimitReached)
	require.NoError(t, err)
	require.Equal(t, 1, r.setCalls)
	require.False(t, r.account.Schedulable)
	require.Equal(t, longer, *r.account.RateLimitResetAt)
	require.Equal(t, longer, result.RetryAt)
	require.False(t, result.ManualSchedulable)
	require.False(t, result.EffectiveSchedulable)
	// Expiry only opens the temporal gate. The admin off switch always wins.
	expired := time.Now().Add(-time.Second)
	r.account.RateLimitResetAt = &expired
	require.False(t, r.account.IsSchedulable())
	r.account.Schedulable = true
	require.True(t, r.account.IsSchedulable())
}

func TestGrokQuotaWaitSetterRejectsUnsupportedEvidenceAndNeverEnablesInactive(t *testing.T) {
	r := &quotaWaitRepo{account: &Account{ID: 900001, Platform: PlatformGrok, Type: AccountTypeOAuth, Status: StatusActive}}
	s := &GrokQuotaService{accountRepo: r}
	_, err := s.SetQuotaCooldown(context.Background(), 900001, "arbitrary_future_reset")
	require.Error(t, err)
	require.Zero(t, r.setCalls)
	_, err = s.SetQuotaCooldown(context.Background(), 900001, "")
	require.Error(t, err)
	require.Zero(t, r.setCalls)
	r.account.Status = "inactive"
	_, err = s.SetQuotaCooldown(context.Background(), 900001, GrokQuotaWaitUsageLimitReached)
	require.Error(t, err)
	require.Zero(t, r.setCalls)
	r.account.Status = StatusActive
	r.writeErr = errors.New("synthetic persistence failed")
	_, err = s.SetQuotaCooldown(context.Background(), 900001, GrokQuotaWaitUsageLimitReached)
	require.Error(t, err)
}

func TestGrokQuotaWaitSetterUsesDatabasePrecisionWithoutEarlyAdmission(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	a := quotaWaitFixture(now, nil)
	billing := a.Extra[grokBillingExtraKey].(*xai.BillingSummary)
	official := now.Add(time.Hour + 123*time.Nanosecond)
	billing.PeriodEnd = official.Format(time.RFC3339Nano)
	r := &quotaWaitRepo{account: a}
	s := &GrokQuotaService{accountRepo: r}
	result, err := s.SetQuotaCooldown(context.Background(), a.ID, "")
	require.NoError(t, err)
	require.Equal(t, official, *result.OfficialResetAt)
	require.Equal(t, 0, result.RetryAt.Nanosecond()%1000)
	require.False(t, result.RetryAt.Before(official))
	require.Less(t, result.RetryAt.Sub(official), time.Microsecond)
	r.account.Extra = nil
	result, err = s.SetQuotaCooldown(context.Background(), a.ID, GrokQuotaWaitUsageLimitReached)
	require.NoError(t, err)
	require.Nil(t, result.OfficialResetAt)
	require.Equal(t, 0, result.RetryAt.Nanosecond()%1000)
}

func TestGrokUsageLimitReached402UsesRecoverableNativeCooldown(t *testing.T) {
	a := quotaWaitFixture(time.Now().UTC(), []xai.BillingProductSummary{{Product: "GrokImagine", UsagePercent: floatPtr(100)}})
	r := &grokQuotaAccountRepo{}
	s := &OpenAIGatewayService{accountRepo: r}
	body := []byte(`{"error":"You have reached your usage limit."}`)
	d := classifyGrokUpstreamFailure(http.StatusPaymentRequired, body, "grok-4.7-build-fast")
	require.Equal(t, GrokFailureBilling, d.Class)
	start := time.Now()
	require.True(t, s.applyGrokUpstreamFailureDecision(context.Background(), a, d))
	require.Equal(t, 1, r.rateLimitedCalls)
	require.Zero(t, r.tempUnschedCalls)
	require.WithinDuration(t, start.Add(grokSpendingLimitProbeCooldown), r.lastRateLimitResetAt, time.Second)
	require.True(t, isGrokSpendingLimitError(body))
	require.False(t, isGrokSpendingLimitError([]byte(`{"error":"input exceeds context limit"}`)))
	for _, message := range []string{"not_usage_limit_reached", "You have not reached your usage limit.", "Usage limit increased", "The usage limit could not be read"} {
		require.False(t, isGrokUsageLimitReachedText(message), message)
	}
}
