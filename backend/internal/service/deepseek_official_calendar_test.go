//go:build unit

package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDeepSeekOfficialHolidayCost(t *testing.T) {
	bs := newTestBillingService()
	official := &Group{Name: "deepseek-official", Platform: PlatformDeepseek}
	shared := &Group{Name: "another-deepseek-supplier", Platform: PlatformDeepseek}
	for _, tc := range []struct {
		date  string
		group *Group
		want  float64
	}{
		{"2026-10-01T10:00:00+08:00", official, 0.30},
		{"2026-10-08T10:00:00+08:00", official, 0.60},
		{"2026-10-08T12:00:00+08:00", official, 0.30},
		{"2027-09-15T14:00:00+08:00", official, 0.30},
		{"2028-01-03T10:00:00+08:00", official, 0.30},
		{"2026-10-01T10:00:00+08:00", shared, 0.60},
	} {
		when, err := time.Parse(time.RFC3339, tc.date)
		require.NoError(t, err)
		cost, err := bs.CalculateCostUnified(CostInput{Ctx: context.Background(), Model: "deepseek-v4.1-flash", Tokens: UsageTokens{InputTokens: 1000000}, Group: tc.group, RateMultiplier: 2, PricingAt: when, Resolver: NewModelPricingResolver(nil, bs)})
		require.NoError(t, err)
		require.InDelta(t, tc.want, cost.ActualCost, 1e-10, tc.date)
	}
}
