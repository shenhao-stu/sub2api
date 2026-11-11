package service

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/sjson"
	"testing"
	"time"
)

type consumedScopeRepo struct {
	consumedModelCooldownRepo
	accountCalls int
	models       []string
}

func (r *consumedScopeRepo) SetRateLimitedIfLater(_ context.Context, _ int64, until time.Time) error {
	r.accountCalls++
	r.until = until
	return nil
}
func (r *consumedScopeRepo) SetModelRateLimitIfLater(ctx context.Context, id int64, model string, until time.Time, reason ...string) error {
	r.models = append(r.models, model)
	return r.consumedModelCooldownRepo.SetModelRateLimitIfLater(ctx, id, model, until, reason...)
}
func TestNativeFableQuotaOnlyCoolsConfiguredFableModels(t *testing.T) {
	a := newAnthropicAPIKeyAccountForTest()
	a.Credentials["model_mapping"] = map[string]any{"claude-fable-5": "claude-fable-5", "claude-fable-5-1": "claude-fable-5-1", "claude-fable-5-2": "claude-fable-5-2", "claude-opus-5-5": "claude-opus-5-5"}
	until := time.Now().Add(time.Hour).Truncate(time.Second)
	body := consumedQuotaBody("claude-fable-5-2", "seven_day_overage_included", until.Unix())
	r := &consumedScopeRepo{}
	s := &RateLimitService{accountRepo: r}
	s.persistAnthropicConsumedModelCooldown(context.Background(), a, []byte(body), "claude-fable-5-2")
	require.ElementsMatch(t, []string{"claude-fable-5", "claude-fable-5-1", "claude-fable-5-2"}, r.models)
	require.Zero(t, r.accountCalls)
	require.True(t, a.IsSchedulable())
	require.True(t, r.until.Equal(until))
}
func TestNativeWholeAccountCooldownRequiresProvenWeeklyExhaustion(t *testing.T) {
	until := time.Now().Add(time.Hour).Truncate(time.Second)
	base := consumedQuotaBody("claude-opus-5-5", "seven_day", until.Unix())
	for _, tc := range []struct {
		name                    string
		level                   any
		window, apiError, scope string
		global                  bool
	}{
		{"confirmed", 1.01, "seven_day", "usage_limit_reached", "account", true},
		{"not_exhausted", .95, "seven_day", "usage_limit_reached", "account", false},
		{"string", "1.01", "seven_day", "usage_limit_reached", "account", false},
		{"five_hour", 1.01, "five_hour", "usage_limit_reached", "account", false},
		{"model_credits", 1.01, "seven_day", "model_requires_usage_credits", "model", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, _ := sjson.Set(base, "error.details.weekly_utilization", tc.level)
			body, _ = sjson.Set(body, "error.details.rate_limit_type", tc.window)
			body, _ = sjson.Set(body, "error.details.api_error", tc.apiError)
			body, _ = sjson.Set(body, "error.details.quota_scope", tc.scope)
			r := &consumedScopeRepo{}
			(&RateLimitService{accountRepo: r}).persistAnthropicConsumedModelCooldown(context.Background(), newAnthropicAPIKeyAccountForTest(), []byte(body), "claude-opus-5-5")
			if tc.global {
				require.Equal(t, 1, r.accountCalls)
				require.Empty(t, r.models)
			} else {
				require.Zero(t, r.accountCalls)
			}
		})
	}
}
