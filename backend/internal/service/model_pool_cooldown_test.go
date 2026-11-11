//go:build unit

package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestModelPoolCooldown(t *testing.T) {
	now := time.Unix(1800000000, 0)
	sooner, later, past := now.Add(1500*time.Millisecond), now.Add(time.Minute), now.Add(-time.Second)
	cases := []struct {
		name     string
		accounts []*Account
		want     int
	}{
		{"empty", nil, 0},
		{"all limited rounds up earliest", []*Account{{RateLimitResetAt: &later}, {RateLimitResetAt: &sooner}}, 2},
		{"healthy alternative", []*Account{{RateLimitResetAt: &later}, {}}, 0},
		{"expired alternative", []*Account{{RateLimitResetAt: &later}, {RateLimitResetAt: &past}}, 0},
		{"Command Code weekly quota", []*Account{{Platform: PlatformCommandCode, TempUnschedulableUntil: &later, TempUnschedulableReason: commandCodeUsageLimitReason + ": weekly usage limit"}}, 60},
		{"Command Code elapsed quota", []*Account{{Platform: PlatformCommandCode, TempUnschedulableUntil: &past, TempUnschedulableReason: commandCodeUsageLimitReason}}, 0},
		{"Command Code other stop", []*Account{{Platform: PlatformCommandCode, TempUnschedulableUntil: &later, TempUnschedulableReason: "manual stop"}}, 0},
		{"another provider's stop", []*Account{{Platform: PlatformOpenAI, TempUnschedulableUntil: &later, TempUnschedulableReason: commandCodeUsageLimitReason}}, 0},
		{"unrelated overload is not rate limit", []*Account{{OverloadUntil: &later}}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, modelPoolRetryAfter(context.Background(), tc.accounts, "", now))
		})
	}
}

func TestModelPoolCooldownScopes(t *testing.T) {
	now := time.Unix(1800000000, 0)
	later := now.Add(60 * time.Second)
	cooled := func(scope string, seconds int) *Account {
		a := &Account{Platform: PlatformOpenAI, Credentials: map[string]any{"model_mapping": map[string]any{"public": "mapped"}}}
		setAccountModelRateLimitSnapshot(a, scope, now.Add(time.Duration(seconds)*time.Second), "upstream_model_cooldown", now)
		return a
	}
	mixed := cooled("mapped", 30)
	mixed.RateLimitResetAt = &later
	for _, tc := range []struct {
		name     string
		accounts []*Account
		want     int
	}{
		{"mapped", []*Account{cooled("mapped", 30)}, 30},
		{"wrong model", []*Account{cooled("other", 30)}, 0},
		{"expired", []*Account{cooled("mapped", -10)}, 0},
		{"global and model use latest", []*Account{mixed}, 60},
		{"earliest eligible account", []*Account{mixed, cooled("mapped", 20)}, 20},
		{"healthy alternative", []*Account{mixed, {}}, 0},
		{"unknown account", []*Account{mixed, nil}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, modelPoolRetryAfter(context.Background(), tc.accounts, "public", now))
		})
	}
}
