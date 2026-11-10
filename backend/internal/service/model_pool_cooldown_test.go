//go:build unit

package service

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
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
		{"unrelated overload is not rate limit", []*Account{{OverloadUntil: &later}}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, modelPoolRetryAfter(tc.accounts, now)) })
	}
}
