//go:build unit

package xai

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMapJWTSubscriptionTierNumber(t *testing.T) {
	t.Parallel()

	require.Equal(t, "free", MapJWTSubscriptionTier(0))
	require.Equal(t, "supergrok", MapJWTSubscriptionTier(1))
	require.Equal(t, "x_basic", MapJWTSubscriptionTier(2))
	require.Equal(t, "x_premium", MapJWTSubscriptionTier(3))
	require.Equal(t, "x_premium_plus", MapJWTSubscriptionTier(4))
	require.Equal(t, "supergrok_heavy", MapJWTSubscriptionTier(5))
	require.Equal(t, "supergrok_lite", MapJWTSubscriptionTier(6))
	require.Equal(t, "supergrok_plus", MapJWTSubscriptionTier(7))
	require.Equal(t, "9", MapJWTSubscriptionTier(9))
}

func TestNormalizeSubscriptionTierAliases(t *testing.T) {
	t.Parallel()

	require.Equal(t, "free", NormalizeSubscriptionTier("Free"))
	require.Equal(t, "free", NormalizeSubscriptionTier(" FREE "))
	require.Equal(t, "supergrok", NormalizeSubscriptionTier("SuperGrok"))
	require.Equal(t, "supergrok_heavy", NormalizeSubscriptionTier("SuperGrok Heavy"))
	require.Equal(t, "supergrok_pro", NormalizeSubscriptionTier("SuperGrokPro"))
	require.Equal(t, "supergrok_lite", NormalizeSubscriptionTier("SuperGrok Lite"))
	require.Equal(t, "supergrok_lite", NormalizeSubscriptionTier("SuperGrokLite"))
	require.Equal(t, "x_basic", NormalizeSubscriptionTier("X Basic"))
	require.Equal(t, "free", NormalizeSubscriptionTier("free-tier"))
	require.Equal(t, "free", NormalizeSubscriptionTier("free_tier"))
	require.Equal(t, "free", NormalizeSubscriptionTier("grok-basic"))
	require.Equal(t, "free", NormalizeSubscriptionTier("grok_basic"))
	require.Equal(t, "supergrok_lite", NormalizeSubscriptionTier("supergrok_lite"))
}

func TestSubscriptionTierFromJWTUsesNumericClaim(t *testing.T) {
	t.Parallel()

	require.Equal(t, "supergrok_heavy", SubscriptionTierFromJWT(jwtWithClaims(t, map[string]any{"tier": 5})))
	require.Equal(t, "free", SubscriptionTierFromJWT(jwtWithClaims(t, map[string]any{"tier": 0})))
	require.Equal(t, "supergrok_lite", SubscriptionTierFromJWT(jwtWithClaims(t, map[string]any{"tier": 6})))
	require.Empty(t, SubscriptionTierFromJWT(jwtWithClaims(t, map[string]any{"sub": "user"})))
	require.Empty(t, SubscriptionTierFromJWT("not-a-jwt"))
}

func TestSubscriptionTierFromAccessToken(t *testing.T) {
	t.Parallel()

	// Shape of a lapsed (free) Build access token: decodable, no tier claim.
	freeClaims := map[string]any{
		"iss": "https://auth.x.ai", "aud": "build", "exp": 1_900_000_000, "iat": 1_899_990_000,
		"scope": "openid", "principal_type": "user", "principal_id": "p-1", "client_id": "c-1",
	}
	withClaim := func(key string, value any) map[string]any {
		claims := make(map[string]any, len(freeClaims)+1)
		for k, v := range freeClaims {
			claims[k] = v
		}
		claims[key] = value
		return claims
	}

	tests := []struct {
		name     string
		token    string
		wantTier string
		wantOK   bool
	}{
		{name: "tier 5 is heavy", token: jwtWithClaims(t, withClaim("tier", 5)), wantTier: "supergrok_heavy", wantOK: true},
		{name: "explicit tier 0 is free", token: jwtWithClaims(t, withClaim("tier", 0)), wantTier: "free", wantOK: true},
		{name: "explicit tier without exp still maps", token: jwtWithClaims(t, map[string]any{"tier": 1}), wantTier: "supergrok", wantOK: true},
		{name: "decodable access token without tier is free", token: jwtWithClaims(t, freeClaims), wantTier: "free", wantOK: true},
		{name: "null tier treated as missing", token: jwtWithClaims(t, withClaim("tier", nil)), wantTier: "free", wantOK: true},
		{name: "unrecognized tier value is no evidence", token: jwtWithClaims(t, withClaim("tier", true)), wantOK: false},
		{name: "jwt without exp is no evidence", token: jwtWithClaims(t, map[string]any{"sub": "user"}), wantOK: false},
		{name: "opaque token is no evidence", token: "opaque-access-token", wantOK: false},
		{name: "undecodable payload is no evidence", token: "header.%%%.sig", wantOK: false},
		{name: "empty token is no evidence", token: "", wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tier, ok := SubscriptionTierFromAccessToken(tt.token)
			require.Equal(t, tt.wantOK, ok)
			require.Equal(t, tt.wantTier, tier)
		})
	}
}

func TestCanonicalGrokPlanUsesOnlyGrok45ResponsesWindow(t *testing.T) {
	t.Parallel()

	zero := float64(0)
	heavyReq, heavyTok := int64(8300), int64(53_000_000)
	superReq, superTok := int64(900), int64(15_000_000)
	fresh := time.Now().UTC().Format(time.RFC3339)
	stale := time.Now().Add(-GrokQuotaSignalMaxAge - time.Hour).UTC().Format(time.RFC3339)

	require.Equal(t, "supergrok", CanonicalGrokPlan(&zero, "SuperGrokPro", nil))
	require.Equal(t, "supergrok", CanonicalGrokPlan(&zero, "SuperGrok", nil))
	require.Equal(t, "supergrok_heavy", CanonicalGrokPlan(&zero, "SuperGrok Heavy", nil))
	require.Empty(t, CanonicalGrokPlan(&zero, "", nil))
	require.Equal(t, "free", CanonicalGrokPlan(&zero, "free", nil))

	require.Equal(t, "supergrok_heavy", CanonicalGrokPlan(&zero, "SuperGrokPro", &QuotaSnapshot{
		Model:             "grok-4.5",
		Requests:          &QuotaWindow{Limit: &heavyReq},
		Tokens:            &QuotaWindow{Limit: &heavyTok},
		LastHeadersSeenAt: fresh,
	}))
	require.Equal(t, "supergrok", CanonicalGrokPlan(&zero, "SuperGrokPro", &QuotaSnapshot{
		Model:             "grok-4.6",
		Requests:          &QuotaWindow{Limit: &heavyReq},
		Tokens:            &QuotaWindow{Limit: &heavyTok},
		LastHeadersSeenAt: fresh,
	}))
	require.Equal(t, "supergrok", CanonicalGrokPlan(&zero, "SuperGrokPro", &QuotaSnapshot{
		Requests:          &QuotaWindow{Limit: &heavyReq},
		Tokens:            &QuotaWindow{Limit: &heavyTok},
		LastHeadersSeenAt: fresh,
	}))
	require.Equal(t, "supergrok", CanonicalGrokPlan(&zero, "SuperGrokPro", &QuotaSnapshot{
		Model:             "grok-4.5",
		Requests:          &QuotaWindow{Limit: &superReq},
		Tokens:            &QuotaWindow{Limit: &superTok},
		LastHeadersSeenAt: fresh,
	}))
	require.Equal(t, "supergrok", CanonicalGrokPlan(&zero, "SuperGrokPro", &QuotaSnapshot{
		Model:             "grok-4.5",
		Requests:          &QuotaWindow{Limit: &heavyReq},
		LastHeadersSeenAt: stale,
	}))
	require.Equal(t, "supergrok_heavy", CanonicalGrokPlan(&zero, "SuperGrokPro", &QuotaSnapshot{
		Model:                 "grok-4.6",
		Requests:              &QuotaWindow{Limit: &superReq},
		PlanFrom45Responses:   "supergrok_heavy",
		PlanFrom45ResponsesAt: fresh,
	}))
	require.Equal(t, "free", CanonicalGrokPlan(&zero, "free", &QuotaSnapshot{
		Model:             "grok-4.5",
		Requests:          &QuotaWindow{Limit: &heavyReq},
		LastHeadersSeenAt: fresh,
	}))
	heavyCents := float64(SuperGrokHeavyLimitCents)
	require.Equal(t, "supergrok_heavy", CanonicalGrokPlan(&heavyCents, "SuperGrokPro", nil))
}

func TestApplyGrok45ResponsesPlanSignalCarriesHint(t *testing.T) {
	t.Parallel()

	heavyReq := int64(8300)
	fresh := time.Now().UTC().Format(time.RFC3339)
	prev := &QuotaSnapshot{
		Model:             "grok-4.5",
		Requests:          &QuotaWindow{Limit: &heavyReq},
		LastHeadersSeenAt: fresh,
	}
	prev.ApplyGrok45ResponsesPlanSignal(nil)
	require.Equal(t, "supergrok_heavy", prev.PlanFrom45Responses)

	next := &QuotaSnapshot{
		Model:             "grok-4.6",
		Requests:          &QuotaWindow{Limit: int64Ptr(100)},
		LastHeadersSeenAt: fresh,
	}
	next.ApplyGrok45ResponsesPlanSignal(prev)
	require.Equal(t, "supergrok_heavy", next.PlanFrom45Responses)
	require.Equal(t, prev.PlanFrom45ResponsesAt, next.PlanFrom45ResponsesAt)
}

func int64Ptr(v int64) *int64 { return &v }

func jwtWithClaims(t *testing.T, claims map[string]any) string {
	t.Helper()
	payload, err := json.Marshal(claims)
	require.NoError(t, err)
	return "header." + base64.RawURLEncoding.EncodeToString(payload) + ".sig"
}
