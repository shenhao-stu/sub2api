//go:build unit

package service

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type fixedChargeFailureCache struct {
	*fixedChargeCacheStub
	balanceErr, scopeErr bool
}

func (c *fixedChargeFailureCache) ReserveInflightBalance(ctx context.Context, userID int64, requestID string, amount, balance float64, ttl time.Duration) (bool, float64, error) {
	if c.balanceErr {
		return false, 0, errors.New("redis unavailable")
	}
	return c.fixedChargeCacheStub.ReserveInflightBalance(ctx, userID, requestID, amount, balance, ttl)
}

func (c *fixedChargeFailureCache) ReserveInflightScope(ctx context.Context, scope, requestID string, amount float64, ttl time.Duration) (float64, error) {
	if c.scopeErr {
		return 0, errors.New("redis unavailable")
	}
	return c.fixedChargeCacheStub.ReserveInflightScope(ctx, scope, requestID, amount, ttl)
}

func TestV0216FixedChargeRequiresAtomicReservation(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		enabled, balanceErr, scopeErr bool
		key                           *APIKey
	}{
		{name: "disabled"},
		{name: "balance redis failure", enabled: true, balanceErr: true},
		{name: "key redis failure", enabled: true, scopeErr: true, key: &APIKey{ID: 2, Quota: 100}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Billing.InflightReservation.Enabled = tc.enabled
			cache := &fixedChargeFailureCache{fixedChargeCacheStub: &fixedChargeCacheStub{balance: 100}, balanceErr: tc.balanceErr, scopeErr: tc.scopeErr}
			svc := NewBillingCacheService(cache, nil, nil, nil, nil, nil, cfg, nil)
			t.Cleanup(svc.Stop)
			res, err := svc.ReserveFixedCharge(context.Background(), &User{ID: 1}, tc.key, nil, nil, 25)
			require.ErrorIs(t, err, ErrBillingServiceUnavailable)
			require.Nil(t, res)
			for scope, reservations := range cache.reserved {
				require.Empty(t, reservations, scope)
			}
			// Existing inference estimates retain their fail-open availability policy.
			res, err = svc.ReserveInflight(context.Background(), &User{ID: 1}, nil, nil, 25)
			require.NoError(t, err)
			res.Release()
		})
	}
}

func TestV0216FixedChargeRejectsInvalidAmount(t *testing.T) {
	svc := &BillingCacheService{}
	for _, amount := range []float64{-1, math.NaN(), math.Inf(1)} {
		_, err := svc.ReserveFixedCharge(context.Background(), &User{ID: 1}, nil, nil, nil, amount)
		require.ErrorIs(t, err, ErrBillingServiceUnavailable)
	}
}

func TestV0216CacheQuotePreservesMissingPriceErrors(t *testing.T) {
	svc, _ := newGeminiCacheBillingGatewayForTest(t)
	key := &APIKey{User: &User{ID: 1}, Group: &Group{RateMultiplier: 1, Hydrated: true}}
	for _, result := range []*ForwardResult{
		{Model: "unknown-upstream", Usage: ClaudeUsage{InputTokens: 10}},
		{Model: "unknown-upstream", CacheStorageTokenHours: 10},
	} {
		quote, err := svc.QuoteUsageCost(context.Background(), &RecordUsageInput{Result: result, APIKey: key})
		require.ErrorIs(t, err, ErrModelPricingUnavailable)
		require.Zero(t, quote)
	}
}
