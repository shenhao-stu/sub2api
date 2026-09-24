//go:build unit

package service

import (
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"testing"
)

func TestGrok47ReasoningAndBridge(t *testing.T) {
	for _, model := range []string{"grok-4.7", "grok-4.7-latest", "grok-4.7-fast", "grok-4.7-build-fast", "xai/grok-4.7-build-fast"} {
		t.Run(model, func(t *testing.T) {
			require.True(t, grokChatResponsesBridgeModel(model))
			body, err := patchGrokResponsesBody([]byte(`{"model":"grok-4.7","input":"hi","reasoning":{"effort":"xhigh"}}`), model)
			require.NoError(t, err)
			require.Equal(t, "xhigh", gjson.GetBytes(body, "reasoning.effort").String())
		})
	}
}

func TestGrok47OfficialRatesAndFixedFastTier(t *testing.T) {
	svc := newTestBillingService()
	for _, model := range []string{"grok-4.7", "grok-4.7-latest", "grok-4.7-fast", "grok-4.7-build-fast", "xai/grok-4.7-build-fast"} {
		fast := model == "grok-4.7-fast" || model == "grok-4.7-build-fast" || model == "xai/grok-4.7-build-fast"
		factor := 1.0
		if fast {
			factor = 2
		}
		p, err := svc.GetModelPricing(model)
		require.NoError(t, err)
		require.InDelta(t, 2e-6*factor, p.InputPricePerToken, 1e-12)
		require.InDelta(t, .5e-6*factor, p.CacheReadPricePerToken, 1e-12)
		require.InDelta(t, 6e-6*factor, p.OutputPricePerToken, 1e-12)
		require.True(t, p.LongContextThresholdInclusive)
		if fast {
			for _, tier := range []string{"", "fast", "priority", "flex"} {
				require.Equal(t, 1.0, configuredServiceTierMultiplier(tier, p), tier)
			}
		}
	}
}
