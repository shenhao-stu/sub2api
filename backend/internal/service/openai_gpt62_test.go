package service

import (
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestGPT62RoutingAndPricing(t *testing.T) {
	s := &BillingService{fallbackPrices: map[string]*ModelPricing{}}
	s.initFallbackPricing()
	boundary := time.Date(2027, 3, 1, 8, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		model                        string
		input, cached, write, output float64
	}{
		{"gpt-6.2-astra", 8, .4, 10, 40}, {"gpt-6.2-sol", 2, .1, 2.5, 10},
	} {
		t.Run(tc.model, func(t *testing.T) {
			require.Contains(t, openai.DefaultModelIDs(), tc.model)
			alias := "openai/" + tc.model + "-high"
			require.Equal(t, tc.model, normalizeCodexModel(alias))
			require.Equal(t, tc.model, NormalizeOpenAICompatRequestedModel(alias))
			require.Equal(t, tc.model, newConfiguredCodexModelDescriptor(tc.model).Slug)
			require.Empty(t, openai.GPT62ModelID(tc.model+"-preview"))
			require.Error(t, validateGPT61CompatRequest([]byte(`{"reasoning":{"effort":"none"}}`), tc.model))
			require.NoError(t, validateGPT61CompatRequest([]byte(`{"reasoning":{"effort":"high"}}`), tc.model))
			p := s.getFallbackPricing(alias)
			require.NotNil(t, p)
			for _, pair := range [][2]float64{{tc.input, p.InputPricePerToken}, {tc.cached, p.CacheReadPricePerToken}, {tc.write, p.CacheCreationPricePerToken}, {tc.output, p.OutputPricePerToken}, {tc.input * 2, p.InputPricePerTokenPriority}} {
				require.InDelta(t, pair[0], pair[1]*1e6, 1e-9)
			}
			for _, v := range []struct {
				at   time.Time
				mult float64
			}{{boundary.Add(-time.Second), 3}, {boundary, 6}} {
				resolved := s.applyModelSpecificPricingPolicyEx(tc.model, p, true, v.at)
				require.Equal(t, v.mult, resolved.UltrafastMultiplier)
				require.NotNil(t, resolved.CacheCreation12hPricePerToken)
				require.InDelta(t, 2*tc.write, *resolved.CacheCreation12hPricePerToken*1e6, 1e-9)
			}
			require.Equal(t, 272000, p.LongContextInputThreshold)
			require.Equal(t, 2.0, p.LongContextInputMultiplier)
			require.Equal(t, 1.5, p.LongContextOutputMultiplier)
			dynamic := (&PricingService{}).matchOpenAIModel(tc.model)
			require.NotNil(t, dynamic)
			require.InDelta(t, tc.input, dynamic.InputCostPerToken*1e6, 1e-9)
		})
	}
}
