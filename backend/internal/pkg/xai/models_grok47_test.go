package xai

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGrok47CatalogAndPaidFastOptIn(t *testing.T) {
	mapping := ModelMappingWithOptions(ModelMappingOptions{})
	require.Contains(t, DefaultModelIDs(), "grok-4.7")
	for _, name := range []string{"grok-4.7", "grok-4.7-latest", "xai/grok-4.7"} {
		require.Equal(t, "grok-4.7", mapping[name])
		require.Equal(t, "grok-4.7", ResolveGrokTextResponsesModelID(name))
	}
	for _, name := range []string{"grok-4.7-fast", "grok-4.7-build-fast", "xai/grok-4.7-build-fast"} {
		require.True(t, IsGrokTextResponsesModelID(name))
		require.Equal(t, "grok-4.7-build-fast", ResolveGrokTextResponsesModelID(name))
		require.NotContains(t, mapping, name, "Fast requires a verified paid account mapping")
	}
}
