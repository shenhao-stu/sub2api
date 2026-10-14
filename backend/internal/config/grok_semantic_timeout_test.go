package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGrokFirstSemanticTimeoutConfiguration(t *testing.T) {
	resetViperWithJWTSecret(t)
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, 1200, cfg.Gateway.GrokFirstSemanticTimeoutSeconds)
	for _, seconds := range []int{0, 30, 600, 1200, 3600} {
		cfg.Gateway.GrokFirstSemanticTimeoutSeconds = seconds
		require.NoError(t, cfg.Validate())
	}
	for _, seconds := range []int{-1, 1, 29, 3601} {
		cfg.Gateway.GrokFirstSemanticTimeoutSeconds = seconds
		require.ErrorContains(t, cfg.Validate(), "grok_first_semantic_timeout_seconds")
	}
	t.Setenv("GATEWAY_GROK_FIRST_SEMANTIC_TIMEOUT_SECONDS", "600")
	cfg, err = Load()
	require.NoError(t, err)
	require.Equal(t, 600, cfg.Gateway.GrokFirstSemanticTimeoutSeconds)
	t.Setenv("GATEWAY_GROK_FIRST_SEMANTIC_TIMEOUT_SECONDS", "0")
	cfg, err = Load()
	require.NoError(t, err)
	require.Zero(t, cfg.Gateway.GrokFirstSemanticTimeoutSeconds)
}
