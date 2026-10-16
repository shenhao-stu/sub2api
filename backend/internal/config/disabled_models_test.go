package config

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLoadDisabledModelsEnvironment(t *testing.T) {
	resetViperWithJWTSecret(t)
	t.Setenv("GATEWAY_DISABLED_MODELS", "gpt-5.5,gpt-5.5-*")
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, []string{"gpt-5.5", "gpt-5.5-*"}, cfg.Gateway.DisabledModels)
}

func TestLoadDisabledModelsDefault(t *testing.T) {
	resetViperWithJWTSecret(t)
	t.Setenv("GATEWAY_DISABLED_MODELS", "")
	cfg, err := Load()
	require.NoError(t, err)
	require.Empty(t, cfg.Gateway.DisabledModels)
}
