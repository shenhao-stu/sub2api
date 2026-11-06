package config

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGrokRefusalIsolationConfig(t *testing.T) {
	resetViperWithJWTSecret(t)
	cfg, err := Load()
	require.NoError(t, err)
	require.True(t, cfg.Gateway.Grok.RefusalSessionIsolationEnabled)
	require.Empty(t, cfg.Gateway.Grok.RefusalSessionRelayKeyIDs)
	t.Setenv("GATEWAY_GROK_REFUSAL_SESSION_RELAY_KEY_IDS", "216,358")
	t.Setenv("GATEWAY_GROK_REFUSAL_SESSION_ISOLATION_ENABLED", "false")
	cfg, err = Load()
	require.NoError(t, err)
	require.Equal(t, []int64{216, 358}, cfg.Gateway.Grok.RefusalSessionRelayKeyIDs)
	require.False(t, cfg.Gateway.Grok.RefusalSessionIsolationEnabled)
}
