package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadAnthropicNonstreamKeepalive(t *testing.T) {
	for _, tc := range []struct {
		env     string
		want    int
		invalid bool
	}{
		{"", 0, false}, {"0", 0, false}, {"5", 5, false}, {"10", 10, false}, {"30", 30, false}, {"-1", 0, true}, {"4", 0, true}, {"31", 0, true},
	} {
		t.Run("seconds="+tc.env, func(t *testing.T) {
			resetViperWithJWTSecret(t)
			if tc.env != "" {
				t.Setenv("GATEWAY_ANTHROPIC_NONSTREAM_KEEPALIVE_INTERVAL", tc.env)
			}
			cfg, err := Load()
			if tc.invalid {
				require.ErrorContains(t, err, "anthropic_nonstream_keepalive_interval")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.Gateway.AnthropicNonstreamKeepaliveInterval)
		})
	}
}

func TestLoadAnthropicKeepaliveEnvironmentOverridesFile(t *testing.T) {
	resetViperWithJWTSecret(t)
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("gateway:\n  anthropic_nonstream_keepalive_interval: 30\n  anthropic_nonstream_keepalive_grace_seconds: 60\n"), 0600))
	t.Setenv("CONFIG_FILE", path)
	t.Setenv("GATEWAY_ANTHROPIC_NONSTREAM_KEEPALIVE_INTERVAL", "10")
	t.Setenv("GATEWAY_ANTHROPIC_NONSTREAM_KEEPALIVE_GRACE_SECONDS", "0")
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, 10, cfg.Gateway.AnthropicNonstreamKeepaliveInterval)
	require.Zero(t, cfg.Gateway.AnthropicNonstreamKeepaliveGraceSeconds)
}

func TestLoadAnthropicNonstreamKeepaliveGrace(t *testing.T) {
	for _, tc := range []struct {
		env     string
		want    int
		invalid bool
	}{
		{"", 90, false}, {"0", 0, false}, {"30", 30, false}, {"90", 90, false}, {"-1", 0, true}, {"91", 0, true},
	} {
		t.Run(tc.env, func(t *testing.T) {
			resetViperWithJWTSecret(t)
			if tc.env != "" {
				t.Setenv("GATEWAY_ANTHROPIC_NONSTREAM_KEEPALIVE_GRACE_SECONDS", tc.env)
			}
			cfg, err := Load()
			if tc.invalid {
				require.ErrorContains(t, err, "anthropic_nonstream_keepalive_grace_seconds")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.Gateway.AnthropicNonstreamKeepaliveGraceSeconds)
			require.Zero(t, cfg.Gateway.AnthropicNonstreamKeepaliveInterval, "grace alone must not enable JSON keepalive")
		})
	}
}
