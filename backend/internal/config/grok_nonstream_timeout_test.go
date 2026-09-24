package config

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLoadGrokNonstreamResponseHeaderBudget(t *testing.T) {
	for _, tc := range []struct {
		env  string
		want int
		bad  bool
	}{
		{"", 240, false}, {"360", 360, false}, {"0", 0, false}, {"-1", 0, true}, {"1801", 0, true},
	} {
		t.Run("seconds="+tc.env, func(t *testing.T) {
			resetViperWithJWTSecret(t)
			if tc.env != "" {
				t.Setenv("GATEWAY_GROK_NONSTREAM_RESPONSE_HEADER_TIMEOUT", tc.env)
			}
			cfg, err := Load()
			if tc.bad {
				require.ErrorContains(t, err, "grok_nonstream_response_header_timeout")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.Gateway.GrokNonstreamResponseHeaderTimeout)
		})
	}
}
