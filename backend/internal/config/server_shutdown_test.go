package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestServerShutdownTimeoutBoundaries(t *testing.T) {
	for _, tc := range []struct {
		value   string
		want    time.Duration
		invalid bool
	}{
		{"", 540 * time.Second, false}, {"0", 540 * time.Second, false},
		{"120", 120 * time.Second, false}, {"-1", 0, true}, {"541", 0, true},
	} {
		t.Run("seconds="+tc.value, func(t *testing.T) {
			resetViperWithJWTSecret(t)
			if tc.value != "" {
				t.Setenv("SERVER_SHUTDOWN_TIMEOUT", tc.value)
			}
			cfg, err := Load()
			if tc.invalid {
				require.ErrorContains(t, err, "server.shutdown_timeout")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.Server.ShutdownDrainTimeout())
		})
	}
}
