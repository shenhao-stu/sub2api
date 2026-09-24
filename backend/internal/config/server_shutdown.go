package config

import "time"

// Leave 20 seconds for forced handler exit and 30 seconds for dependency cleanup
// within the deployment's ten-minute stop grace period.
const DefaultServerShutdownTimeout = 540

func (c ServerConfig) ShutdownDrainTimeout() time.Duration {
	seconds := c.ShutdownTimeout
	if seconds <= 0 {
		seconds = DefaultServerShutdownTimeout
	}
	return time.Duration(seconds) * time.Second
}
