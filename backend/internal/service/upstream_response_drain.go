package service

import (
	"context"
	"io"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"go.uber.org/zap"
)

const disconnectedResponseDrainTimeout = 5 * time.Minute

// A disconnected client still needs upstream usage, but heartbeats must not keep
// an abandoned response alive forever. Connected requests have no added limit.
func boundDisconnectedResponseDrain(ctx context.Context, body io.Closer) func() {
	finished, finish := context.WithCancel(context.Background())
	stop := context.AfterFunc(ctx, func() {
		timer := time.NewTimer(disconnectedResponseDrainTimeout)
		defer timer.Stop()
		select {
		case <-finished.Done():
		case <-timer.C:
			logger.FromContext(ctx).Warn("upstream response drain deadline exceeded",
				zap.Duration("drain_timeout", disconnectedResponseDrainTimeout))
			_ = body.Close()
		}
	})
	return func() {
		stop()
		finish()
	}
}
