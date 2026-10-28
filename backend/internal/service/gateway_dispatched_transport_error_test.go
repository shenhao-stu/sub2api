//go:build unit

package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"syscall"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestGatewayTransportDispatchBoundary(t *testing.T) {
	for _, path := range []string{"anthropic", "gemini"} {
		for _, cause := range []error{io.EOF, context.DeadlineExceeded, syscall.ECONNREFUSED} {
			for _, sent := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%v/sent=%t", path, cause, sent), func(t *testing.T) {
					repo := &transportTempUnschedRepoStub{}
					c := newTransportErrorTestGin(t)
					account := &Account{ID: 33861, Platform: PlatformAnthropic}
					attempt := &HTTPUpstreamAttemptError{Err: cause, RequestMayHaveBeenSent: sent, ConnectionReused: true, Elapsed: 12 * time.Second}
					var forward func(context.Context, *gin.Context, *Account, error) error
					if path == "anthropic" {
						s := &GatewayService{accountRepo: repo}
						forward = func(ctx context.Context, c *gin.Context, a *Account, err error) error {
							return s.handleUpstreamTransportError(ctx, c, a, err, OpsUpstreamErrorEvent{Passthrough: true, UpstreamURL: "https://example.invalid/v1/messages"})
						}
					} else {
						forward = (&GeminiMessagesCompatService{accountRepo: repo}).handleUpstreamTransportError
					}
					err := forward(context.Background(), c, account, fmt.Errorf("forward: %w", attempt))
					var failover *UpstreamFailoverError
					require.Equal(t, !sent, errors.As(err, &failover), "an ambiguous dispatched request must not be submitted again")
					if sent {
						require.ErrorIs(t, err, cause)
					}
					wantEvictions := 0
					if !sent && cause == syscall.ECONNREFUSED {
						wantEvictions = 1
					}
					require.Equal(t, wantEvictions, repo.calls)
					require.False(t, c.Writer.Written(), "the handler owns the HTTP/SSE response")
					raw, exists := c.Get(OpsUpstreamErrorsKey)
					require.True(t, exists)
					events := raw.([]*OpsUpstreamErrorEvent)
					require.Len(t, events, 1)
					require.EqualValues(t, 12000, events[0].DurationMs)
					require.True(t, events[0].ConnectionReused)
					require.Equal(t, sent, events[0].RequestMayHaveBeenSent)
					if sent {
						require.Equal(t, "awaiting_response_headers", events[0].Stage)
						require.Equal(t, "dispatched_request_not_retried", events[0].Reason)
					}
					if path == "anthropic" {
						require.True(t, events[0].Passthrough)
						require.Equal(t, "https://example.invalid/v1/messages", events[0].UpstreamURL)
					}
				})
			}
		}
	}
}
