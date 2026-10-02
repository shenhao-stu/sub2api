//go:build unit

package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type responseDrainCloser struct{ closed atomic.Bool }

func (b *responseDrainCloser) Close() error { b.closed.Store(true); return nil }

func TestResponseDrainStartsAtDisconnectAndStopsOnCompletion(t *testing.T) {
	for _, complete := range []bool{false, true} {
		t.Run(map[bool]string{false: "deadline", true: "completed"}[complete], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				body := &responseDrainCloser{}
				finish := boundDisconnectedResponseDrain(ctx, body)
				defer finish()
				time.Sleep(time.Hour)
				require.False(t, body.closed.Load(), "connected streams have no new lifetime limit")
				cancel()
				synctest.Wait()
				time.Sleep(disconnectedResponseDrainTimeout - time.Second)
				require.False(t, body.closed.Load(), "keep reading authoritative usage during the allowance")
				if complete {
					finish()
					finish()
				}
				time.Sleep(time.Second)
				synctest.Wait()
				require.Equal(t, !complete, body.closed.Load())
			})
		})
	}
}

func TestDisconnectedChatStreamWithHeartbeatsRetainsUsageAndTerminates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil).WithContext(ctx)
		reader, writer := io.Pipe()
		defer reader.Close()
		writerDone := make(chan struct{})
		go func() {
			defer close(writerDone)
			defer writer.Close()
			_, _ = io.WriteString(writer, "data: {\"type\":\"response.in_progress\",\"response\":{\"id\":\"drain-test\",\"usage\":{\"input_tokens\":100,\"output_tokens\":7}}}\n\n")
			cancel()
			for {
				time.Sleep(time.Second)
				if _, err := io.WriteString(writer, ": heartbeat\n\n"); err != nil {
					return
				}
			}
		}()
		start := time.Now()
		result, err := (&OpenAIGatewayService{}).handleChatStreamingResponse(
			&http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: reader}, c,
			&Account{ID: 1, Platform: PlatformGrok}, "grok-4.6", "grok-4.6", "grok-4.6", start, 1)
		require.Error(t, err)
		require.NotNil(t, result)
		require.Equal(t, 100, result.Usage.InputTokens)
		require.Equal(t, 7, result.Usage.OutputTokens)
		require.Equal(t, disconnectedResponseDrainTimeout, time.Since(start))
		<-writerDone
	})
}
