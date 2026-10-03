//go:build unit

package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/requestlifecycle"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type shutdownStreamRecorder struct {
	*httptest.ResponseRecorder
	visible chan struct{}
	once    sync.Once
}

func TestDetachUpstreamContextRetainsForcedShutdown(t *testing.T) {
	for name, detach := range map[string]func(context.Context) (context.Context, context.CancelFunc){
		"always": detachUpstreamContext,
		"stream": func(ctx context.Context) (context.Context, context.CancelFunc) {
			return detachStreamUpstreamContext(ctx, true)
		},
	} {
		t.Run(name, func(t *testing.T) {
			client, cancelClient := context.WithCancel(context.Background())
			defer cancelClient()
			force, cancelForce := context.WithCancel(context.Background())
			defer cancelForce()
			upstream, release := detach(requestlifecycle.WithForceCancellation(client, force))
			release() // Existing builders may release immediately after building a request.
			cancelClient()
			require.NoError(t, upstream.Err())
			cancelForce()
			require.ErrorIs(t, upstream.Err(), context.Canceled)
		})
	}
}

func (w *shutdownStreamRecorder) Write(p []byte) (int, error) {
	n, err := w.ResponseRecorder.Write(p)
	if strings.Contains(string(p), "shutdown-fixture") {
		w.once.Do(func() { close(w.visible) })
	}
	return n, err
}

func TestGrokChatUpstreamLifecycleWithRealHTTP(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name          string
		forceShutdown bool
		partialUsage  bool
	}{
		{name: "client_cancel_still_collects_terminal_usage"},
		{name: "forced_shutdown_retains_observed_usage", forceShutdown: true, partialUsage: true},
		{name: "forced_shutdown_never_invents_usage", forceShutdown: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			finish, abort, upstreamCancelled := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("X-Request-ID", "shutdown-offline")
				if tc.partialUsage {
					_, _ = io.WriteString(w, "data: {\"type\":\"response.in_progress\",\"response\":{\"usage\":{\"input_tokens\":100,\"output_tokens\":7,\"input_tokens_details\":{\"cached_tokens\":40}}}}\n\n")
				}
				_, _ = io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"shutdown-fixture\"}\n\n")
				w.(http.Flusher).Flush()
				heartbeat := time.NewTicker(5 * time.Millisecond)
				defer heartbeat.Stop()
				for {
					select {
					case <-r.Context().Done():
						close(upstreamCancelled)
						return
					case <-abort:
						return
					case <-finish:
						_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_done\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":12,\"output_tokens\":5}}}\n\n")
						return
					case <-heartbeat.C:
						_, _ = io.WriteString(w, ": ping\n\n")
						w.(http.Flusher).Flush()
					}
				}
			}))
			t.Cleanup(upstream.Close)
			t.Cleanup(func() { close(abort) })
			target, err := url.Parse(upstream.URL)
			require.NoError(t, err)
			svc := openAIClientToolsTestService(nil)
			svc.httpUpstream = &grokRefusalHTTPUpstream{client: upstream.Client(), url: target}
			account := &Account{ID: 25031, Platform: PlatformGrok, Type: AccountTypeAPIKey,
				Credentials: map[string]any{"api_key": "offline-only", "base_url": "https://api.x.ai/v1"}}
			client, cancelClient := context.WithCancel(context.Background())
			defer cancelClient()
			force, cancelForce := context.WithCancel(context.Background())
			defer cancelForce()
			ctx := requestlifecycle.WithForceCancellation(client, force)
			recorder := &shutdownStreamRecorder{ResponseRecorder: httptest.NewRecorder(), visible: make(chan struct{})}
			c, _ := gin.CreateTestContext(recorder)
			body := []byte(`{"model":"grok-4.6","messages":[{"role":"user","content":"offline fixture"}],"stream":true}`)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(body))).WithContext(ctx)
			c.Set("api_key", &APIKey{ID: 25031})
			var result *OpenAIForwardResult
			var forwardErr error
			done := make(chan struct{})
			go func() {
				result, forwardErr = svc.forwardGrokChatCompletionsViaResponses(ctx, c, account, body, "offline-session", "")
				close(done)
			}()
			select {
			case <-recorder.visible:
			case <-time.After(2 * time.Second):
				t.Fatal("real upstream did not deliver the observed output")
			}
			cancelClient()
			select {
			case <-done:
				t.Fatal("ordinary client cancellation stopped usage collection")
			case <-upstreamCancelled:
				t.Fatal("ordinary client cancellation reached the upstream")
			case <-time.After(25 * time.Millisecond):
			}
			if tc.forceShutdown {
				cancelForce()
			} else {
				close(finish)
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("forwarder did not finish after lifecycle termination")
			}
			require.EqualValues(t, 1, calls.Load(), "termination must never replay an upstream attempt")
			require.NotNil(t, result)
			if tc.forceShutdown {
				require.Error(t, forwardErr)
				var failover *UpstreamFailoverError
				require.False(t, errors.As(forwardErr, &failover))
				if tc.partialUsage {
					require.Equal(t, 100, result.Usage.InputTokens)
					require.Equal(t, 7, result.Usage.OutputTokens)
					require.Equal(t, 40, result.Usage.CacheReadInputTokens)
				} else {
					require.Equal(t, OpenAIUsage{}, result.Usage)
				}
			} else {
				require.NoError(t, forwardErr)
				require.Equal(t, 12, result.Usage.InputTokens)
				require.Equal(t, 5, result.Usage.OutputTokens)
			}
		})
	}
}
