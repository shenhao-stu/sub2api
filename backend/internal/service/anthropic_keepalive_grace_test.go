package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// A synthetic upstream operation drives the same wait/write seam as Forward.
// One invocation spans both header and body waits, never a customer replay.
func exerciseAnthropicJSONGrace(t *testing.T, interval, grace int, delay time.Duration, status int, wantBeat bool) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	svc := newForwardPartialUsageServiceForTest(nil)
	svc.cfg.Gateway.AnthropicNonstreamKeepaliveInterval = interval
	svc.cfg.Gateway.AnthropicNonstreamKeepaliveGraceSeconds = grace
	var calls atomic.Int32
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		c, _ := gin.CreateTestContext(w)
		c.Request = r
		svc.initAnthropicKeepalive(c, newAnthropicAPIKeyAccountForTest(), false)
		ctx, cancel := anthropicKeepaliveForwardContext(r.Context(), c)
		defer cancel()
		_, _ = awaitAnthropicUpstream(c, func() (int, error) {
			calls.Add(1)
			select {
			case <-time.After(delay):
				return status, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		})
		if status >= 400 {
			WriteAnthropicErrorResponse(c, status, gin.H{"type": "error", "error": gin.H{"type": "api_error", "message": "synthetic failure"}})
		} else {
			c.Data(status, "application/json", []byte(`{"type":"message","content":[{"type":"text","text":"synthetic result"}],"usage":{"input_tokens":7,"output_tokens":4}}`))
		}
	}))
	defer server.Close()
	started := time.Now()
	resp, err := (&http.Client{Timeout: delay + 5*time.Second}).Post(server.URL+"/v1/messages", "application/json", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	headersAfter := time.Since(started)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	<-done
	require.Equal(t, int32(1), calls.Load())
	require.Equal(t, wantBeat, strings.HasPrefix(string(body), " \n"))
	wantStatus := status
	if wantBeat {
		wantStatus = http.StatusOK
		require.Less(t, headersAfter, delay)
	}
	require.Equal(t, wantStatus, resp.StatusCode)
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	var payload map[string]any
	require.NoError(t, decoder.Decode(&payload))
	require.ErrorIs(t, decoder.Decode(new(any)), io.EOF)
	if status >= 400 {
		require.Equal(t, "error", payload["type"])
		if wantBeat {
			require.Equal(t, float64(status), payload["error"].(map[string]any)["status_code"])
		}
	} else {
		require.Equal(t, float64(7), payload["usage"].(map[string]any)["input_tokens"])
		require.Equal(t, float64(4), payload["usage"].(map[string]any)["output_tokens"])
	}
	t.Logf("status=%d wire=%d headers=%s total=%s calls=%d", status, resp.StatusCode, headersAfter, time.Since(started), calls.Load())
}

func TestAnthropicJSONGraceKeepsShortFailureHTTPStatus(t *testing.T) {
	for _, status := range []int{429, 502} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			exerciseAnthropicJSONGrace(t, 1, 3, 1200*time.Millisecond, status, false)
		})
	}
}

func TestAnthropicJSONGraceRetainsLongJSONAndLateError(t *testing.T) {
	for _, status := range []int{200, 502} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			exerciseAnthropicJSONGrace(t, 1, 1, 2200*time.Millisecond, status, true)
		})
	}
}

func TestAnthropicJSONGraceRealThirtySecondError(t *testing.T) {
	if os.Getenv("SUB2API_TEST_LONG_KEEPALIVE") != "1" {
		t.Skip("explicit loopback 30-second error")
	}
	exerciseAnthropicJSONGrace(t, 20, 90, 30*time.Second, 429, false)
}

func TestAnthropicJSONGraceIsNotResetBetweenWaits(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	svc := newForwardPartialUsageServiceForTest(nil)
	svc.cfg.Gateway.AnthropicNonstreamKeepaliveInterval = 1
	svc.cfg.Gateway.AnthropicNonstreamKeepaliveGraceSeconds = 2
	svc.initAnthropicKeepalive(c, newAnthropicAPIKeyAccountForTest(), false)
	_, cancel := anthropicKeepaliveForwardContext(context.Background(), c)
	defer cancel()
	for i := 0; i < 3; i++ {
		_, _ = awaitAnthropicUpstream(c, func() (int, error) { time.Sleep(750 * time.Millisecond); return 0, nil })
	}
	require.True(t, AnthropicJSONKeepaliveCommitted(c), "header/body phases share the first commit deadline")
}

func TestAnthropicJSONGraceUsesIngressTimeAcrossAccounts(t *testing.T) {
	for _, age := range []time.Duration{0, 80 * time.Second, 120 * time.Second, -time.Hour} {
		t.Run(age.String(), func(t *testing.T) {
			now := time.Now()
			start := now.Add(-age)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil).WithContext(context.WithValue(context.Background(), ctxkey.RequestStartedAt, start))
			svc := newForwardPartialUsageServiceForTest(nil)
			svc.cfg.Gateway.AnthropicNonstreamKeepaliveInterval = 20
			svc.cfg.Gateway.AnthropicNonstreamKeepaliveGraceSeconds = 90
			require.True(t, svc.initAnthropicKeepalive(c, newAnthropicAPIKeyAccountForTest(), false))
			beat := anthropicKeepaliveFromContext(c).nextBeat
			if age >= 0 {
				require.Equal(t, start.Add(90*time.Second), beat)
			} else {
				require.WithinDuration(t, now.Add(90*time.Second), beat, time.Second, "future time cannot extend the grace")
			}
			_, cancel := anthropicKeepaliveForwardContext(c.Request.Context(), c)
			cancel()
			require.True(t, svc.initAnthropicKeepalive(c, newAnthropicOAuthAccountForPartialUsageTest(), false))
			require.Equal(t, beat, anthropicKeepaliveFromContext(c).nextBeat, "account failover cannot restart grace")
		})
	}
}

func TestAnthropicJSONGraceDoesNotCommitOnCancellation(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprint(deadline), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil).WithContext(ctx)
			svc := newForwardPartialUsageServiceForTest(nil)
			svc.cfg.Gateway.AnthropicNonstreamKeepaliveInterval = 1
			svc.cfg.Gateway.AnthropicNonstreamKeepaliveGraceSeconds = 90
			svc.initAnthropicKeepalive(c, newAnthropicAPIKeyAccountForTest(), false)
			if deadline {
				anthropicKeepaliveFromContext(c).deadline = time.Now().Add(30 * time.Millisecond)
			} else {
				time.AfterFunc(30*time.Millisecond, cancel)
			}
			upstream, release := anthropicKeepaliveForwardContext(ctx, c)
			defer release()
			_, err := awaitAnthropicUpstream(c, func() (int, error) { <-upstream.Done(); return 0, upstream.Err() })
			require.Error(t, err)
			require.False(t, c.Writer.Written())
		})
	}
}

func TestAnthropicJSONGraceExpiredDoesNotAddAnotherDelay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	release := make(chan struct{})
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		c, _ := gin.CreateTestContext(w)
		c.Request = r.WithContext(context.WithValue(r.Context(), ctxkey.RequestStartedAt, time.Now().Add(-120*time.Second)))
		svc := newForwardPartialUsageServiceForTest(nil)
		svc.cfg.Gateway.AnthropicNonstreamKeepaliveInterval = 20
		svc.cfg.Gateway.AnthropicNonstreamKeepaliveGraceSeconds = 90
		svc.initAnthropicKeepalive(c, newAnthropicAPIKeyAccountForTest(), false)
		_, cancel := anthropicKeepaliveForwardContext(c.Request.Context(), c)
		defer cancel()
		_, _ = awaitAnthropicUpstream(c, func() (int, error) { <-release; return 0, nil })
		c.Data(200, "application/json", []byte(`{"type":"message"}`))
	}))
	defer server.Close()
	resp, err := (&http.Client{Timeout: time.Second}).Post(server.URL+"/v1/messages", "application/json", nil)
	close(release)
	<-done
	require.NoError(t, err, "elapsed ingress grace must not restart at Forward")
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(string(body), " \n"))
	require.True(t, json.Valid(body))
}

func TestAnthropicJSONGraceZeroRetainsExistingOptIn(t *testing.T) {
	exerciseAnthropicJSONGrace(t, 1, 0, 1200*time.Millisecond, 200, true)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	svc := newForwardPartialUsageServiceForTest(nil)
	svc.cfg.Gateway.AnthropicNonstreamKeepaliveInterval = 0
	svc.cfg.Gateway.AnthropicNonstreamKeepaliveGraceSeconds = 90
	require.False(t, svc.initAnthropicKeepalive(c, newAnthropicAPIKeyAccountForTest(), false))
	require.Nil(t, anthropicKeepaliveFromContext(c))
}
