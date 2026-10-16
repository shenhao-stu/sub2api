package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestAnthropicKeepaliveNonstreamDeadlineAndCancellation(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		for _, stage := range []string{"headers", "body"} {
			for _, canceled := range []bool{false, true} {
				t.Run(fmt.Sprintf("passthrough=%t/%s/canceled=%t", passthrough, stage, canceled), func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					rec := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(rec)
					c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil).WithContext(ctx)
					release := make(chan struct{})
					body := io.NopCloser(strings.NewReader(`{"type":"message"}`))
					if stage == "body" {
						close(release)
						r, w := io.Pipe()
						defer w.Close()
						body = r
					}
					up := &anthropicWaitingUpstream{release: release, response: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: body}}
					svc := newForwardPartialUsageServiceForTest(&anthropicHTTPUpstreamRecorder{})
					svc.httpUpstream = up
					svc.cfg.Gateway.AnthropicNonstreamKeepaliveInterval = 10
					account := newAnthropicOAuthAccountForPartialUsageTest()
					if passthrough {
						account = newAnthropicAPIKeyAccountForTest()
					}
					svc.initAnthropicKeepalive(c, account, false)
					k := anthropicKeepaliveFromContext(c)
					k.interval = 5 * time.Millisecond
					if canceled {
						time.AfterFunc(50*time.Millisecond, cancel)
					} else {
						k.deadline = time.Now().Add(50 * time.Millisecond)
					}
					parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(`{"model":"claude-sonnet-4-5","stream":false,"messages":[{"role":"user","content":"hi"}]}`)), PlatformAnthropic)
					require.NoError(t, err)
					started := time.Now()
					result, err := svc.Forward(ctx, c, account, parsed)
					require.Less(t, time.Since(started), time.Second, "deadline/cancel must close a stalled body and join the reader")
					require.Error(t, err)
					require.Nil(t, result, "no authoritative usage was received")
					require.Equal(t, int32(1), up.calls.Load())
					require.Nil(t, k.upstreamContext, "returned attempt must not contaminate the next provider")
					require.Contains(t, rec.Body.String(), " \n")
					if stage == "body" {
						_, writeErr := body.(*io.PipeReader).Read(make([]byte, 1))
						require.ErrorIs(t, writeErr, io.ErrClosedPipe)
					}
				})
			}
		}
	}
}

func TestAnthropicKeepaliveNonstreamBudgetSharedAcrossAttempts(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	svc := newForwardPartialUsageServiceForTest(&anthropicHTTPUpstreamRecorder{})
	svc.cfg.Gateway.AnthropicNonstreamKeepaliveInterval = 10
	account := newAnthropicOAuthAccountForPartialUsageTest()
	svc.initAnthropicKeepalive(c, account, false)
	k := anthropicKeepaliveFromContext(c)
	deadline := k.deadline
	require.InDelta(t, 1200, time.Until(deadline).Seconds(), 1)
	first, cancel := anthropicKeepaliveForwardContext(context.Background(), c)
	d1, ok := first.Deadline()
	require.True(t, ok)
	cancel()
	svc.initAnthropicKeepalive(c, newAnthropicAPIKeyAccountForTest(), false)
	second, cancelSecond := anthropicKeepaliveForwardContext(context.Background(), c)
	defer cancelSecond()
	d2, ok := second.Deadline()
	require.True(t, ok)
	require.Equal(t, deadline, d1)
	require.Equal(t, d1, d2)
	require.NoError(t, second.Err(), "canceling one attempt must not poison the next")
	parent, cancelParent := context.WithDeadline(context.Background(), time.Now().Add(time.Second))
	defer cancelParent()
	shorter, release := anthropicKeepaliveForwardContext(parent, c)
	defer release()
	d3, _ := shorter.Deadline()
	parentDeadline, _ := parent.Deadline()
	require.Equal(t, parentDeadline, d3)
}

func TestAnthropicKeepaliveSlowSSEHTTPErrorIsNamedTerminal(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		t.Run(fmt.Sprint(passthrough), func(t *testing.T) {
			release := make(chan struct{})
			time.AfterFunc(30*time.Millisecond, func() { close(release) })
			up := &anthropicWaitingUpstream{release: release, response: &http.Response{StatusCode: 400, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("<html>invalid upstream</html>"))}}
			svc := newForwardPartialUsageServiceForTest(&anthropicHTTPUpstreamRecorder{})
			svc.httpUpstream = up
			svc.cfg.Gateway.StreamKeepaliveInterval = 10
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			account := newAnthropicOAuthAccountForPartialUsageTest()
			if passthrough {
				account = newAnthropicAPIKeyAccountForTest()
			}
			svc.initAnthropicKeepalive(c, account, true)
			anthropicKeepaliveFromContext(c).interval = 5 * time.Millisecond
			parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(`{"model":"claude-sonnet-4-5","stream":true,"messages":[{"role":"user","content":"hi"}]}`)), PlatformAnthropic)
			require.NoError(t, err)
			result, err := svc.Forward(context.Background(), c, account, parsed)
			require.Error(t, err)
			require.Nil(t, result)
			require.Equal(t, 200, rec.Code)
			require.Contains(t, rec.Body.String(), anthropicPingFrame)
			require.Contains(t, rec.Body.String(), "event: error\ndata: ")
			require.NotContains(t, rec.Body.String(), "<html>")
			require.Equal(t, 1, strings.Count(rec.Body.String(), "event: error\n"))
			require.Equal(t, int32(1), up.calls.Load())
			require.True(t, IsResponseCommitted(c))
		})
	}
}

func TestAnthropicKeepaliveScopeEndsBeforeAnotherProvider(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	svc := newForwardPartialUsageServiceForTest(&anthropicHTTPUpstreamRecorder{})
	svc.cfg.Gateway.AnthropicNonstreamKeepaliveInterval = 10
	require.True(t, svc.initAnthropicKeepalive(c, newAnthropicOAuthAccountForPartialUsageTest(), false))
	parent, releaseParent := anthropicKeepaliveForwardContext(context.Background(), c)
	child, releaseChild := anthropicKeepaliveForwardContext(parent, c)
	require.Equal(t, child, anthropicKeepaliveFromContext(c).upstreamContext)
	releaseChild()
	require.Equal(t, parent, anthropicKeepaliveFromContext(c).upstreamContext)
	releaseParent()
	require.Nil(t, anthropicKeepaliveFromContext(c).upstreamContext)
	require.False(t, svc.initAnthropicKeepalive(c, &Account{Platform: PlatformAntigravity}, false))
	r, w := io.Pipe()
	defer r.Close()
	go func() {
		defer w.Close()
		time.Sleep(20 * time.Millisecond)
		_, _ = io.WriteString(w, `{"type":"message"}`)
	}()
	body, err := ReadUpstreamResponseBody(r, svc.cfg, c, nil)
	require.NoError(t, err)
	require.JSONEq(t, `{"type":"message"}`, string(body))
	require.Empty(t, rec.Body.String(), "inactive state must neither close another provider nor inject heartbeats")
}
