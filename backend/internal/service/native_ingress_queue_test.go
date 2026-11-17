package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestNativeIngressWaitThenForwardOnce(t *testing.T) {
	s := newForwardPartialUsageServiceForTest(nil)
	acc := newAnthropicAPIKeyAccountForTest()
	acc.Extra["anthropic_passthrough"] = true
	acc.Extra["native_relay_large_body_queue"] = true
	lane := newNativeIngressLane()
	s.nativeIngressLanes.Store(acc.ID, lane)
	require.True(t, lane.slots.TryAcquire(2))
	defer lane.slots.Release(1)
	up := &queuedHTTPUpstreamStub{responses: []*http.Response{{
		StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(strings.NewReader(`{"type":"message","id":"msg_test","role":"assistant","model":"claude-opus-5-5","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":7,"output_tokens":1}}`)),
	}}}
	s.httpUpstream = up
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
	body := `{"model":"claude-opus-5-5","messages":[{"role":"user","content":"` + strings.Repeat("x", nativeLargeBodyMin) + `"}]}`
	parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(body)), PlatformAnthropic)
	require.NoError(t, err)
	type outcome struct {
		result *ForwardResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() { result, err := s.Forward(c.Request.Context(), c, acc, parsed); done <- outcome{result, err} }()
	require.Eventually(t, func() bool { return len(lane.waiters) == 1 }, 2*time.Second, time.Millisecond)
	lane.slots.Release(1)
	select {
	case out := <-done:
		require.NoError(t, out.err)
		require.NotNil(t, out.result)
		require.EqualValues(t, 7, out.result.Usage.InputTokens)
		require.EqualValues(t, 1, out.result.Usage.OutputTokens)
		require.Equal(t, 1, up.callCount)
		require.Equal(t, 200, rec.Code)
	case <-time.After(3 * time.Second):
		t.Fatal("waiter did not wake after a stream finished")
	}
}

func TestNativeIngressBoundaries(t *testing.T) {
	s := &GatewayService{}
	acc := newAnthropicAPIKeyAccountForTest()
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Request = httptest.NewRequest("POST", "/v1/messages", nil).WithContext(ctx)
	release, err := s.acquireNativeIngress(c, acc, nativeLargeBodyMin)
	require.NoError(t, err)
	require.Nil(t, release)
	acc.Extra["native_relay_large_body_queue"] = true
	for _, size := range []int{nativeLargeBodyMin - 1, nativeLargeBodyMax + 1} {
		release, err = s.acquireNativeIngress(c, acc, size)
		require.NoError(t, err)
		require.Nil(t, release)
	}
	lane := newNativeIngressLane()
	s.nativeIngressLanes.Store(acc.ID, lane)
	require.True(t, lane.slots.TryAcquire(2))
	defer lane.slots.Release(2)
	waiters := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { _, err := lane.acquire(ctx, time.Minute); waiters <- err }()
	}
	require.Eventually(t, func() bool { return len(lane.waiters) == 2 }, time.Second, time.Millisecond)
	release, err = s.acquireNativeIngress(c, acc, nativeLargeBodyMax)
	require.ErrorIs(t, err, errNativeIngressBusy)
	require.Nil(t, release)
	require.Equal(t, 429, rec.Code)
	require.Equal(t, "1", rec.Header().Get("Retry-After"))
	require.Contains(t, rec.Body.String(), "native_ingress_busy")
	other := *acc
	other.ID = acc.ID + 1
	release, err = s.acquireNativeIngress(c, &other, nativeLargeBodyMin)
	require.NoError(t, err)
	require.NotNil(t, release)
	release()
	cancel()
	for i := 0; i < 2; i++ {
		require.ErrorIs(t, <-waiters, context.Canceled)
	}
	_, err = lane.acquire(context.Background(), time.Millisecond)
	require.ErrorIs(t, err, errNativeIngressBusy)
	require.Empty(t, lane.waiters)
}
