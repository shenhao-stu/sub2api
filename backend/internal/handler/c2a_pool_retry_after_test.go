package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestC2APoolConcurrencyDelayCancellationDoesNotSwitch(t *testing.T) {
	for _, alreadyCanceled := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		if alreadyCanceled {
			cancel()
		} else {
			defer time.AfterFunc(10*time.Millisecond, cancel).Stop()
		}
		state := NewFailoverState(10, true)
		unscheduler := &mockTempUnscheduler{}
		failure := &service.UpstreamFailoverError{StatusCode: 429, RetryableOnSameAccount: true, SameAccountRetryDelay: 3 * time.Second}
		started := time.Now()
		action := state.HandleFailoverError(ctx, unscheduler, 465, service.PlatformOpenAI, 10, failure)
		require.Equal(t, FailoverCanceled, action)
		require.Less(t, time.Since(started), time.Second)
		require.Zero(t, state.SwitchCount)
		require.Empty(t, state.FailedAccountIDs)
		require.Empty(t, unscheduler.calls)
		require.False(t, state.ForceCacheBilling)
		cancel()
	}
}

func TestC2APoolConcurrencyOverBudgetStopsBeforeAnotherAccount(t *testing.T) {
	state := NewFailoverState(10, true)
	unscheduler := &mockTempUnscheduler{}
	failure := &service.UpstreamFailoverError{StatusCode: 429, NextAccountAction: service.NextAccountStop,
		ResponseHeaders: http.Header{"Retry-After": {"30"}}, ResponseBody: []byte(`{"error":{"type":"concurrency","message":"synthetic pool busy"}}`)}
	require.Equal(t, FailoverExhausted, state.HandleFailoverError(context.Background(), unscheduler, 465, service.PlatformOpenAI, 10, failure))
	require.Zero(t, state.SwitchCount)
	require.Empty(t, state.FailedAccountIDs)
	require.Empty(t, state.SameAccountRetryCount)
	require.Empty(t, unscheduler.calls)
	require.False(t, state.ForceCacheBilling)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	(&OpenAIGatewayHandler{}).handleFailoverExhausted(c, state.LastFailoverErr, false)
	require.Equal(t, 429, recorder.Code)
	require.Equal(t, "30", recorder.Header().Get("Retry-After"))
}

func TestC2APoolConcurrencyRetryBudgetAndOrdinaryRotationRemainBounded(t *testing.T) {
	failure := &service.UpstreamFailoverError{StatusCode: 429, RetryableOnSameAccount: true, SameAccountRetryDelay: 3 * time.Second}
	require.Equal(t, 3*time.Second, sameAccountRetryDelayFor(failure, 1))
	require.True(t, sameAccountRetryAllowed(failure, 9, 10))
	require.False(t, sameAccountRetryAllowed(failure, 10, 10))
	require.False(t, sameAccountRetryAllowed(failure, 0, 0))
	state := NewFailoverState(2, false)
	state.SameAccountRetryCount[465] = 10
	// The existing 429 cleanup path does not change account state.
	unscheduler := &service.GatewayService{}
	require.Equal(t, FailoverContinue, state.HandleFailoverError(context.Background(), unscheduler, 465, service.PlatformOpenAI, 10, failure))
	require.Equal(t, 1, state.SwitchCount)
	require.Contains(t, state.FailedAccountIDs, int64(465))
	require.False(t, state.ForceCacheBilling)
}
