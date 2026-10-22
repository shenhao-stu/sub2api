package handler

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestPolicyBlockedPoolDoesNotReenterAfterSelectionExhaustion(t *testing.T) {
	state := NewFailoverState(10, true)
	failure := &service.UpstreamFailoverError{StatusCode: http.StatusServiceUnavailable, ExcludeFromRetry: true}
	unscheduler := &mockTempUnscheduler{}
	require.Equal(t, FailoverContinue, state.HandleFailoverError(context.Background(), unscheduler, 50, service.PlatformAnthropic, 3, failure))
	require.Contains(t, state.FailedAccountIDs, int64(50))
	require.NotContains(t, state.FailedAccountIDs, int64(51), "a healthy independent account remains eligible")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.Equal(t, FailoverExhausted, state.HandleSelectionExhausted(ctx), "a policy-blocked 503 must not back off, clear exclusions and try the same pool 11 times")
	require.Contains(t, state.FailedAccountIDs, int64(50))
	require.Empty(t, unscheduler.calls)
}

func TestPolicyExclusionSurvivesAnotherAccountsTransient503(t *testing.T) {
	state := NewFailoverState(10, false)
	unscheduler := &mockTempUnscheduler{}
	blocked := &service.UpstreamFailoverError{StatusCode: 503, ExcludeFromRetry: true}
	transient := &service.UpstreamFailoverError{StatusCode: 503}
	require.Equal(t, FailoverContinue, state.HandleFailoverError(context.Background(), unscheduler, 50, service.PlatformAnthropic, 0, blocked))
	require.Equal(t, FailoverContinue, state.HandleFailoverError(context.Background(), unscheduler, 51, service.PlatformAnthropic, 0, transient))
	require.Equal(t, FailoverContinue, state.HandleSelectionExhausted(context.Background()))
	require.Contains(t, state.FailedAccountIDs, int64(50), "a later transient error must not undo an earlier policy block")
	require.NotContains(t, state.FailedAccountIDs, int64(51), "ordinary capacity retry is preserved")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.Equal(t, FailoverExhausted, state.HandleSelectionExhausted(ctx), "a failed reselection cannot spin without a new upstream attempt")
}
