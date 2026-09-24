//go:build unit

package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestForwardGrokResponsesRetainsKnownUsageAfterCommittedFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const usageEvent = `data: {"type":"response.in_progress","response":{"id":"resp_partial_usage","usage":{"input_tokens":100,"output_tokens":7,"input_tokens_details":{"cached_tokens":40}}}}` + "\n\n"
	const visibleEvent = `data: {"type":"response.output_text.delta","delta":"visible output"}` + "\n\n"
	const failedEvent = `data: {"type":"response.failed","response":{"id":"resp_partial_usage","error":{"code":"server_error","message":"upstream stopped"},"usage":{"input_tokens":100,"output_tokens":7,"input_tokens_details":{"cached_tokens":40}}}}` + "\n\n"
	for _, tt := range []struct {
		name string
		body string
	}{
		{name: "terminal_failure_with_authoritative_usage", body: visibleEvent + failedEvent},
		{name: "eof_after_reported_usage", body: visibleEvent + usageEvent},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result, err, recorder := forwardGrokBillingStream(t, tt.body)
			require.Error(t, err)
			var failoverErr *UpstreamFailoverError
			require.False(t, errors.As(err, &failoverErr), "already delivered output must never replay")
			require.Contains(t, recorder.Body.String(), "visible output")
			require.NotNil(t, result, "known partial usage must reach the handler's existing error billing path")
			require.Equal(t, 100, result.Usage.InputTokens)
			require.Equal(t, 7, result.Usage.OutputTokens)
			require.Equal(t, 40, result.Usage.CacheReadInputTokens)
			require.Equal(t, "partial-usage-request", result.RequestID)
			require.Equal(t, "resp_partial_usage", result.ResponseID)
		})
	}
}

func TestForwardGrokResponsesPreOutputFailoverDoesNotCreateCustomerBill(t *testing.T) {
	gin.SetMode(gin.TestMode)
	result, err, recorder := forwardGrokBillingStream(t,
		`data: {"type":"response.failed","response":{"error":{"code":"rate_limit_exceeded","message":"Concurrency limit exceeded for account, please retry later"},"usage":{"input_tokens":0,"output_tokens":0}}}`+"\n\n")
	var failoverErr *UpstreamFailoverError
	require.ErrorAs(t, err, &failoverErr)
	require.Nil(t, result, "a failed attempt must not collide with the final attempt's customer billing key")
	require.Empty(t, recorder.Body.String())
}

func TestForwardGrokResponsesCanceledClientStillCollectsUsage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err, _ := forwardGrokBillingStream(t,
		`data: {"type":"response.output_text.delta","delta":"visible output"}`+"\n\n"+
			`data: {"type":"response.completed","response":{"id":"resp_after_cancel","status":"completed","output":[],"usage":{"input_tokens":12,"output_tokens":5}}}`+"\n\n",
		func(c *gin.Context) { c.Request = c.Request.WithContext(ctx) })
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 12, result.Usage.InputTokens)
	require.Equal(t, 5, result.Usage.OutputTokens)
}

func TestForwardGrokResponsesDoesNotInventUsageAfterCommittedFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	result, err, _ := forwardGrokBillingStream(t,
		`data: {"type":"response.output_text.delta","delta":"visible output"}`+"\n\n")
	require.Error(t, err)
	var failoverErr *UpstreamFailoverError
	require.False(t, errors.As(err, &failoverErr))
	require.NotNil(t, result)
	require.Equal(t, OpenAIUsage{}, result.Usage)
}

func forwardGrokBillingStream(t *testing.T, upstreamBody string, configure ...func(*gin.Context)) (*OpenAIForwardResult, error, *httptest.ResponseRecorder) {
	t.Helper()
	requestBody := []byte(`{"model":"grok-4.6","input":"test","stream":true}`)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	for _, apply := range configure {
		apply(c)
	}
	upstream := &httpUpstreamRecorder{responses: []*http.Response{{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "X-Request-Id": []string{"partial-usage-request"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	account := &Account{
		ID: 923001, Platform: PlatformGrok, Type: AccountTypeAPIKey, Concurrency: 1,
		Credentials: map[string]any{"api_key": "test-only-token", "base_url": "https://api.x.ai/v1"},
	}
	result, err := svc.forwardGrokResponses(c.Request.Context(), c, account, requestBody, "grok-4.6", true, time.Now())
	require.Len(t, upstream.requests, 1)
	require.NoError(t, upstream.requests[0].Context().Err(), "client cancellation must not discard upstream usage")
	return result, err, recorder
}
