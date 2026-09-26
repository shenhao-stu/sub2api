//go:build unit

package service

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestForwardOpenAINativeRetainsKnownPartialUsage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tt := range []struct{ name, terminal string }{
		{"failed", `data: {"type":"response.failed","response":{"id":"resp_partial","error":{"code":"server_error","message":"upstream stopped"},"usage":{"input_tokens":100,"output_tokens":7}}}` + "\n\n"},
		{"eof", `data: {"type":"response.in_progress","response":{"id":"resp_partial","usage":{"input_tokens":100,"output_tokens":7}}}` + "\n\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result, err, recorder := forwardOpenAINativeBillingStream(t,
				`data: {"type":"response.output_text.delta","delta":"visible output"}`+"\n\n"+tt.terminal)
			require.Error(t, err)
			var failoverErr *UpstreamFailoverError
			require.False(t, errors.As(err, &failoverErr))
			require.Contains(t, recorder.Body.String(), "visible output")
			require.NotNil(t, result)
			require.Equal(t, 100, result.Usage.InputTokens)
			require.Equal(t, 7, result.Usage.OutputTokens)
			require.Equal(t, "resp_partial", result.ResponseID)
		})
	}
}

func TestForwardOpenAINativePreOutputFailoverReturnsNoBillableResult(t *testing.T) {
	gin.SetMode(gin.TestMode)
	result, err, recorder := forwardOpenAINativeBillingStream(t,
		`data: {"type":"response.failed","response":{"error":{"code":"server_error","message":"upstream stopped"},"usage":{"input_tokens":0,"output_tokens":0}}}`+"\n\n")
	var failoverErr *UpstreamFailoverError
	require.ErrorAs(t, err, &failoverErr)
	require.Nil(t, result)
	require.Empty(t, recorder.Body.String())
}

func TestForwardOpenAINativePreOutputMeteredFailureRetainsInputUsage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	result, err, recorder := forwardOpenAINativeBillingStream(t,
		`data: {"type":"response.failed","response":{"id":"resp_metered_failure","error":{"code":"server_error","message":"upstream stopped"},"usage":{"input_tokens":100,"output_tokens":0}}}`+"\n\n")
	require.Error(t, err)
	var failoverErr *UpstreamFailoverError
	require.False(t, errors.As(err, &failoverErr))
	require.NotNil(t, result)
	require.Equal(t, 100, result.Usage.InputTokens)
	require.Zero(t, result.Usage.OutputTokens)
	require.Equal(t, "resp_metered_failure", result.ResponseID)
	require.Contains(t, recorder.Body.String(), "response.failed")
	require.NotContains(t, recorder.Body.String(), "response.completed")
}

func TestForwardOpenAINativeCompletedBindsResponseAccount(t *testing.T) {
	gin.SetMode(gin.TestMode)
	result, err, _ := forwardOpenAINativeBillingStream(t,
		`data: {"type":"response.completed","response":{"id":"resp_completed","usage":{"input_tokens":100,"output_tokens":7}}}`+"\n\n")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "resp_completed", result.ResponseID)
}

func forwardOpenAINativeBillingStream(t *testing.T, upstreamBody string) (*OpenAIForwardResult, error, *httptest.ResponseRecorder) {
	t.Helper()
	requestBody := []byte(`{"model":"gpt-5.4","input":"test","stream":true}`)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	groupID := int64(232)
	c.Set("api_key", &APIKey{ID: 501, GroupID: &groupID})
	SetOpenAIHTTPResponseOwner(c, 601, 501)
	upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: http.StatusOK,
		Header: http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:   io.NopCloser(strings.NewReader(upstreamBody))}}
	cache := &responseBindContextProbeCache{}
	svc := &OpenAIGatewayService{billingService: newLocalFixtureBilling(), cfg: &config.Config{}, httpUpstream: upstream, cache: cache}
	account := &Account{ID: 923002, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Concurrency: 1,
		Credentials: map[string]any{"api_key": "test-only-token"}}
	result, err := svc.Forward(c.Request.Context(), c, account, requestBody)
	require.Len(t, upstream.requests, 1)
	if err != nil {
		require.Empty(t, cache.sessionBindings, "failed responses must not acquire successful response affinity")
		require.Empty(t, cache.setContextErrors)
	} else {
		require.Len(t, cache.sessionBindings, 3)
		require.Contains(t, cache.sessionBindings, openAIWSResponseAccountCacheKey(result.ResponseID))
	}
	return result, err, recorder
}
