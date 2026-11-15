//go:build unit

package service

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type openAIImagesBalanceRepo struct {
	AccountRepository
	scope  string
	reason string
	until  time.Time
}

func (r *openAIImagesBalanceRepo) SetModelRateLimit(_ context.Context, _ int64, scope string, resetAt time.Time, reason ...string) error {
	r.scope = scope
	r.until = resetAt
	if len(reason) > 0 {
		r.reason = reason[0]
	}
	return nil
}

type openAIImagesBalanceUpstream struct {
	HTTPUpstream
	status int
	body   string
}

func (u openAIImagesBalanceUpstream) Do(*http.Request, string, int64, int) (*http.Response, error) {
	return &http.Response{
		StatusCode: u.status,
		Header: http.Header{
			"X-Request-Id": []string{"req-images-balance"},
			"Retry-After":  []string{"23"},
		},
		Body: io.NopCloser(bytes.NewBufferString(u.body)),
	}, nil
}

func TestForwardOpenAIImagesAPIKey_ClassifiesWrappedBalanceBeforeTransientFailover(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const wrappedBalance = `{"error":{"message":"upstream request failed","cause":{"errorKey":"insufficient_balance","errorMessage":"Insufficient credit balance"}}}`
	const quotaRejection = `{"error":{"code":"insufficient_user_quota","message":"provider account balance is exhausted","type":"new_api_error"}}`
	for _, tc := range []struct {
		name     string
		endpoint string
		stream   bool
		status   int
		body     string
		scope    string
	}{
		{name: "wrapped balance generation", endpoint: openAIImagesGenerationsEndpoint, status: http.StatusBadGateway, body: wrappedBalance, scope: openAIImageGenerationRateLimitKey},
		{name: "wrapped balance stream edit", endpoint: openAIImagesEditsEndpoint, stream: true, status: http.StatusBadGateway, body: wrappedBalance, scope: openAIImageGenerationRateLimitKey},
		{name: "provider quota rejection", endpoint: openAIImagesEditsEndpoint, status: http.StatusForbidden, body: quotaRejection, scope: "gpt-image-1"},
		{name: "ambiguous 502 with quota code", endpoint: openAIImagesEditsEndpoint, status: http.StatusBadGateway, body: quotaRejection},
		{name: "quota code with generation evidence", endpoint: openAIImagesEditsEndpoint, status: http.StatusForbidden, body: `{"error":{"code":"insufficient_user_quota","type":"new_api_error"},"usage":{"total_tokens":1}}`},
		{name: "quota code inside echoed input", endpoint: openAIImagesEditsEndpoint, status: http.StatusForbidden, body: `{"error":{"type":"new_api_error","code":"invalid_value","message":"invalid request"},"request":{"error":{"code":"insufficient_user_quota","type":"new_api_error"}}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &openAIImagesBalanceRepo{}
			svc := &OpenAIGatewayService{accountRepo: repo, httpUpstream: openAIImagesBalanceUpstream{status: tc.status, body: tc.body}, cfg: &config.Config{}}
			account := &Account{
				ID: 7, Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
				Status: StatusActive, Schedulable: true,
				Credentials: map[string]any{"api_key": "test-key", "base_url": "https://images.example.test"},
			}
			body := []byte(`{"model":"gpt-image-1","prompt":"cat"}`)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/"+tc.endpoint, bytes.NewReader(body))
			parsed := &OpenAIImagesRequest{Model: "gpt-image-1", Endpoint: tc.endpoint, ContentType: "application/json", Stream: tc.stream, N: 1}

			_, err := svc.ForwardImages(context.Background(), c, account, body, parsed, "")
			if tc.scope == "" {
				var upstreamErr *OpenAIImagesUpstreamError
				require.ErrorAs(t, err, &upstreamErr)
				require.Equal(t, tc.status, upstreamErr.StatusCode)
				require.Empty(t, repo.scope)
				return
			}
			var failoverErr *UpstreamFailoverError
			require.ErrorAs(t, err, &failoverErr)
			require.Equal(t, "req-images-balance", failoverErr.ResponseHeaders.Get("X-Request-Id"))
			require.Equal(t, "23", failoverErr.ResponseHeaders.Get("Retry-After"))
			require.Equal(t, OpenAIImagesInsufficientBalanceReason, failoverErr.Reason)
			require.True(t, failoverErr.ShouldRetryNextAccount())
			require.Equal(t, OpenAIImagesInsufficientBalanceCode, string(failoverErr.Reason)[len("openai_images_"):])
			require.Equal(t, tc.scope, repo.scope)
			require.Equal(t, openAIImagesBalanceRateLimitReason, repo.reason)
			require.WithinDuration(t, time.Now().Add(openAIImagesBalanceCooldown), repo.until, 2*time.Second)
			require.True(t, account.isModelRateLimitedWithContext(context.Background(), "gpt-image-1"))
			if tc.scope != openAIImageGenerationRateLimitKey {
				require.False(t, account.isModelRateLimitedWithContext(context.Background(), "gpt-image-2"))
			}
			require.False(t, account.isModelRateLimitedWithContext(context.Background(), "gpt-5.5"), "text requests must remain schedulable")
		})
	}
}

func TestOpenAIImagesInsufficientBalanceDetectionRejectsUnstructuredAndEchoedText(t *testing.T) {
	require.True(t, isOpenAIImagesInsufficientBalance([]byte(`{"errorKey":"insufficient_balance"}`)))
	require.True(t, isOpenAIImagesInsufficientBalance([]byte(`{"error":{"cause":{"errorKey":"insufficient_balance"}}}`)))
	require.True(t, isOpenAIImagesInsufficientBalance([]byte(`{"error":{"details":[{"code":"insufficient_balance"}]}}`)))
	require.True(t, isOpenAIImagesInsufficientBalance([]byte(`{"error":{"inner_error":{"error_key":" INSUFFICIENT_BALANCE. "}}}`)))
	require.False(t, isOpenAIImagesInsufficientBalance([]byte(`{"message":"Insufficient credit balance"}`)))
	require.False(t, isOpenAIImagesInsufficientBalance([]byte(`{"detail":"Insufficient credit"}`)))
	require.False(t, isOpenAIImagesInsufficientBalance([]byte(`{"error":{"message":"Insufficient credit balance"}}`)))
	require.False(t, isOpenAIImagesInsufficientBalance([]byte(`{"error":{"details":[{"detail":"Insufficient credit"}]}}`)))
	require.False(t, isOpenAIImagesInsufficientBalance([]byte(`{"error":{"errorMessage":"Insufficient credit balance"}}`)))
	require.False(t, isOpenAIImagesInsufficientBalance([]byte(`{"error":{"message":"{\"errorKey\":\"insufficient_balance\"}"}}`)))
	require.False(t, isOpenAIImagesInsufficientBalance([]byte(`{"error":{"detail":"upstream returned 400 {\"errorKey\":\"insufficient_balance\"}"}}`)))
	require.False(t, isOpenAIImagesInsufficientBalance([]byte(`gateway says insufficient_balance`)))
	require.False(t, isOpenAIImagesInsufficientBalance([]byte(`{"request":{"prompt":"show the words insufficient_balance"}}`)))
	require.False(t, isOpenAIImagesInsufficientBalance([]byte(`{"request":{"errorKey":"insufficient_balance"}}`)))
	require.False(t, isOpenAIImagesInsufficientBalance([]byte(`{"message":"the prompt mentioned insufficient credit balance yesterday"}`)))
}
