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

func TestGrokOpaqueForbiddenDoesNotDrainPool(t *testing.T) {
	for _, body := range []string{"", "{}", `{"error":{}}`, `{"code":"permission_denied"}`} {
		t.Run(body, func(t *testing.T) {
			repo := &grokQuotaAccountRepo{}
			svc := &OpenAIGatewayService{accountRepo: repo}
			account := &Account{ID: 24301, Platform: PlatformGrok, Type: AccountTypeOAuth}
			svc.handleGrokAccountUpstreamError(context.Background(), account, http.StatusForbidden, nil, []byte(body))
			require.Zero(t, repo.tempUnschedCalls)
			require.Zero(t, repo.rateLimitedCalls)
			require.False(t, svc.isOpenAIAccountRuntimeBlocked(account))
			require.False(t, svc.shouldFailoverGrokUpstreamError(http.StatusForbidden, []byte(body)))
			require.False(t, svc.handleOpenAIAccountUpstreamError(context.Background(), account, http.StatusForbidden, nil, []byte(body), "grok-4.7"))
			require.Nil(t, account.TempUnschedulableUntil)
		})
	}
}

func TestGrokOpaqueForbiddenForwardingRemainsError(t *testing.T) {
	for _, protocol := range []string{"responses", "chat"} {
		t.Run(protocol, func(t *testing.T) {
			repo := &grokQuotaAccountRepo{}
			upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: http.StatusForbidden, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}}
			svc := &OpenAIGatewayService{accountRepo: repo, httpUpstream: upstream}
			account := &Account{ID: 24302, Platform: PlatformGrok, Type: AccountTypeAPIKey, Credentials: map[string]any{"api_key": "test-only", "base_url": "https://xai.test/v1"}}
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			var result *OpenAIForwardResult
			var err error
			if protocol == "responses" {
				result, err = svc.forwardGrokResponses(context.Background(), c, account, []byte(`{"model":"grok-4.7","input":"hi","stream":true}`), "grok-4.7", true, time.Now())
			} else {
				result, err = svc.forwardGrokChatCompletionsViaResponses(context.Background(), c, account, []byte(`{"model":"grok-4.7","messages":[{"role":"user","content":"hi"}],"stream":true}`), "session", "")
			}
			require.Error(t, err)
			var failover *UpstreamFailoverError
			require.False(t, errors.As(err, &failover))
			require.Nil(t, result)
			status := http.StatusBadGateway
			if protocol == "chat" {
				status = http.StatusForbidden
			}
			require.Equal(t, status, w.Code)
			require.Contains(t, w.Body.String(), "error")
			require.Zero(t, repo.tempUnschedCalls)
			require.Zero(t, repo.rateLimitedCalls)
			require.False(t, svc.isOpenAIAccountRuntimeBlocked(account))
		})
	}
}

func TestGrokOpaqueForbiddenPreservesExplicitFailureScopes(t *testing.T) {
	for _, body := range []string{
		`{"error":{"code":"content_filter"}}`,
		`{"code":"subscription_required"}`,
		`{"code":"personal-team-blocked:spending-limit"}`,
		`{"error":{"code":"invalid_api_key"}}`,
		`{"error":{"message":"subscription required"}}`,
		`{"error":{"message":"account suspended"}}`,
		`{"message":"The model is at capacity"}`,
	} {
		t.Run(body, func(t *testing.T) {
			require.False(t, isGrokOpaqueForbidden(http.StatusForbidden, []byte(body)))
		})
	}
	for _, status := range []int{http.StatusUnauthorized, http.StatusPaymentRequired, http.StatusTooManyRequests, http.StatusBadGateway} {
		require.False(t, isGrokOpaqueForbidden(status, nil))
	}
}

func TestGrokOpaqueForbiddenRawChatFailover(t *testing.T) {
	svc := &OpenAIGatewayService{}
	grok := &Account{Platform: PlatformGrok, Type: AccountTypeOAuth}
	openai := &Account{Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	require.False(t, svc.shouldFailoverOpenAIUpstreamResponse(grok, http.StatusForbidden, "", nil))
	require.True(t, svc.shouldFailoverOpenAIUpstreamResponse(openai, http.StatusForbidden, "", nil))
	require.True(t, svc.shouldFailoverOpenAIUpstreamResponse(grok, http.StatusForbidden, "subscription required", []byte(`{"error":{"message":"subscription required"}}`)))
}
