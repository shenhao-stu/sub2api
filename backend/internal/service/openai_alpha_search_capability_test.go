//go:build unit

package service

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUnsupportedAlphaSearchDoesNotDisableChatOrRetryPool(t *testing.T) {
	for _, status := range []int{404, 405, 500, 501} {
		account := &Account{ID: 32, Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
			Credentials: map[string]any{"base_url": "https://relay.example", "api_key": "test", "pool_mode": true, "pool_mode_retry_count": 10}}
		repo := &rateLimitAccountRepoStub{}
		upstream := &httpUpstreamRecorder{resp: newJSONResponse(status, `{"error":{"message":"channel does not support /v1/alpha/search"}}`)}
		svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig(), accountRepo: repo, httpUpstream: upstream}
		c, _ := newTestContext()
		result, err := svc.ForwardAlphaSearch(context.Background(), c, account, []byte(`{"model":"gpt-6.1-sol","commands":{"search_query":[{"q":"test"}]}}`))
		var failover *UpstreamFailoverError
		require.ErrorAs(t, err, &failover)
		require.Nil(t, result, "unsupported searches are not billable")
		require.False(t, failover.RetryableOnSameAccount)
		require.Zero(t, repo.setErrorCalls)
		require.Zero(t, repo.tempCalls)
		account.Extra = repo.lastExtraUpdates
		require.False(t, account.SupportsOpenAIEndpointCapability(OpenAIEndpointCapabilityAlphaSearch))
		require.True(t, account.SupportsOpenAIEndpointCapability(OpenAIEndpointCapabilityChatCompletions))
		require.False(t, account.alphaSearchUnavailable(time.Now().Add(2*time.Hour)))
		account.Credentials["base_url"] = "https://new-relay.example"
		require.True(t, account.SupportsOpenAIEndpointCapability(OpenAIEndpointCapabilityAlphaSearch))
	}
}

func TestAlphaSearchDoesNotLearnFromModelAuthOrTransientErrors(t *testing.T) {
	a := &Account{Type: AccountTypeAPIKey}
	for _, tc := range []struct {
		status int
		body   string
	}{
		{404, `{"error":{"message":"model not found"}}`},
		{500, `{"error":{"message":"temporary failure"}}`},
		{http.StatusUnauthorized, `{"error":{"message":"Not found"}}`},
		{400, `{"error":{"message":"channel does not support /v1/alpha/search"}}`},
	} {
		require.False(t, isOpenAIAlphaSearchEndpointUnsupported(a, tc.status, []byte(tc.body)))
	}
}

func TestWalletExhaustionOverridesPoolWithoutPermanentDisable(t *testing.T) {
	for _, platform := range []string{PlatformOpenAI, PlatformAnthropic} {
		a := &Account{ID: 5, Platform: platform, Type: AccountTypeAPIKey, Credentials: map[string]any{"pool_mode": true}}
		repo := &upstreamQuotaRepo{account: a}
		svc := NewRateLimitService(repo, nil, rawChatCompletionsTestConfig(), nil, nil)
		body := []byte(`{"error":{"code":"insufficient_quota","message":"Insufficient account balance"}}`)
		require.True(t, svc.HandleUpstreamError(context.Background(), a, 429, nil, body))
		require.Equal(t, 1, repo.atomicCalls)
		require.Zero(t, repo.tempCalls)
		require.NotNil(t, a.RateLimitResetAt)
		require.Zero(t, repo.setErrorCalls)
		require.False(t, isAPIKeyWalletExhausted(a, 400, body))
		require.False(t, isAPIKeyWalletExhausted(a, 429, []byte(`{"error":{"code":"rate_limit_exceeded","message":"Insufficient account balance"}}`)))
		require.False(t, isAPIKeyWalletExhausted(a, 429, []byte(`{"error":{"code":"insufficient_quota","message":"Try again later"}}`)))
	}
}
