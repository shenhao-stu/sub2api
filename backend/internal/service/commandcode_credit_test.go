//go:build unit

package service

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommandCodeCreditExhaustionStopsOnlySelectedProvider(t *testing.T) {
	body := []byte(`{"error":{"code":"BAD_REQUEST","message":"You have insufficient credits to make this request. private"}}`)
	for _, provider := range []string{CommandCodeGoProvider, CommandCodeProvider, "custom"} {
		t.Run(provider, func(t *testing.T) {
			account := commandCodeTestAccount(provider == CommandCodeGoProvider)
			account.Extra["provider"] = provider
			account.Credentials["custom_error_codes_enabled"] = true
			account.Credentials["custom_error_codes"] = []any{float64(401)}
			repo := &teamLinkedAccountRepoStub{}
			limiter := NewRateLimitService(repo, nil, rawChatCompletionsTestConfig(), nil, nil)
			blocker := &runtimeBlockRecorder{}
			limiter.SetAccountRuntimeBlocker(blocker)
			disabled := limiter.HandleUpstreamError(context.Background(), account, 400, nil, body)
			expected := provider != "custom"
			require.Equal(t, expected, disabled)
			require.Equal(t, expected, (&OpenAIGatewayService{}).shouldFailoverOpenAIUpstreamResponse(account, 400, "", body))
			if expected {
				require.Equal(t, []string{"commandcode_credit_exhausted"}, blocker.reasons)
				require.Equal(t, []int64{account.ID}, repo.setErrorIDs)
				require.NotContains(t, repo.setErrorMsgs[account.ID], "private")
			} else {
				require.Empty(t, repo.setErrorIDs)
			}
		})
	}
}

func TestCommandCodeGoCreditErrorRemainsFailoverBeforeClientOutput(t *testing.T) {
	for _, endpoint := range []string{"chat", "responses", "messages"} {
		t.Run(endpoint, func(t *testing.T) {
			account := commandCodeTestAccount(true)
			repo := &teamLinkedAccountRepoStub{}
			cfg := rawChatCompletionsTestConfig()
			limiter := NewRateLimitService(repo, nil, cfg, nil, nil)
			upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 400, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":{"code":"BAD_REQUEST","message":"You have insufficient credits to make this request. private"}}`))}}
			svc := &OpenAIGatewayService{cfg: cfg, httpUpstream: upstream, rateLimitService: limiter}
			c, rec := newTestContext()
			body := []byte(`{"model":"glm-5.3-flash","max_tokens":512,"messages":[{"role":"user","content":"hi"}]}`)
			var err error
			switch endpoint {
			case "responses":
				_, err = svc.Forward(context.Background(), c, account, []byte(`{"model":"glm-5.3-flash","input":"hi"}`))
			case "messages":
				_, err = svc.ForwardAsAnthropic(context.Background(), c, account, body, "", "")
			default:
				_, err = svc.ForwardAsChatCompletions(context.Background(), c, account, body, "", "")
			}
			var failover *UpstreamFailoverError
			require.ErrorAs(t, err, &failover)
			require.Equal(t, 400, failover.StatusCode)
			require.False(t, c.Writer.Written())
			require.Empty(t, rec.Body.String())
			require.Equal(t, []int64{account.ID}, repo.setErrorIDs)
		})
	}
}

func TestCommandCodeQuotaUsesSavedAccountTransport(t *testing.T) {
	account := commandCodeTestAccount(true)
	proxyID := int64(98)
	account.ProxyID, account.Proxy = &proxyID, &Proxy{ID: proxyID, Protocol: "socks5", Host: "proxy.invalid", Port: 1080}
	called := 0
	svc := &AccountTestService{httpUpstream: commandCodeTestUpstream(func(req *http.Request, proxy string, id int64, _ int) (*http.Response, error) {
		called++
		require.Equal(t, account.ID, id)
		require.Equal(t, account.Proxy.URL(), proxy)
		require.True(t, HTTPUpstreamRedirectsDisabled(req.Context()))
		require.Equal(t, "Bearer upstream-only-secret", req.Header.Get("Authorization"))
		require.Equal(t, http.MethodGet, req.Method)
		require.Equal(t, CommandCodeGoBaseURL+"/alpha/billing/credits", req.URL.String())
		return newJSONResponse(200, `{"credits":{"monthlyCredits":0}}`), nil
	})}
	quota, err := svc.GetCommandCodeQuota(context.Background(), account)
	require.NoError(t, err)
	require.Zero(t, *quota.Credits.MonthlyCredits)
	_, err = svc.GetCommandCodeQuota(context.Background(), commandCodeTestAccount(false))
	require.Error(t, err)
	require.Equal(t, 1, called)
}
