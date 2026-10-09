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
			account := commandCodePolicyTestAccount(provider == CommandCodeGoProvider)
			if provider == "custom" {
				account.Platform = PlatformOpenAI
			}
			account.Credentials["custom_error_codes_enabled"] = true
			account.Credentials["custom_error_codes"] = []any{float64(401)}
			repo := &rateLimitAccountRepoStub{}
			limiter := NewRateLimitService(repo, nil, rawChatCompletionsTestConfig(), nil, nil)
			blocker := &runtimeBlockRecorder{}
			limiter.SetAccountRuntimeBlocker(blocker)
			disabled := limiter.HandleUpstreamError(context.Background(), account, 400, nil, body)
			expected := provider != "custom"
			require.Equal(t, expected, disabled)
			require.Equal(t, expected, (&OpenAIGatewayService{}).shouldFailoverOpenAIUpstreamResponse(account, 400, "", body))
			if expected {
				require.Equal(t, []string{"cn_insufficient_balance"}, blocker.reasons)
				require.Equal(t, 1, repo.tempCalls)
				require.NotContains(t, repo.lastTempReason, "private")
				require.Zero(t, repo.setErrorCalls)
			} else {
				require.Zero(t, repo.tempCalls)
			}
		})
	}
}

func TestCommandCodeGoCreditErrorRemainsFailoverBeforeClientOutput(t *testing.T) {
	for _, endpoint := range []string{"chat", "responses", "messages"} {
		t.Run(endpoint, func(t *testing.T) {
			account := commandCodePolicyTestAccount(true)
			repo := &rateLimitAccountRepoStub{}
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
			require.Equal(t, 1, repo.tempCalls)
		})
	}
}
