//go:build unit

package service

import (
	"io"
	"net/http"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestAccountTestService_AnthropicNativeRelayProbe(t *testing.T) {
	const header = "X-Native-Relay-Principal"
	for _, enabled := range []bool{true, false} {
		account := &Account{
			ID: 7, Platform: PlatformAnthropic, Type: AccountTypeAPIKey,
			Credentials: map[string]any{"base_url": "https://relay.example", "api_key": "fixture"},
			Extra:       map[string]any{"anthropic_relay_session_scope": enabled, "anthropic_relay_require_affinity": enabled},
		}
		if enabled {
			account.Credentials[credKeyHeaderOverrideEnabled] = true
			account.Credentials[credKeyHeaderOverrides] = map[string]any{
				"x-native-relay-principal": "fixed-account-override",
				"x-request-id":             "fixed-account-override",
			}
		}
		var principals []string
		for range 2 {
			upstream := &queuedHTTPUpstream{responses: []*http.Response{newJSONResponse(
				http.StatusOK, "data: {\"type\":\"message_stop\"}\n\n",
			)}}
			svc := &AccountTestService{
				accountRepo:  &mockAccountRepoForGemini{accountsByID: map[int64]*Account{account.ID: account}},
				httpUpstream: upstream, cfg: &config.Config{},
				tlsFPProfileService: &TLSFingerprintProfileService{},
			}
			c, response := newTestContext()
			c.Request.Header.Set(header, "caller-controlled")
			require.NoError(t, svc.TestAccountConnection(c, account.ID, "claude-opus-5-5", "", ""))
			require.Contains(t, response.Body.String(), "test_complete")
			require.Len(t, upstream.requests, 1)
			request := upstream.requests[0]
			principal := request.Header.Get(header)
			if !enabled {
				require.Empty(t, principal)
				continue
			}
			require.Regexp(t, "^[a-f0-9]{64}$", principal)
			require.Equal(t, []string{principal}, request.Header.Values(header))
			require.NotEqual(t, openAIRelayPrincipal(c.Request.Context(), 1, 230), principal)
			require.Regexp(t, "^[0-9a-f-]{36}$", request.Header.Get("X-Request-Id"))
			raw, err := io.ReadAll(request.Body)
			require.NoError(t, err)
			require.Contains(t, string(raw), "\"max_tokens\":64")
			require.NotContains(t, string(raw), "\"temperature\"")
			principals = append(principals, principal)
		}
		if enabled {
			require.NotEqual(t, principals[0], principals[1])
		}
	}
}
