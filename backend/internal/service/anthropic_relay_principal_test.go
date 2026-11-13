//go:build unit

package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestAnthropicRelayPrincipalIgnoresClientIdentityAndOverrides(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
	c.Request.Header.Set("X-Getoken-User-Id", "999")
	c.Request.Header.Set("X-Native-Relay-Principal", "spoof")
	c.Set("api_key", &APIKey{ID: 8, UserID: 9})
	a := &Account{Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Extra: map[string]any{"anthropic_relay_session_scope": true}}
	h := http.Header{"x-native-relay-principal": {"override"}, "X-Native-Relay-Principal": {"spoof"}}
	require.NoError(t, applyAnthropicRelayPrincipal(context.Background(), c, a, h))
	require.Equal(t, openAIRelayPrincipal(context.Background(), 9, 8), h.Get("X-Native-Relay-Principal"))
	require.Len(t, h, 1)
	c.Set("api_key", &APIKey{ID: 8, UserID: 10})
	other := make(http.Header)
	require.NoError(t, applyAnthropicRelayPrincipal(context.Background(), c, a, other))
	require.NotEqual(t, h.Get("X-Native-Relay-Principal"), other.Get("X-Native-Relay-Principal"))
}

func TestAnthropicRelayPrincipalBuildersUseVerifiedTransitIdentity(t *testing.T) {
	svc := &GatewayService{cfg: &config.Config{}}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
	c.Set("api_key", &APIKey{ID: 8, UserID: 9})
	a := &Account{Platform: PlatformAnthropic, Type: AccountTypeAPIKey,
		Credentials: map[string]any{"api_key": "fixture", "base_url": "https://private.example.com"},
		Extra:       map[string]any{"anthropic_relay_session_scope": true}}
	body := []byte(`{"model":"claude-opus-5-5","max_tokens":32,"messages":[{"role":"user","content":"fixture"}]}`)
	seen := make(map[string]bool)
	for _, tag := range []string{"getoken:34", "getoken:42"} {
		ctx := WithAccountAffinityTags(context.Background(), []string{tag})
		first, _, err := svc.buildUpstreamRequest(ctx, c, a, body, "fixture", "apikey", "claude-opus-5-5", false, false)
		require.NoError(t, err)
		second, _, err := svc.buildUpstreamRequestAnthropicAPIKeyPassthrough(ctx, c, a, body, "fixture")
		require.NoError(t, err)
		principal := first.Header.Get("X-Native-Relay-Principal")
		require.Len(t, principal, 64)
		require.Equal(t, principal, second.Header.Get("X-Native-Relay-Principal"))
		require.False(t, seen[principal], "different Getoken users must not share a native tenant")
		seen[principal] = true
	}
}

func TestAnthropicRelayPrincipalRequiresOwnerOnlyWhenEnabled(t *testing.T) {
	a := &Account{Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Extra: map[string]any{"anthropic_relay_session_scope": true}}
	h := http.Header{"X-Native-Relay-Principal": {"spoof"}}
	require.Error(t, applyAnthropicRelayPrincipal(context.Background(), nil, a, h))
	require.Empty(t, h)
	a.Extra["anthropic_relay_session_scope"] = false
	h.Set("X-Native-Relay-Principal", "spoof")
	require.NoError(t, applyAnthropicRelayPrincipal(context.Background(), nil, a, h))
	require.Empty(t, h)
}
func TestAnthropicRelaySharedKeyRequiresVerifiedTag(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
	c.Request.Header.Set(AccountAffinityHeader, "101")
	c.Set("api_key", &APIKey{ID: 8, UserID: 9})
	a := &Account{Platform: PlatformAnthropic, Type: AccountTypeAPIKey,
		Extra: map[string]any{"anthropic_relay_session_scope": true, "anthropic_relay_require_affinity": true}}
	h := make(http.Header)
	require.ErrorContains(t, applyAnthropicRelayPrincipal(context.Background(), c, a, h), "verified transit identity")
	require.Empty(t, h)
	ctx := WithAccountAffinityTags(context.Background(), []string{"getoken:101"})
	require.NoError(t, applyAnthropicRelayPrincipal(ctx, c, a, h))
	require.Equal(t, openAIRelayPrincipal(ctx, 9, 8), h.Get("X-Native-Relay-Principal"))
}
