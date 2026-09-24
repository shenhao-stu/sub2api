//go:build unit

package service

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestGrokCompactStateStrictClientRoundTrip(t *testing.T) {
	svc := &OpenAIGatewayService{cfg: &config.Config{JWT: config.JWTConfig{Secret: strings.Repeat("test-key-", 4)}}}
	response := []byte(`{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"The project color is blue. README remains pending."}]}],"usage":{"input_tokens":10,"output_tokens":12}}`)
	compacted, err := svc.convertGrokCompactResponse(response)
	require.NoError(t, err)
	blob := gjson.GetBytes(compacted, "output.0.encrypted_content").String()
	require.NotContains(t, blob, "blue")
	require.Equal(t, int64(12), gjson.GetBytes(compacted, "usage.output_tokens").Int())
	// Strict client retains no gateway-specific summary fields.
	request, err := json.Marshal(map[string]any{"input": []any{
		map[string]any{"type": "compaction", "encrypted_content": blob},
		map[string]any{"type": "function_call_output", "call_id": "tool1", "output": json.Number("9007199254740993")},
		map[string]any{"role": "user", "content": "What color?"},
	}})
	require.NoError(t, err)
	restored, err := svc.restoreGrokCompactState(request)
	require.NoError(t, err)
	require.Contains(t, gjson.GetBytes(restored, "input.0.content.0.text").String(), "color is blue")
	require.Equal(t, "9007199254740993", gjson.GetBytes(restored, "input.1.output").Raw)
	require.Equal(t, "What color?", gjson.GetBytes(restored, "input.2.content").String())
	require.NotContains(t, string(restored), grokCompactStatePrefix)
	// The restored history remains usable as input to the next compaction.
	next, err := buildGrokCompactRequestBody(restored)
	require.NoError(t, err)
	require.Contains(t, string(next), "README remains pending")
	second, err := svc.sealGrokCompactSummary("same summary")
	require.NoError(t, err)
	third, err := svc.sealGrokCompactSummary("same summary")
	require.NoError(t, err)
	require.NotEqual(t, second, third, "each state uses a fresh nonce")
	other := &OpenAIGatewayService{cfg: &config.Config{JWT: config.JWTConfig{Secret: strings.Repeat("otherkey", 4)}}}
	_, err = other.openGrokCompactSummary(blob)
	require.Error(t, err)
}

func TestGrokCompactStateRejectsTamperingBeforeUpstream(t *testing.T) {
	svc := &OpenAIGatewayService{cfg: &config.Config{JWT: config.JWTConfig{Secret: strings.Repeat("test-key-", 4)}}}
	blob, err := svc.sealGrokCompactSummary("real summary")
	require.NoError(t, err)
	i := len(grokCompactStatePrefix) + 20
	replacement := byte('A')
	if blob[i] == replacement {
		replacement = 'B'
	}
	blob = blob[:i] + string(replacement) + blob[i+1:]
	body, err := json.Marshal(map[string]any{"model": "grok-4.7", "input": []any{map[string]any{"type": "compaction", "encrypted_content": blob}}})
	require.NoError(t, err)
	gin.SetMode(gin.TestMode)
	r := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(r)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
	result, err := svc.forwardGrokResponses(context.Background(), c, &Account{Platform: PlatformGrok, Type: AccountTypeAPIKey}, body, "grok-4.7", false, time.Now())
	require.Error(t, err)
	require.Nil(t, result)
	require.Equal(t, http.StatusBadRequest, r.Code)
	_, err = svc.openGrokCompactSummary(grokCompactStatePrefix + "bad")
	require.Error(t, err)
	_, err = svc.sealGrokCompactSummary(strings.Repeat("x", grokCompactStateLimit+1))
	require.Error(t, err)
}

func TestGrokNativeCompactSkipsCacheToolInjection(t *testing.T) {
	c := newGrokCacheTestContext(701)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	MarkOpenAINativeCompactionV2(c)
	body := []byte(`{"model":"grok-4.7","input":"compact this","prompt_cache_key":"client-key"}`)
	identity := resolveGrokCacheIdentity(c, body, "", "grok-4.7")
	require.Empty(t, identity)
	out, err := applyGrokResponsesCacheIdentity(body, body, identity, true)
	require.NoError(t, err)
	require.False(t, gjson.GetBytes(out, "tools").Exists())
	require.False(t, gjson.GetBytes(out, "prompt_cache_key").Exists())
}
