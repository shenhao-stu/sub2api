//go:build unit

package service

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCacheTTLActualUsageIgnoresLegacyOverride(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, split := range []string{``, `,"cache_creation":{"ephemeral_5m_input_tokens":7,"ephemeral_1h_input_tokens":13}`} {
			t.Run(strconv.FormatBool(stream)+split, func(t *testing.T) {
				svc := newMinimalGatewayService()
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
				account := &Account{ID: 1, Platform: PlatformAnthropic, Type: AccountTypeOAuth, Extra: map[string]any{"cache_ttl_override_enabled": true, "cache_ttl_override_target": "5m"}}
				u := `{"input_tokens":3,"output_tokens":2,"cache_creation_input_tokens":20` + split + `}`
				body := `{"id":"synthetic","type":"message","content":[{"type":"text","text":"OK"}],"usage":` + u + `}`
				if stream {
					body = "data: {\"type\":\"message_start\",\"message\":{\"usage\":" + u + "}}\n\ndata: {\"type\":\"message_stop\"}\n\n"
				}
				resp := &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}
				var usage *ClaudeUsage
				if stream {
					r, err := svc.handleStreamingResponse(context.Background(), resp, c, account, time.Now(), "claude-sonnet-4", "claude-sonnet-4", false)
					require.NoError(t, err)
					usage = r.usage
				} else {
					var err error
					usage, err = svc.handleNonStreamingResponse(context.Background(), resp, c, account, "claude-sonnet-4", "claude-sonnet-4")
					require.NoError(t, err)
				}
				require.Equal(t, 20, usage.CacheCreationInputTokens)
				if split == "" {
					require.Zero(t, usage.CacheCreation5mTokens)
					require.Zero(t, usage.CacheCreation1hTokens)
				} else {
					require.Equal(t, 7, usage.CacheCreation5mTokens)
					require.Equal(t, 13, usage.CacheCreation1hTokens)
				}
			})
		}
	}
}
func TestCacheTTLExplicitFiveMinutesPreserved(t *testing.T) {
	body := []byte(`{"model":"claude-sonnet-4","system":[{"type":"text","text":"s","cache_control":{"type":"ephemeral","ttl":"5m"}}]}`)
	out, err := normalizeClaudeCacheTTL(body, "claude-sonnet-4")
	require.NoError(t, err)
	require.Equal(t, "5m", gjson.GetBytes(out, "system.0.cache_control.ttl").String())
}

func TestCacheTTLDefaultPreservesInvalidAndUnknownFields(t *testing.T) {
	for _, ttl := range []string{`null`, `42`, `false`, `"invalid"`, `""`} {
		body := []byte(`{"system":[{"cache_control":{"type":"ephemeral","ttl":` + ttl + `}}],"tools":[{"input_schema":{"cache_control":{"type":"ephemeral"}}}],"messages":[{"role":"user","content":[{"type":"tool_use","input":{"cache_control":{"type":"ephemeral"}}}]}]}`)
		out, err := normalizeClaudeCacheTTL(body, "claude-sonnet-5-5")
		require.NoError(t, err)
		require.Equal(t, string(body), string(out))
	}
	for _, model := range []string{"kimi-k3.1-pro", "gpt-6.1-sol", "gemini-4-argon"} {
		body := []byte(`{"cache_control":{"type":"ephemeral"}}`)
		out, err := normalizeClaudeCacheTTL(body, model)
		require.NoError(t, err)
		require.Equal(t, string(body), string(out))
	}
}

func TestCacheTTLAutomaticFinalAndInlineSystemOrder(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		conflict   bool
	}{
		{"automatic_conflicts_final_hour", `{"cache_control":{"type":"ephemeral","ttl":"5m"},"messages":[{"role":"user","content":[{"type":"text","text":"a","cache_control":{"type":"ephemeral","ttl":"1h"}}]}]}`, true},
		{"automatic_new_unmarked_block", `{"cache_control":{"type":"ephemeral","ttl":"5m"},"messages":[{"role":"user","content":[{"type":"text","text":"a","cache_control":{"type":"ephemeral","ttl":"1h"}},{"type":"text","text":"b"}]}]}`, false},
		{"native_system_stays_later", `{"messages":[{"role":"user","content":[{"type":"text","text":"a","cache_control":{"type":"ephemeral","ttl":"5m"}}]},{"role":"system","content":[{"type":"text","text":"later","cache_control":{"type":"ephemeral"}}]}]}`, true},
		{"inline_tool_definition_defaults", `{"messages":[{"role":"system","content":[{"type":"tool_addition","tool":{"type":"tool_definition","definition":{"name":"probe","input_schema":{"cache_control":{"type":"ephemeral"}},"cache_control":{"type":"ephemeral"}}}}]}]}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := normalizeClaudeCacheTTL([]byte(tc.body), "claude-opus-5-5")
			if tc.conflict {
				require.ErrorIs(t, err, ErrCacheTTLOrderConflict)
				return
			}
			require.NoError(t, err)
			if tc.name == "inline_tool_definition_defaults" {
				require.Equal(t, "1h", gjson.GetBytes(out, "messages.0.content.0.tool.definition.cache_control.ttl").String())
				require.False(t, gjson.GetBytes(out, "messages.0.content.0.tool.definition.input_schema.cache_control.ttl").Exists())
			}
		})
	}
}

func TestCacheTTLConcurrentDefaultsDoNotMutateSharedInput(t *testing.T) {
	body := []byte(`{"model":"claude-sonnet-5-5","messages":[{"role":"user","content":[{"type":"text","text":"synthetic","cache_control":{"type":"ephemeral"}}]}]}`)
	original := string(body)
	var wg sync.WaitGroup
	for range 128 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := normalizeClaudeCacheTTL(body, "claude-sonnet-5-5")
			if err != nil || gjson.GetBytes(out, "messages.0.content.0.cache_control.ttl").String() != "1h" {
				t.Errorf("invalid pure transformation: %v", err)
			}
		}()
	}
	wg.Wait()
	require.Equal(t, original, string(body))
}
