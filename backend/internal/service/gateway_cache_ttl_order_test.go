package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// requireCacheTTLOrderAccepted fails when a ttl=1h breakpoint follows a 5m one
// in Anthropic's processing order (tools, system, messages). Anthropic rejects
// such requests with HTTP 400.
func requireCacheTTLOrderAccepted(t *testing.T, body []byte) {
	t.Helper()
	_, messagePaths, toolPaths, systemPaths := collectCacheControlPaths(body)
	var paths []string
	paths = append(paths, toolPaths...)
	paths = append(paths, systemPaths...)
	paths = append(paths, messagePaths...)
	shortTTLPath := ""
	for _, path := range paths {
		if gjson.GetBytes(body, path+".ttl").String() == cacheTTLTarget1h {
			require.Empty(t, shortTTLPath, "1h breakpoint %s follows 5m breakpoint %s in %s", path, shortTTLPath, body)
			continue
		}
		if shortTTLPath == "" {
			shortTTLPath = path
		}
	}
}

func TestCacheTTLDefaultOrdering(t *testing.T) {
	cases := []struct {
		name, body string
		conflict   bool
	}{
		{"five_before_missing", `{"system":[{"type":"text","text":"s","cache_control":{"type":"ephemeral","ttl":"5m"}}],"messages":[{"role":"user","content":[{"type":"text","text":"a","cache_control":{"type":"ephemeral"}}]}]}`, true},
		{"five_before_hour", `{"tools":[{"cache_control":{"type":"ephemeral","ttl":"5m"}}],"system":[{"cache_control":{"type":"ephemeral","ttl":"1h"}}]}`, true},
		{"automatic_last", `{"system":[{"cache_control":{"type":"ephemeral","ttl":"5m"}}],"cache_control":{"type":"ephemeral"}}`, true},
		{"all_five", `{"system":[{"cache_control":{"type":"ephemeral","ttl":"5m"}}],"cache_control":{"type":"ephemeral","ttl":"5m"}}`, false},
		{"hour_before_five", `{"system":[{"cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":[{"cache_control":{"type":"ephemeral","ttl":"5m"}}]}]}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			original := []byte(tc.body)
			out, err := normalizeClaudeCacheTTL(original, "claude-opus-5-5")
			require.Equal(t, tc.body, string(original))
			if tc.conflict {
				require.ErrorIs(t, err, ErrCacheTTLOrderConflict)
				require.Nil(t, out)
			} else {
				require.NoError(t, err)
				requireCacheTTLOrderAccepted(t, out)
			}
		})
	}
}

// A non-Claude Code client sends one 1h breakpoint. The mimic path adds default 1h
// breakpoints to the injected system block and the last tool, which come first
// in Anthropic's processing order. The outbound body must still be accepted.
func TestEnforceCacheControlLimit_MimicPathKeepsClient1hBreakpointValid(t *testing.T) {
	cases := map[string]string{
		"message 1h": `{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":[` +
			`{"type":"text","text":"Reply OK.","cache_control":{"type":"ephemeral","ttl":"1h"}}]}]}`,
		"system 1h moved into messages": `{"model":"claude-sonnet-4-6",` +
			`"system":[{"type":"text","text":"Be terse.","cache_control":{"type":"ephemeral","ttl":"1h"}}],` +
			`"messages":[{"role":"user","content":"Reply OK."}]}`,
		"tools and message 1h": `{"model":"claude-sonnet-4-6","tools":[{"name":"probe","input_schema":{"type":"object"}}],` +
			`"messages":[{"role":"user","content":[` +
			`{"type":"text","text":"Reply OK.","cache_control":{"type":"ephemeral","ttl":"1h"}}]}]}`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			body := []byte(raw)
			var system any
			if sys := gjson.GetBytes(body, "system"); sys.Exists() {
				require.NoError(t, json.Unmarshal([]byte(sys.Raw), &system))
			}
			body = rewriteSystemForNonClaudeCode(body, system)
			body = applyToolsLastCacheBreakpoint(body)

			out, err := normalizeClaudeCacheTTL(enforceCacheControlLimit(body), "claude-sonnet-4-6")
			require.NoError(t, err)

			requireCacheTTLOrderAccepted(t, out)
		})
	}
}

func TestForwardCountTokens_MimicPathKeepsClient1hBreakpointValid(t *testing.T) {
	gin.SetMode(gin.TestMode)

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", nil)
	c.Request.Header.Set("User-Agent", "third-party-client/1.0")

	// The mimic path adds a default 1h breakpoint to tools[-1], ahead of the client's 1h one.
	body := []byte(`{"model":"claude-sonnet-4-6",` +
		`"tools":[{"name":"probe","description":"d","input_schema":{"type":"object"}}],` +
		`"messages":[{"role":"user","content":[{"type":"text","text":"Reply OK.","cache_control":{"type":"ephemeral","ttl":"1h"}}]}]}`)
	parsed := &ParsedRequest{Body: NewRequestBodyRef(body), Model: "claude-sonnet-4-6"}

	upstream := &anthropicHTTPUpstreamRecorder{
		resp: &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"input_tokens":42}`)),
		},
	}
	svc := &GatewayService{
		cfg:              &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}},
		httpUpstream:     upstream,
		rateLimitService: &RateLimitService{},
	}
	account := &Account{
		ID:          402,
		Name:        "count-tokens-ttl-order-test",
		Platform:    PlatformAnthropic,
		Type:        AccountTypeSetupToken,
		Concurrency: 1,
		Credentials: map[string]any{"access_token": "test-oauth-token"},
		Status:      StatusActive,
		Schedulable: true,
	}

	require.NoError(t, svc.ForwardCountTokens(context.Background(), c, account, parsed))

	require.True(t, gjson.GetBytes(upstream.lastBody, "tools.0.cache_control").Exists(),
		"the mimic tools breakpoint should still be present")
	requireCacheTTLOrderAccepted(t, upstream.lastBody)
}
