//go:build unit

package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestGrokResponsesBudgetFollowsActualOutboundMode(t *testing.T) {
	account := &Account{Platform: PlatformGrok, Type: AccountTypeAPIKey,
		Credentials: map[string]any{"base_url": "https://api.x.ai/v1"}}
	for _, stream := range []bool{false, true} {
		body := []byte(fmt.Sprintf(`{"model":"grok-4.7-build-fast","stream":%t}`, stream))
		req, err := buildGrokResponsesRequest(context.Background(), nil, account, body, "test-token", "", nil)
		require.NoError(t, err)
		want := HTTPUpstreamProfileGrokNonstream
		if stream {
			want = HTTPUpstreamProfileGrok
		}
		require.Equal(t, want, HTTPUpstreamProfileFromContext(req.Context()))
	}
}

func TestSendCCUpstreamRequestUsesProviderTimeoutProfile(t *testing.T) {
	for _, tt := range []struct {
		name     string
		platform string
		kind     string
		profile  HTTPUpstreamProfile
	}{
		{"grok_api_key", PlatformGrok, AccountTypeAPIKey, HTTPUpstreamProfileGrok},
		{"grok_oauth", PlatformGrok, AccountTypeOAuth, HTTPUpstreamProfileGrok},
		{"openai_api_key", PlatformOpenAI, AccountTypeAPIKey, HTTPUpstreamProfileOpenAI},
		{"openai_oauth", PlatformOpenAI, AccountTypeOAuth, HTTPUpstreamProfileOpenAI},
	} {
		for _, stream := range []bool{false, true} {
			name := tt.name + "/json"
			if stream {
				name = tt.name + "/stream"
			}
			t.Run(name, func(t *testing.T) {
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
				upstream := &httpUpstreamRecorder{resp: &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader(`{}`)),
				}}
				svc := &OpenAIGatewayService{httpUpstream: upstream}
				account := &Account{ID: 922901, Platform: tt.platform, Type: tt.kind}
				resp, err := svc.sendCCUpstreamRequest(context.Background(), c, account,
					"https://upstream.example/v1/chat/completions", []byte(`{"model":"test"}`),
					stream, "test-token", "", "")
				require.NoError(t, err)
				t.Cleanup(func() { _ = resp.Body.Close() })
				wantProfile := tt.profile
				if tt.platform == PlatformGrok && !stream {
					wantProfile = HTTPUpstreamProfileGrokNonstream
				}
				require.Equal(t, wantProfile, HTTPUpstreamProfileFromContext(upstream.lastReq.Context()))
				wantAccept := "application/json"
				if stream {
					wantAccept = "text/event-stream"
				}
				require.Equal(t, wantAccept, upstream.lastReq.Header.Get("Accept"))
			})
		}
	}
}
