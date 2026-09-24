//go:build unit

package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

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
				require.Equal(t, tt.profile, HTTPUpstreamProfileFromContext(upstream.lastReq.Context()),
					"Grok Chat must receive the same configured header timeout as Grok Responses")
				wantAccept := "application/json"
				if stream {
					wantAccept = "text/event-stream"
				}
				require.Equal(t, wantAccept, upstream.lastReq.Header.Get("Accept"))
			})
		}
	}
}
