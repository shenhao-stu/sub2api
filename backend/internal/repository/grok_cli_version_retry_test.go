//go:build unit

package repository

import (
	"context"
	"errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/xai"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"strings"
	"testing"
)

type cliRefreshFunc func(context.Context, string) string

func (f cliRefreshFunc) Refresh(ctx context.Context, old string) string { return f(ctx, old) }

func TestGrokCLIVersionRetryBoundaries(t *testing.T) {
	const rejection = `{"error":"Your Grok CLI version (1.0.13) is outdated."}`
	for _, tt := range []struct {
		name, host, body, version string
		status                    int
		noReplay, unauthenticated bool
		wantCalls                 int
	}{
		{name: "recover", host: xai.CLIProxyHost, body: rejection, version: "1.0.50", status: 426, wantCalls: 2},
		{name: "unchanged", host: xai.CLIProxyHost, body: rejection, version: "1.0.13", status: 426, wantCalls: 1},
		{name: "older", host: xai.CLIProxyHost, body: rejection, version: "1.0.12", status: 426, wantCalls: 1},
		{name: "malformed", host: xai.CLIProxyHost, body: rejection, version: "1.0.51\r\nInjected: true", status: 426, wantCalls: 1},
		{name: "api host", host: "api.x.ai", body: rejection, version: "1.0.50", status: 426, wantCalls: 1},
		{name: "lookalike host", host: "cli-chat-proxy.grok.com.evil.test", body: rejection, version: "1.0.50", status: 426, wantCalls: 1},
		{name: "different rejection", host: xai.CLIProxyHost, body: "upgrade required", version: "1.0.50", status: 426, wantCalls: 1},
		{name: "stream success", host: xai.CLIProxyHost, body: "data: " + rejection, version: "1.0.50", status: 200, wantCalls: 1},
		{name: "quota", host: xai.CLIProxyHost, body: rejection, version: "1.0.50", status: 429, wantCalls: 1},
		{name: "not replayable", host: xai.CLIProxyHost, body: rejection, version: "1.0.50", status: 426, noReplay: true, wantCalls: 1},
		{name: "no authentication", host: xai.CLIProxyHost, body: rejection, version: "1.0.50", status: 426, unauthenticated: true, wantCalls: 1},
		{name: "oversized", host: xai.CLIProxyHost, body: rejection + strings.Repeat("x", grokFallbackBodyLimit), version: "1.0.50", status: 426, wantCalls: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("POST", "https://"+tt.host+"/v1/chat/completions", strings.NewReader(`{"model":"grok-4.7-build-fast"}`))
			require.NoError(t, err)
			if !tt.unauthenticated {
				req.Header.Set("Authorization", "Bearer test-token")
			}
			req.Header.Set(xai.CLIClientVersionHeader, "1.0.13")
			req.Header.Set("Idempotency-Key", "same-request")
			if tt.noReplay {
				req.GetBody = nil
			}
			calls, refreshes := 0, 0
			tr := &grokCLIVersionTransport{versions: cliRefreshFunc(func(ctx context.Context, old string) string {
				refreshes++
				require.Equal(t, "1.0.13", old)
				return tt.version
			}), base: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				if calls == 2 {
					require.Equal(t, req.URL.String(), r.URL.String())
					require.Equal(t, req.Header.Get("Authorization"), r.Header.Get("Authorization"))
					require.Equal(t, "same-request", r.Header.Get("Idempotency-Key"))
					require.Equal(t, tt.version, r.Header.Get(xai.CLIClientVersionHeader))
					require.Equal(t, xai.CLIUserAgent(tt.version), r.UserAgent())
					b, e := io.ReadAll(r.Body)
					require.NoError(t, e)
					require.JSONEq(t, `{"model":"grok-4.7-build-fast"}`, string(b))
				}
				// A second 426 must be returned without recursion or additional inference.
				return &http.Response{StatusCode: tt.status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(tt.body))}, nil
			})}
			resp, err := tr.RoundTrip(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			b, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, tt.body, string(b))
			require.Equal(t, tt.wantCalls, calls)
			require.LessOrEqual(t, refreshes, 1)
			require.Equal(t, "1.0.13", req.Header.Get(xai.CLIClientVersionHeader))
		})
	}
}

func TestGrokCLIVersionRetryNeverReplaysNetworkFailure(t *testing.T) {
	req, _ := http.NewRequest("POST", "https://"+xai.CLIProxyHost+"/v1/responses", strings.NewReader("{}"))
	sentinel := errors.New("ambiguous network failure")
	tr := &grokCLIVersionTransport{base: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, sentinel }), versions: cliRefreshFunc(func(context.Context, string) string { t.Fatal("must not refresh or replay"); return "" })}
	_, err := tr.RoundTrip(req)
	require.ErrorIs(t, err, sentinel)
}
