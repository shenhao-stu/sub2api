package xai

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"strings"
	"testing"
)

type cliMetadataTransport func(*http.Request) (*http.Response, error)

func (f cliMetadataTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestCLIReleaseMetadataBoundaries(t *testing.T) {
	for _, tt := range []struct {
		name, primary, fallback, want string
		status                        int
	}{
		{"stable", "1.0.50\n", "", "1.0.50", 200},
		{"official fallback", "unavailable", `{"name":"@xai-official/grok","version":"1.0.51"}`, "1.0.51", 503},
		{"redirect denied", "", `{"name":"@xai-official/grok","version":"1.0.51"}`, "1.0.51", 302},
		{"prerelease", "1.0.99-beta.1", `{"name":"wrong/package","version":"1.0.51"}`, "", 200},
		{"oversized", strings.Repeat("1", 65537), `{"name":"@xai-official/grok","version":"1.0.51+build"}`, "", 200},
		{"header injection", "1.0.51\r\nInjected: true", "{}", "", 200},
	} {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			tr := cliMetadataTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				require.Empty(t, r.Header.Get("Authorization"))
				require.Empty(t, r.Header.Get("Cookie"))
				require.Equal(t, "GET", r.Method)
				body, status := tt.primary, tt.status
				if calls == 1 {
					require.Equal(t, CLIStableURL, r.URL.String())
				} else {
					require.Equal(t, CLINPMVersionURL, r.URL.String())
					body, status = tt.fallback, 200
				}
				return &http.Response{StatusCode: status, Header: http.Header{"Location": {"https://untrusted.invalid/steal"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
			})
			v, err := FetchCLIStableVersion(context.Background(), tr)
			require.Equal(t, tt.want, v)
			if tt.want == "" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.LessOrEqual(t, calls, 2)
		})
	}
}

func TestCLIReleaseCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	_, err := FetchCLIStableVersion(ctx, cliMetadataTransport(func(r *http.Request) (*http.Response, error) { calls++; return nil, context.Canceled }))
	require.True(t, errors.Is(err, context.Canceled))
	require.Equal(t, 1, calls)
}

func TestCLIVersionPolicyPrecedence(t *testing.T) {
	for _, tt := range []struct{ name, manual, synced, env, version, source string }{
		{"fallback", "", "", "", CLIClientVersion, "builtin"},
		{"sync", "", "1.0.51", "", "1.0.51", "synchronized"},
		{"no automatic downgrade", "", "1.0.13", "", CLIClientVersion, "builtin"},
		{"manual", "1.0.13", "1.0.51", "", "1.0.13", "manual"},
		{"environment", "1.0.13", "1.0.51", "1.0.52", "1.0.52", "environment"},
		{"invalid environment", "", "1.0.51", "0.2.120", "1.0.51", "synchronized"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			v, src := (CLIVersionPolicy{Manual: tt.manual, Synced: tt.synced}).Resolve(tt.env)
			require.Equal(t, tt.version, v)
			require.Equal(t, tt.source, src)
		})
	}
}

func TestCLIOutdatedRejectionRequiresExplicit426(t *testing.T) {
	body := []byte(`{"error":"Your Grok CLI version (1.0.13) is outdated. Please update."}`)
	require.True(t, IsCLIOutdatedRejection(426, body))
	for _, status := range []int{200, 400, 403, 429, 502} {
		require.False(t, IsCLIOutdatedRejection(status, body))
	}
	require.False(t, IsCLIOutdatedRejection(426, []byte("Upgrade required")))
}
