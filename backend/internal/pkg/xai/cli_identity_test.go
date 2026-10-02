package xai

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveCLIVersionDefaultsToPinnedClientVersion(t *testing.T) {
	t.Setenv(CLIVersionEnv, "")
	// Default advertise pin is CLIClientVersion; CLIStableVersion is only the floor.
	require.Equal(t, CLIClientVersion, ResolveCLIVersion())
	require.True(t, IsSupportedCLIVersion(CLIClientVersion))
	require.True(t, IsSupportedCLIVersion(CLIStableVersion))
}

func TestResolveCLIVersionAcceptsValidOverride(t *testing.T) {
	t.Setenv(CLIVersionEnv, "1.0.47")
	require.Equal(t, "1.0.47", ResolveCLIVersion())
}

func TestResolveCLIVersionRejectsUnsafeOrTooOld(t *testing.T) {
	for _, version := range []string{
		"0.2.92",
		"1.0.47-beta.1",
		"1.0.47+build.1",
		"0.2.95\r\nX-Injected: true",
		"0.2.093",
		"0.3",
		"1",
	} {
		t.Run(version, func(t *testing.T) {
			t.Setenv(CLIVersionEnv, version)
			require.Equal(t, CLIClientVersion, ResolveCLIVersion())
		})
	}
}

func TestApplyCLIProxyHeaders(t *testing.T) {
	t.Setenv(CLIVersionEnv, "")

	req, err := http.NewRequest(http.MethodPost, "https://cli-chat-proxy.grok.com/v1/responses", nil)
	require.NoError(t, err)
	req.Header.Set("User-Agent", "legacy-client/1.0")

	ApplyCLIProxyHeaders(req)

	require.Equal(t, CLIClientVersion, req.Header.Get("x-grok-client-version"))
	require.Equal(t, CLIClientIdentifier, req.Header.Get("x-grok-client-identifier"))
	require.Equal(t, CLITokenAuth, req.Header.Get("X-XAI-Token-Auth"))
	require.Equal(t, "interactive", req.Header.Get("x-grok-client-mode"))
	require.Equal(t, "authenticate-response", req.Header.Get("x-authenticateresponse"))
	require.Equal(t, CLIUserAgent(CLIClientVersion), req.Header.Get("User-Agent"))
}

func TestApplyCLIProxyHeadersLeavesAPIHostUnchanged(t *testing.T) {
	t.Setenv(CLIVersionEnv, "0.2.95")

	req, err := http.NewRequest(http.MethodPost, "https://api.x.ai/v1/responses", nil)
	require.NoError(t, err)
	req.Header.Set("User-Agent", "direct-api-client/1.0")

	ApplyCLIProxyHeaders(req)

	require.Empty(t, req.Header.Get("x-grok-client-version"))
	require.Empty(t, req.Header.Get("x-grok-client-identifier"))
	require.Empty(t, req.Header.Get("X-XAI-Token-Auth"))
	require.Empty(t, req.Header.Get("x-grok-client-mode"))
	require.Empty(t, req.Header.Get("x-authenticateresponse"))
	require.Equal(t, "direct-api-client/1.0", req.Header.Get("User-Agent"))
}

func TestCLIUserAgentOfficialPlatformNames(t *testing.T) {
	for _, tc := range []struct{ os, arch, platform string }{
		{"linux", "amd64", "linux; x86_64"},
		{"darwin", "arm64", "macos; aarch64"},
		{"windows", "386", "windows; x86"},
		{"linux", "riscv64", "linux; riscv64"},
	} {
		t.Run(tc.platform, func(t *testing.T) {
			require.Equal(t, "grok-pager/1.0.50 grok-shell/1.0.50 ("+tc.platform+")", cliUserAgent("1.0.50", tc.os, tc.arch))
		})
	}
}
