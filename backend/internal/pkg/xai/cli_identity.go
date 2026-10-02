package xai

import (
	"net/http"
	"os"
	"runtime"
	"strings"

	"golang.org/x/mod/semver"
)

// Grok Build identity. Only version metadata is synchronized; no CLI code is run.
const (
	// CLIProxyHost is the hostname that requires the official CLI identity headers.
	CLIProxyHost = "cli-chat-proxy.grok.com"

	// CLIClientVersion is the verified offline baseline; automatic metadata updates may advance it.
	CLIClientVersion = "1.0.46"

	// CLIStableVersion is the known-good minimum client version accepted by cli-chat-proxy.
	CLIStableVersion = "1.0.13"

	// CLIVersionEnv is the optional operator pin for the outbound identity.
	CLIVersionEnv = "XAI_GROK_CLI_VERSION"

	// CLITokenAuth is required by cli-chat-proxy for Grok Build OAuth tokens.
	CLITokenAuth = "xai-grok-cli"

	// CLIClientIdentifier is the x-grok-client-identifier value used by Grok shell/CLI.
	CLIClientIdentifier = "grok-pager"

	// CLIClientMode is used by billing / quota probes on the CLI surface.
	CLIClientMode = "interactive"
)

// ResolveCLIVersion uses one policy for gateway, billing, and transport headers.
func ResolveCLIVersion() string {
	version, _ := DefaultCLIIdentity.Policy().Resolve(os.Getenv(CLIVersionEnv))
	return version
}

// IsSupportedCLIVersion accepts only canonical, stable versions above the protocol floor.
func IsSupportedCLIVersion(version string) bool {
	canonical := "v" + version
	minimum := "v" + CLIStableVersion
	return len(version) <= 32 && semver.IsValid(canonical) &&
		semver.Prerelease(canonical) == "" &&
		semver.Canonical(canonical) == canonical &&
		semver.Compare(canonical, minimum) >= 0
}

// CLIUserAgent matches the official interactive CLI's platform identifiers.
func CLIUserAgent(version string) string {
	if strings.TrimSpace(version) == "" {
		version = CLIClientVersion
	}
	return cliUserAgent(version, runtime.GOOS, runtime.GOARCH)
}

func cliUserAgent(version, platform, arch string) string {
	if platform == "darwin" {
		platform = "macos"
	}
	switch arch {
	case "amd64":
		arch = "x86_64"
	case "arm64":
		arch = "aarch64"
	case "386":
		arch = "x86"
	}
	return "grok-pager/" + version + " grok-shell/" + version + " (" + platform + "; " + arch + ")"
}

// ApplyCLIProxyHeaders stamps the fixed Grok CLI identity when the request
// targets cli-chat-proxy. Direct api.x.ai traffic is left unchanged.
func ApplyCLIProxyHeaders(req *http.Request) {
	if req == nil || req.URL == nil || !strings.EqualFold(strings.TrimSpace(req.URL.Hostname()), CLIProxyHost) {
		return
	}
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	version := ResolveCLIVersion()
	req.Header.Set("X-XAI-Token-Auth", CLITokenAuth)
	req.Header.Set("x-grok-client-version", version)
	req.Header.Set("x-grok-client-identifier", CLIClientIdentifier)
	req.Header.Set("x-grok-client-mode", CLIClientMode)
	req.Header.Set("x-authenticateresponse", "authenticate-response")
	req.Header.Set("User-Agent", CLIUserAgent(version))
}
