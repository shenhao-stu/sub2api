package repository

import (
	"context"
	"log/slog"
	"net/http"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/xai"
	"golang.org/x/mod/semver"
)

type grokCLIVersionRefresher interface {
	Refresh(context.Context, string) string
}

type grokCLIVersionTransport struct {
	base     http.RoundTripper
	versions grokCLIVersionRefresher
}

func httpClientWithGrokVersionRetry(client *http.Client, versions grokCLIVersionRefresher) *http.Client {
	if client == nil || versions == nil {
		return client
	}
	clone := *client
	base := clone.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	clone.Transport = &grokCLIVersionTransport{base: base, versions: versions}
	return &clone
}

func (t *grokCLIVersionTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil || resp == nil || resp.StatusCode != http.StatusUpgradeRequired ||
		req.URL.Scheme != "https" || !strings.EqualFold(req.URL.Hostname(), xai.CLIProxyHost) ||
		req.Header.Get("Authorization") == "" || (req.Body != nil && req.GetBody == nil) {
		return resp, err
	}
	body, ok := bufferSmallResponseBody(resp, grokFallbackBodyLimit)
	if !ok || !xai.IsCLIOutdatedRejection(resp.StatusCode, body) {
		return resp, nil
	}
	previous := req.Header.Get(xai.CLIClientVersionHeader)
	version := t.versions.Refresh(req.Context(), previous)
	if !xai.IsSupportedCLIVersion(version) || semver.Compare("v"+version, "v"+previous) <= 0 {
		return resp, nil
	}
	retry := req.Clone(req.Context())
	if req.Body != nil {
		retry.Body, err = req.GetBody()
		if err != nil {
			return resp, nil
		}
	}
	retry.Header.Set(xai.CLIClientVersionHeader, version)
	retry.Header.Set("User-Agent", xai.CLIUserAgent(version))
	_ = resp.Body.Close()
	slog.Info("grok_cli_version_retry", "previous", previous, "version", version)
	// No recursion: a second rejection, transport error, or stream is returned as-is.
	return t.base.RoundTrip(retry)
}
