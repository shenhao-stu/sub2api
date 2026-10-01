package xai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	CLIStableURL     = "https://x.ai/cli/stable"
	CLINPMVersionURL = "https://registry.npmjs.org/@xai-official%2fgrok/latest"
)

// FetchCLIStableVersion reads bounded metadata from fixed official endpoints.
// A private client prevents credential propagation and denies every redirect.
func FetchCLIStableVersion(ctx context.Context, transport http.RoundTripper) (string, error) {
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for _, endpoint := range []string{CLIStableURL, CLINPMVersionURL} {
		version, err := fetchCLIRelease(ctx, client, endpoint)
		if err == nil {
			return version, nil
		}
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
	}
	return "", errors.New("official Grok CLI version metadata unavailable or invalid")
}

func fetchCLIRelease(ctx context.Context, client *http.Client, endpoint string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/json, text/plain")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metadata HTTP %d", resp.StatusCode)
	}
	const limit = 64 << 10
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil || len(body) > limit {
		return "", errors.New("invalid metadata body")
	}
	version := strings.TrimSpace(string(body))
	if endpoint == CLINPMVersionURL {
		var pkg struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		}
		if err := json.Unmarshal(body, &pkg); err != nil || pkg.Name != "@xai-official/grok" {
			return "", errors.New("invalid official package metadata")
		}
		version = pkg.Version
	}
	if !IsSupportedCLIVersion(version) {
		return "", errors.New("invalid stable CLI version")
	}
	return version, nil
}

// IsCLIOutdatedRejection recognizes only the explicit pre-execution rejection.
func IsCLIOutdatedRejection(status int, body []byte) bool {
	if status != http.StatusUpgradeRequired {
		return false
	}
	text := strings.ToLower(string(body))
	return strings.Contains(text, "your grok cli version (") && strings.Contains(text, ") is outdated")
}
