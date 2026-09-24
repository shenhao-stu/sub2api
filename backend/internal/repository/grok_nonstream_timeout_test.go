//go:build unit

package repository

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestGrokJSONCanFinishBeyondStreamingHeaderBudget(t *testing.T) {
	const body = `{"choices":[{"message":{"tool_calls":[{"id":"call_1","type":"function","function":{"name":"status","arguments":"{}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":20,"completion_tokens":30}}`
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(1300 * time.Millisecond):
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	}))
	defer origin.Close()
	cfg := &config.Config{}
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Security.URLAllowlist.AllowPrivateHosts = true
	cfg.Gateway.GrokResponseHeaderTimeout = 1
	cfg.Gateway.GrokNonstreamResponseHeaderTimeout = 3
	upstream := NewHTTPUpstream(cfg)
	for _, profile := range []service.HTTPUpstreamProfile{service.HTTPUpstreamProfileGrok, service.HTTPUpstreamProfileGrokNonstream} {
		ctx := service.WithHTTPUpstreamProfile(context.Background(), profile)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, origin.URL, strings.NewReader(`{}`))
		require.NoError(t, err)
		resp, err := upstream.Do(req, "", 9981, 1)
		if profile == service.HTTPUpstreamProfileGrok {
			require.ErrorContains(t, err, "timeout awaiting response headers")
			continue
		}
		require.NoError(t, err)
		got, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, body, string(got), "tool calls and metered usage must survive unchanged")
	}
}

func TestGrokJSONDeadlineIsBoundedAndKeepsGrokTransport(t *testing.T) {
	svc := &httpUpstreamService{}
	settings := svc.applyProfilePoolSettings(poolSettings{}, service.HTTPUpstreamProfileGrokNonstream)
	require.Equal(t, 240*time.Second, settings.responseHeaderTimeout)
	svc.cfg = &config.Config{}
	settings = svc.applyProfilePoolSettings(poolSettings{}, service.HTTPUpstreamProfileGrokNonstream)
	require.Equal(t, 240*time.Second, settings.responseHeaderTimeout, "zero must not disable the bound")
	require.Equal(t, upstreamProtocolModeGrok, svc.resolveProtocolMode(service.HTTPUpstreamProfileGrokNonstream, "", nil))
	svc.cfg.Gateway.GrokNonstreamResponseHeaderTimeout = 17
	settings = svc.applyProfilePoolSettings(poolSettings{}, service.HTTPUpstreamProfileGrokNonstream)
	require.Equal(t, 17*time.Second, settings.responseHeaderTimeout)
}

func TestGrokJSONAndSSEReuseIndependentConnectionPools(t *testing.T) {
	svc := NewHTTPUpstream(&config.Config{}).(*httpUpstreamService)
	for _, tls := range []bool{false, true} {
		get := func(profile service.HTTPUpstreamProfile) *upstreamClientEntry {
			var entry *upstreamClientEntry
			var err error
			if tls {
				entry, err = svc.getClientEntryWithTLS("", 9982, 2, nil, profile, false, false)
			} else {
				entry, err = svc.getClientEntry("", 9982, 2, profile, false, false)
			}
			require.NoError(t, err)
			return entry
		}
		stream := get(service.HTTPUpstreamProfileGrok)
		buffered := get(service.HTTPUpstreamProfileGrokNonstream)
		require.NotSame(t, stream, buffered)
		require.Same(t, stream, get(service.HTTPUpstreamProfileGrok))
		require.Same(t, buffered, get(service.HTTPUpstreamProfileGrokNonstream))
	}
}
