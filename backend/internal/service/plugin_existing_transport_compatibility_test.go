package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	pluginv1 "github.com/Wei-Shaw/sub2api/pkg/pluginapi/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPluginExistingRustTransportCompatibility(t *testing.T) {
	binaryPath := os.Getenv("SUB2API_TEST_EXISTING_RUST_PLUGIN")
	if binaryPath == "" {
		t.Skip("requires the existing Linux Rust transport binary")
	}
	binary, err := os.ReadFile(binaryPath)
	require.NoError(t, err)
	digest := sha256.Sum256(binary)
	require.Equal(t, "d514553d5ef3868da06a98822b4961f4adfd53b529d49ce382c56b079d521f4d", hex.EncodeToString(digest[:]))
	installation := &PluginInstallation{
		PluginKey: "tech.getoken.codex-rustls-transport", Version: "0.5.1",
		BinaryPath: binaryPath, BinarySHA256: hex.EncodeToString(digest[:]),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	runtime, err := startPluginRuntime(ctx, installation, 5*time.Second, t.TempDir(), &pluginv1.UnimplementedHostServiceServer{})
	require.NoError(t, err)
	t.Cleanup(runtime.kill)
	_, err = runtime.api.InitHostServices(ctx, &pluginv1.InitHostServicesRequest{HostServiceApiVersion: 1})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	require.NoError(t, runtime.validateAndApplyConfig(ctx, []byte(`{"force_http11":true,"zstd_request_compression":false}`)))
	before, err := runtime.status(ctx)
	require.NoError(t, err)
	require.True(t, before.Healthy)
	require.Empty(t, before.StatusJson)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, readErr := io.ReadAll(r.Body)
		if readErr != nil || string(body) != `{"model":"fixture"}` {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\"}\n\n")
	}))
	t.Cleanup(upstream.Close)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, upstream.URL, bytes.NewBufferString(`{"model":"fixture"}`))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	require.True(t, runtime.beginRequest())
	response, err := runtime.roundTrip(ctx, request, "", &Account{ID: 424242, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Concurrency: 1})
	if err != nil {
		runtime.finishRequest()
	}
	require.NoError(t, err)
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Equal(t, "data: {\"type\":\"response.completed\"}\n\n", string(body))
	require.Zero(t, runtime.inFlight.Load())
	after, err := runtime.status(ctx)
	require.NoError(t, err)
	require.True(t, after.Healthy)
}
