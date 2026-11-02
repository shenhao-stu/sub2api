//go:build unit

package middleware

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestCaptureCredentialHeaderNames(t *testing.T) {
	for _, name := range []string{"Authorization", "Proxy-Authorization", "Cookie", "Set-Cookie", "X-Goog-Api-Key", "X-APIKEY", "X-Custom.Token", "X_Secret", "Password", "Credential", "Signature", "X-Auth", "X-Authentication", "X-JWT", "X-Session-ID", "X-CSRF-Token", "X-XSRF", "X-Credentials"} {
		require.True(t, captureCredentialHeader(name), name)
	}
	for _, name := range []string{"X-Client-Fingerprint", "X-Codex-Turn-Metadata", "X-Monkey", "X-Secretariat", "X-Tokenize", "X-Key1", "X-1key", "Content-Type", "Sec-Fetch-Site"} {
		require.False(t, captureCredentialHeader(name), name)
	}
}

func TestCaptureParsedHeadersPreservesSpecialParsedFields(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://capture.example/path", nil)
	req.TransferEncoding = []string{"chunked"}
	req.Header["X-Empty"] = nil
	value := captureParsedHeaders(req)
	require.Equal(t, []string{"capture.example"}, value.Headers["Host"])
	require.Equal(t, []string{"chunked"}, value.Headers["Transfer-Encoding"])
	require.NotContains(t, value.Headers, "Content-Length")
	require.Equal(t, []string{}, value.Headers["X-Empty"])
	req.ContentLength = 123
	require.Equal(t, []string{"123"}, captureParsedHeaders(req).Headers["Content-Length"])
	req.Header["Content-Length"] = []string{"0"}
	req.Header["Transfer-Encoding"] = []string{"identity"}
	value = captureParsedHeaders(req)
	require.Equal(t, []string{"0"}, value.Headers["Content-Length"])
	require.Equal(t, []string{"identity"}, value.Headers["Transfer-Encoding"])
}

func TestRequestCaptureRealHTTPHeadersEntityAndStreaming(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "capture")
	router, _, _ := captureAuthFixture(t, dir)
	var entity bytes.Buffer
	zip := gzip.NewWriter(&entity)
	_, err := zip.Write([]byte("\x00 original invalid JSON <&> 中文 \xff\n"))
	require.NoError(t, err)
	require.NoError(t, zip.Close())
	type observation struct {
		body, input []byte
		err         error
	}
	observed := make(chan observation, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	router.POST("/stream", func(c *gin.Context) {
		body, err := io.ReadAll(c.Request.Body)
		files, globErr := filepath.Glob(filepath.Join(dir, "*.input.json"))
		if err == nil {
			err = globErr
		}
		var input []byte
		if err == nil && len(files) != 1 {
			err = fmt.Errorf("input count %d before dispatch", len(files))
		}
		if err == nil {
			input, err = os.ReadFile(files[0])
		}
		observed <- observation{body, input, err}
		if err != nil {
			c.Status(http.StatusInternalServerError)
			return
		}
		c.Header("Content-Type", "text/event-stream")
		_, _ = c.Writer.WriteString("data: first\n\n")
		c.Writer.Flush()
		<-release
		_, _ = c.Writer.WriteString("data: last\n\n")
	})
	router.POST("/other", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	server := httptest.NewServer(router)
	defer server.Close()
	defer releaseOnce.Do(func() { close(release) })
	client := server.Client()
	client.Timeout = 5 * time.Second
	req, err := http.NewRequest(http.MethodPost, server.URL+"/stream?ignored=synthetic-query-secret", bytes.NewReader(entity.Bytes()))
	require.NoError(t, err)
	req.Host = "client.example:8181"
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Add("X-Client-Fingerprint", "first <&>")
	req.Header.Add("X-Client-Fingerprint", "second")
	req.Header.Set("X-Codex-Turn-Metadata", `{"turn":"<&>"}`)
	secrets := map[string]string{
		"X-Api-Key": "synthetic-selected", "Authorization": "not-bearer synthetic-authorization-secret",
		"Proxy-Authorization": "synthetic-proxy-secret", "Cookie": "synthetic-cookie-secret", "Set-Cookie": "synthetic-set-cookie-secret",
		"X-Goog-Api-Key": "synthetic-goog-secret", "X-Custom-Token": "synthetic-custom-secret", "X-Secret": "synthetic-secret-secret",
		"X-Password": "synthetic-password-secret", "X-Credential": "synthetic-credential-secret", "X-Signature": "synthetic-signature-secret",
		"X-Auth": "synthetic-auth-secret", "X-Authentication": "synthetic-authentication-secret", "X-JWT": "synthetic-jwt-secret",
		"X-Session-ID": "synthetic-session-secret", "X-CSRF": "synthetic-csrf-secret", "X-XSRF": "synthetic-xsrf-secret", "X-Credentials": "synthetic-credentials-secret",
	}
	for name, secret := range secrets {
		req.Header.Set(name, secret)
	}
	req.Header.Add("X-Custom-Token", "synthetic-second-custom-secret")
	resp, err := client.Do(req)
	require.NoError(t, err, "the first SSE flush must arrive while the handler is waiting")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	stream := bufio.NewReader(resp.Body)
	line, err := stream.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "data: first\n", line)
	got := <-observed
	require.NoError(t, got.err)
	require.Equal(t, entity.Bytes(), got.body)
	var input requestCaptureInput
	require.NoError(t, json.Unmarshal(got.input, &input))
	require.Equal(t, 2, input.SchemaVersion)
	require.True(t, input.BodyComplete)
	require.Equal(t, input.CaptureID+".headers.gz", input.HeadersFile)
	require.Equal(t, captureHeadersPolicy, input.HeadersPolicy)
	require.Equal(t, map[string]string{"Content-Type": "application/octet-stream", "Content-Encoding": "gzip", "Accept": "text/event-stream"}, input.Headers)
	headerRaw := captureReadGzipArtifact(t, dir, input.HeadersFile, input.HeadersCompressedBytes, input.HeadersCompressedSHA256, input.HeadersRawBytes, input.HeadersRawSHA256)
	var headers requestCaptureHeaders
	require.NoError(t, json.Unmarshal(headerRaw, &headers))
	require.Equal(t, 1, headers.SchemaVersion)
	require.Equal(t, captureHeadersPolicy, headers.Policy)
	require.Equal(t, len(secrets), headers.RedactedCount)
	require.Equal(t, headers.RedactedCount, input.HeadersRedactedCount)
	require.Equal(t, []string{"first <&>", "second"}, headers.Headers["X-Client-Fingerprint"])
	require.Equal(t, []string{`{"turn":"<&>"}`}, headers.Headers["X-Codex-Turn-Metadata"])
	require.Equal(t, []string{"client.example:8181"}, headers.Headers["Host"])
	require.Equal(t, []string{fmt.Sprint(entity.Len())}, headers.Headers["Content-Length"])
	require.NotContains(t, string(headerRaw), `\u003c`)
	for name, secret := range secrets {
		require.Equal(t, []string{"[REDACTED]"}, headers.Headers[http.CanonicalHeaderKey(name)], name)
		require.NotContains(t, string(headerRaw), secret)
		require.NotContains(t, string(got.input), secret)
	}
	require.NotContains(t, string(headerRaw), "synthetic-second-custom-secret")
	require.NotContains(t, string(got.input), "synthetic-query-secret")
	require.Equal(t, entity.Bytes(), captureReadGzipArtifact(t, dir, input.PayloadFile, input.CompressedBytes, input.CompressedSHA256, input.RawBytes, input.RawSHA256))
	other, err := http.NewRequest(http.MethodPost, server.URL+"/other", nil)
	require.NoError(t, err)
	other.Header.Set("X-Api-Key", "synthetic-other")
	otherResp, err := client.Do(other)
	require.NoError(t, err)
	require.NoError(t, otherResp.Body.Close())
	require.Equal(t, http.StatusNoContent, otherResp.StatusCode)
	releaseOnce.Do(func() { close(release) })
	rest, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, "\ndata: last\n\n", string(rest))
	require.Len(t, captureJSONFiles(t, dir, ".input.json"), 1)
	require.Len(t, captureJSONFiles(t, dir, ".outcome.json"), 1)
}

func captureReadGzipArtifact(t *testing.T, dir, name string, compressedBytes int64, compressedSHA string, rawBytes int64, rawSHA string) []byte {
	t.Helper()
	compressed, err := os.ReadFile(filepath.Join(dir, name))
	require.NoError(t, err)
	sum := sha256.Sum256(compressed)
	require.Equal(t, compressedSHA, hex.EncodeToString(sum[:]))
	require.Equal(t, compressedBytes, int64(len(compressed)))
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	require.NoError(t, err)
	defer reader.Close()
	raw, err := io.ReadAll(reader)
	require.NoError(t, err)
	sum = sha256.Sum256(raw)
	require.Equal(t, rawSHA, hex.EncodeToString(sum[:]))
	require.Equal(t, rawBytes, int64(len(raw)))
	return raw
}

func TestRequestCaptureHeaderFailuresNeverPublishReadyInput(t *testing.T) {
	for _, failure := range []string{"oversize", "canceled", "corrupt", "missing", "partial", "storage"} {
		t.Run(failure, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "capture")
			router, cfg, keys := captureAuthFixture(t, dir)
			calls := 0
			router.POST("/capture", func(c *gin.Context) { calls++; c.Status(http.StatusOK) })
			req := httptest.NewRequest(http.MethodPost, "/capture", strings.NewReader("original"))
			req.Header.Set("X-Api-Key", "synthetic-selected")
			switch failure {
			case "oversize":
				req.Header.Set("X-Debug", strings.Repeat("a", int(captureHeadersRawLimit)))
			case "canceled":
				ctx, cancel := context.WithCancel(req.Context())
				cancel()
				req = req.WithContext(ctx)
			default:
				req.Body = &captureHeaderSabotageBody{dir: dir, action: failure}
				req.ContentLength = -1
			}
			w := httptest.NewRecorder()
			if failure == "canceled" {
				c, _ := gin.CreateTestContext(w)
				c.Request = req
				finish, ok := newRequestCapture(cfg).begin(c, keys["synthetic-selected"])
				require.False(t, ok)
				require.Nil(t, finish)
				require.Equal(t, 499, w.Code)
				_, err := os.Stat(dir)
				require.ErrorIs(t, err, os.ErrNotExist)
				return
			}
			router.ServeHTTP(w, req)
			require.Equal(t, http.StatusServiceUnavailable, w.Code)
			require.Zero(t, calls)
			require.Empty(t, captureJSONFiles(t, dir, ".input.json"))
			outcomes := captureJSONFiles(t, dir, ".outcome.json")
			require.Len(t, outcomes, 1)
			require.Equal(t, false, outcomes[0]["request_released"])
		})
	}
}

type captureHeaderSabotageBody struct{ dir, action string }

func (r *captureHeaderSabotageBody) Read(p []byte) (int, error) {
	paths, err := filepath.Glob(filepath.Join(r.dir, "*.headers.gz"))
	if err != nil || len(paths) != 1 {
		return 0, fmt.Errorf("missing headers before entity read")
	}
	path := paths[0]
	switch r.action {
	case "corrupt":
		err = os.WriteFile(path, []byte("not gzip"), 0600)
	case "missing":
		err = os.Remove(path)
	case "partial":
		err = os.Rename(path, path+".partial")
	case "storage":
		err = os.Remove(path)
		if err == nil {
			err = os.Mkdir(path, 0700)
		}
	}
	if err != nil {
		return 0, err
	}
	return copy(p, "original"), io.EOF
}
func (*captureHeaderSabotageBody) Close() error { return nil }

func TestRequestCaptureCancellationAtEntityEOFDoesNotDispatch(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "capture")
	router, _, _ := captureAuthFixture(t, dir)
	calls := 0
	router.POST("/capture", func(c *gin.Context) { calls++; c.Status(http.StatusOK) })
	req := httptest.NewRequest(http.MethodPost, "/capture", nil)
	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()
	req = req.WithContext(ctx)
	req.ContentLength = -1
	req.Body = &captureCancelEOFBody{cancel: cancel}
	req.Header.Set("X-Api-Key", "synthetic-selected")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Zero(t, calls)
	inputs := captureJSONFiles(t, dir, ".input.json")
	require.Len(t, inputs, 1)
	require.Equal(t, false, inputs[0]["body_complete"])
	require.Equal(t, "context_canceled", inputs[0]["read_error"])
	require.Equal(t, false, captureJSONFiles(t, dir, ".outcome.json")[0]["request_released"])
}

type captureCancelEOFBody struct{ cancel context.CancelFunc }

func (r *captureCancelEOFBody) Read(p []byte) (int, error) {
	r.cancel()
	return copy(p, "original"), io.EOF
}
func (*captureCancelEOFBody) Close() error { return nil }

func TestCaptureHeadersVerificationRejectsTamperingAndLimits(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	defer root.Close()
	input := requestCaptureInput{HeadersFile: "fixture.headers.gz"}
	req := httptest.NewRequest(http.MethodGet, "http://example.test", nil)
	require.NoError(t, capturePublishHeaders(req, root, &input))
	require.NoError(t, captureVerifyHeaders(root, input.HeadersFile, &input))
	for _, change := range []func(*requestCaptureInput){
		func(v *requestCaptureInput) { v.HeadersCompressedSHA256 = strings.Repeat("0", 64) },
		func(v *requestCaptureInput) { v.HeadersRawSHA256 = strings.Repeat("0", 64) },
		func(v *requestCaptureInput) { v.HeadersCompressedBytes++ },
		func(v *requestCaptureInput) { v.HeadersRawBytes++ },
		func(v *requestCaptureInput) { v.HeadersCompressedBytes = captureHeadersCompressedLimit + 1 },
		func(v *requestCaptureInput) { v.HeadersRawBytes = captureHeadersRawLimit + 1 },
	} {
		modified := input
		change(&modified)
		require.Error(t, captureVerifyHeaders(root, input.HeadersFile, &modified))
	}
	// A second publication cannot replace an already published UUID artifact.
	before, err := os.ReadFile(filepath.Join(dir, input.HeadersFile))
	require.NoError(t, err)
	req.Header.Set("X-Client-Fingerprint", "replacement")
	require.Error(t, capturePublishHeaders(req, root, &input))
	after, err := os.ReadFile(filepath.Join(dir, input.HeadersFile))
	require.NoError(t, err)
	require.Equal(t, before, after)
}
