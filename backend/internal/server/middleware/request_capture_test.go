//go:build unit

package middleware

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ip"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func captureAuthFixture(t *testing.T, dir string, configure ...func(*config.Config, map[string]*service.APIKey)) (*gin.Engine, *config.Config, map[string]*service.APIKey) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	keys := make(map[string]*service.APIKey)
	for credential, groupID := range map[string]int64{"synthetic-selected": 42, "synthetic-other": 43} {
		group := &service.Group{ID: groupID, Status: service.StatusActive, Hydrated: true, Platform: service.PlatformGrok}
		user := &service.User{ID: 7, Status: service.StatusActive, Balance: 10}
		keys[credential] = &service.APIKey{ID: groupID + 100, UserID: user.ID, Key: credential, Status: service.StatusActive, User: user, Group: group, GroupID: &group.ID}
	}
	cfg := &config.Config{RunMode: config.RunModeSimple}
	cfg.Server.MaxRequestBodySize = 32 << 20
	cfg.RequestCapture = config.RequestCaptureConfig{GroupIDs: []int64{42}, Directory: dir, MinFreeBytes: 1 << 20, MaxConcurrent: 2}
	for _, apply := range configure {
		apply(cfg, keys)
	}
	repo := &stubApiKeyRepo{getByKey: func(_ context.Context, key string) (*service.APIKey, error) {
		if value := keys[key]; value != nil {
			clone := *value
			return &clone, nil
		}
		return nil, service.ErrAPIKeyNotFound
	}}
	svc := service.NewAPIKeyService(repo, nil, nil, nil, nil, nil, cfg)
	router := gin.New()
	router.Use(gin.HandlerFunc(NewAPIKeyAuthMiddleware(svc, nil, cfg)))
	return router, cfg, keys
}

func TestRequestCaptureRetainsPartialReadFailuresWithoutDispatch(t *testing.T) {
	for _, reason := range []string{"read_error", "context_canceled", "body_too_large"} {
		t.Run(reason, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "private")
			router, _, _ := captureAuthFixture(t, dir, func(cfg *config.Config, _ map[string]*service.APIKey) { cfg.Server.MaxRequestBodySize = 8 })
			calls := 0
			router.POST("/capture", func(c *gin.Context) { calls++; c.Status(http.StatusOK) })
			req := httptest.NewRequest(http.MethodPost, "/capture", nil)
			req.ContentLength = -1
			req.Header.Set("x-api-key", "synthetic-selected")
			switch reason {
			case "body_too_large":
				req.Body = io.NopCloser(strings.NewReader("1234567890"))
			case "context_canceled":
				ctx, cancel := context.WithCancel(req.Context())
				req = req.WithContext(ctx)
				req.Body = &captureFailureReader{cancel: cancel}
			default:
				req.Body = &captureFailureReader{}
			}
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if reason == "body_too_large" {
				require.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
			} else {
				require.Equal(t, http.StatusBadRequest, w.Code)
			}
			require.Zero(t, calls)
			inputs := captureJSONFiles(t, dir, ".input.json")
			require.Len(t, inputs, 1)
			require.Equal(t, false, inputs[0]["body_complete"])
			require.Equal(t, reason, inputs[0]["read_error"])
			outcome := captureJSONFiles(t, dir, ".outcome.json")
			require.Len(t, outcome, 1)
			require.Equal(t, reason == "context_canceled", outcome[0]["context_canceled"])
			require.Equal(t, false, outcome[0]["request_released"])
		})
	}
}

type captureFailureReader struct{ cancel context.CancelFunc }

func (r *captureFailureReader) Read(p []byte) (int, error) {
	n := copy(p, "part")
	if r.cancel != nil {
		r.cancel()
	}
	return n, errors.New("synthetic private read error must not be recorded")
}
func (*captureFailureReader) Close() error { return nil }

func TestRequestCaptureStorageFailureIsSelectedGroupOnly(t *testing.T) {
	for _, failure := range []string{"not_directory", "not_private", "symlink", "reserve"} {
		t.Run(failure, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "capture")
			switch failure {
			case "not_directory":
				require.NoError(t, os.WriteFile(dir, []byte("fixture"), 0600))
			case "not_private":
				require.NoError(t, os.Mkdir(dir, 0755))
				require.NoError(t, os.Chmod(dir, 0755))
			case "symlink":
				require.NoError(t, os.Symlink(t.TempDir(), dir))
			}
			router, _, _ := captureAuthFixture(t, dir, func(cfg *config.Config, _ map[string]*service.APIKey) {
				if failure == "reserve" {
					cfg.RequestCapture.MinFreeBytes = ^uint64(0)
				}
			})
			calls := 0
			router.POST("/capture", func(c *gin.Context) { calls++; c.Status(http.StatusOK) })
			for _, key := range []string{"synthetic-selected", "synthetic-other"} {
				req := httptest.NewRequest(http.MethodPost, "/capture", strings.NewReader("original"))
				req.Header.Set("x-api-key", key)
				w := httptest.NewRecorder()
				router.ServeHTTP(w, req)
				if key == "synthetic-selected" {
					require.Equal(t, http.StatusServiceUnavailable, w.Code)
				} else {
					require.Equal(t, http.StatusOK, w.Code)
				}
			}
			require.Equal(t, 1, calls)
			require.Empty(t, captureJSONFiles(t, dir, ".input.json"))
		})
	}
}

func TestRequestCaptureRespectsExistingMaxBytesReaderAndKeepsPartial(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "capture")
	router, _, _ := captureAuthFixture(t, dir)
	calls := 0
	router.POST("/capture", func(c *gin.Context) { calls++; c.Status(http.StatusOK) })
	req := httptest.NewRequest(http.MethodPost, "/capture", strings.NewReader("0123456789abcdef"))
	req.Header.Set("x-api-key", "synthetic-selected")
	w := httptest.NewRecorder()
	http.MaxBytesHandler(router, 8).ServeHTTP(w, req)
	require.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
	require.Zero(t, calls)
	input := captureJSONFiles(t, dir, ".input.json")[0]
	require.Equal(t, false, input["body_complete"])
	require.Equal(t, "body_too_large", input["read_error"])
	require.Equal(t, float64(8), input["raw_bytes"])
}

func TestRequestCapturePreservesNilAndNoBody(t *testing.T) {
	for _, body := range []io.ReadCloser{nil, http.NoBody} {
		t.Run(map[bool]string{true: "nil", false: "no_body"}[body == nil], func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "capture")
			router, _, _ := captureAuthFixture(t, dir)
			router.GET("/capture", func(c *gin.Context) {
				require.Equal(t, body, c.Request.Body)
				require.Zero(t, c.Request.ContentLength)
				c.Status(http.StatusOK)
			})
			req := httptest.NewRequest(http.MethodGet, "/capture", nil)
			req.Body = body
			req.Header.Set("x-api-key", "synthetic-selected")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			require.Equal(t, http.StatusOK, w.Code)
			input := captureJSONFiles(t, dir, ".input.json")[0]
			require.Equal(t, true, input["body_complete"])
			require.Equal(t, float64(0), input["raw_bytes"])
		})
	}
}

func TestRequestCaptureBasicAuthRejectionsAreNotCapturedButBillingRejectionsAre(t *testing.T) {
	for _, reason := range []string{"key_disabled", "user_disabled", "group_disabled", "ip_denied", "quota"} {
		t.Run(reason, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "capture")
			router, _, _ := captureAuthFixture(t, dir, func(cfg *config.Config, keys map[string]*service.APIKey) {
				key := keys["synthetic-selected"]
				switch reason {
				case "key_disabled":
					key.Status = service.StatusDisabled
				case "user_disabled":
					key.User.Status = service.StatusDisabled
				case "group_disabled":
					key.Group.Status = service.StatusDisabled
				case "ip_denied":
					key.IPWhitelist = []string{"10.0.0.1"}
					key.CompiledIPWhitelist = ip.CompileIPRules(key.IPWhitelist)
				case "quota":
					cfg.RunMode = config.RunModeStandard
					key.Status = service.StatusAPIKeyQuotaExhausted
				}
			})
			calls := 0
			router.POST("/capture", func(c *gin.Context) { calls++; c.Status(http.StatusOK) })
			req := httptest.NewRequest(http.MethodPost, "/capture", strings.NewReader("full original"))
			req.Header.Set("x-api-key", "synthetic-selected")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			require.Zero(t, calls)
			inputs := captureJSONFiles(t, dir, ".input.json")
			if reason == "quota" {
				require.Equal(t, http.StatusTooManyRequests, w.Code)
				require.Len(t, inputs, 1)
				require.Equal(t, true, inputs[0]["body_complete"])
				require.Equal(t, float64(http.StatusTooManyRequests), captureJSONFiles(t, dir, ".outcome.json")[0]["http_status"])
			} else {
				require.GreaterOrEqual(t, w.Code, 400)
				require.Empty(t, inputs)
			}
		})
	}
}

func TestRequestCaptureLargeUnknownLengthBodyUsesBoundedReads(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "capture")
	router, _, _ := captureAuthFixture(t, dir)
	const length = 8<<20 + 123
	original := &generatedCaptureBody{remaining: length}
	expected := sha256.New()
	_, err := io.Copy(expected, &generatedCaptureBody{remaining: length})
	require.NoError(t, err)
	calls := 0
	router.POST("/capture", func(c *gin.Context) {
		calls++
		require.True(t, original.closed, "network body must be closed before dispatch")
		require.Equal(t, int64(-1), c.Request.ContentLength)
		sum := sha256.New()
		n, err := io.Copy(sum, c.Request.Body)
		require.NoError(t, err)
		require.Equal(t, int64(length), n)
		require.Equal(t, expected.Sum(nil), sum.Sum(nil))
		c.Status(http.StatusNoContent)
	})
	req := httptest.NewRequest(http.MethodPost, "/capture", original)
	req.Header.Set("x-api-key", "synthetic-selected")
	req.ContentLength = -1
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusNoContent, w.Code)
	require.Equal(t, 1, calls)
	require.LessOrEqual(t, original.maxRead, 64<<10)
	input := captureJSONFiles(t, dir, ".input.json")[0]
	require.Equal(t, float64(length), input["raw_bytes"])
	require.Equal(t, hex.EncodeToString(expected.Sum(nil)), input["raw_sha256"])
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	info, err := os.Stat(dir)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0700), info.Mode().Perm())
	for _, entry := range entries {
		info, err := entry.Info()
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	}
}

type generatedCaptureBody struct {
	remaining, offset, maxRead int
	closed                     bool
}

func (r *generatedCaptureBody) Read(p []byte) (int, error) {
	if len(p) > r.maxRead {
		r.maxRead = len(p)
	}
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := min(len(p), r.remaining)
	for i := range p[:n] {
		p[i] = byte(((r.offset+i)*7 + (r.offset+i)>>8) % 251)
	}
	r.offset += n
	r.remaining -= n
	return n, nil
}
func (r *generatedCaptureBody) Close() error { r.closed = true; return nil }

func TestRequestCaptureBusyInputRejectsOnlySelectedGroupAndRecovers(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "capture")
	router, _, _ := captureAuthFixture(t, dir, func(cfg *config.Config, _ map[string]*service.APIKey) { cfg.RequestCapture.MaxConcurrent = 1 })
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	router.POST("/capture", func(c *gin.Context) { c.Status(http.StatusCreated) })
	first := httptest.NewRequest(http.MethodPost, "/capture", nil)
	first.ContentLength = -1
	first.Body = &blockingCaptureBody{started: started, release: release}
	first.Header.Set("x-api-key", "synthetic-selected")
	w1 := httptest.NewRecorder()
	go func() { defer close(done); router.ServeHTTP(w1, first) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("first request never started reading")
	}
	for _, key := range []string{"synthetic-selected", "synthetic-other"} {
		req := httptest.NewRequest(http.MethodPost, "/capture", strings.NewReader("second"))
		req.Header.Set("x-api-key", key)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if key == "synthetic-selected" {
			require.Equal(t, http.StatusServiceUnavailable, w.Code)
		} else {
			require.Equal(t, http.StatusCreated, w.Code)
		}
	}
	close(release)
	<-done
	require.Equal(t, http.StatusCreated, w1.Code)
	third := httptest.NewRequest(http.MethodPost, "/capture", strings.NewReader("third"))
	third.Header.Set("x-api-key", "synthetic-selected")
	w3 := httptest.NewRecorder()
	router.ServeHTTP(w3, third)
	require.Equal(t, http.StatusCreated, w3.Code)
	require.Len(t, captureJSONFiles(t, dir, ".input.json"), 2)
}

type blockingCaptureBody struct{ started, release chan struct{} }

func (r *blockingCaptureBody) Read(p []byte) (int, error) {
	close(r.started)
	<-r.release
	return copy(p, "original"), io.EOF
}
func (*blockingCaptureBody) Close() error { return nil }

func TestRequestCaptureDefaultOffDoesNotReadOrCreateStorage(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "must-not-exist")
	router, _, _ := captureAuthFixture(t, dir, func(cfg *config.Config, _ map[string]*service.APIKey) {
		cfg.RequestCapture = config.RequestCaptureConfig{}
	})
	body := &captureUnreadBody{}
	router.POST("/capture", func(c *gin.Context) { c.Status(http.StatusAccepted) })
	req := httptest.NewRequest(http.MethodPost, "/capture", body)
	req.Header.Set("x-api-key", "synthetic-selected")
	req.Header.Set("X-Debug-Group-ID", "42")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusAccepted, w.Code)
	require.False(t, body.read)
	_, err := os.Stat(dir)
	require.True(t, os.IsNotExist(err))
}

type captureUnreadBody struct{ read bool }

func (r *captureUnreadBody) Read([]byte) (int, error) { r.read = true; return 0, io.EOF }
func (*captureUnreadBody) Close() error               { return nil }

func TestRequestCaptureReplaysCompressedEntityWithoutChangingHeaders(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "capture")
	router, _, _ := captureAuthFixture(t, dir)
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, err := writer.Write([]byte("{invalid JSON but original bytes}"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	router.POST("/capture", func(c *gin.Context) {
		body, err := io.ReadAll(c.Request.Body)
		require.NoError(t, err)
		require.Equal(t, compressed.Bytes(), body)
		require.Equal(t, "gzip", c.GetHeader("Content-Encoding"))
		require.Equal(t, int64(compressed.Len()), c.Request.ContentLength)
		c.Status(http.StatusBadRequest)
	})
	req := httptest.NewRequest(http.MethodPost, "/capture", bytes.NewReader(compressed.Bytes()))
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("x-api-key", "synthetic-selected")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Equal(t, true, captureJSONFiles(t, dir, ".input.json")[0]["body_complete"])
}

func TestRequestCaptureInputSlotDoesNotLimitSSEConcurrencyOrFlush(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "capture")
	router, _, _ := captureAuthFixture(t, dir, func(cfg *config.Config, _ map[string]*service.APIKey) { cfg.RequestCapture.MaxConcurrent = 1 })
	flushed := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	router.POST("/stream", func(c *gin.Context) {
		_, _ = c.Writer.WriteString("data: first\n\n")
		c.Writer.Flush()
		close(flushed)
		<-release
		_, _ = c.Writer.WriteString("data: last\n\n")
	})
	router.POST("/capture", func(c *gin.Context) { c.Status(http.StatusCreated) })
	first := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("stream request"))
	first.Header.Set("x-api-key", "synthetic-selected")
	w := httptest.NewRecorder()
	go func() { defer close(done); router.ServeHTTP(w, first) }()
	select {
	case <-flushed:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("stream did not flush before completion")
	}
	second := httptest.NewRequest(http.MethodPost, "/capture", strings.NewReader("second request"))
	second.Header.Set("x-api-key", "synthetic-selected")
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, second)
	close(release)
	<-done
	require.Equal(t, http.StatusCreated, w2.Code)
	require.True(t, w.Flushed)
	require.Equal(t, "data: first\n\ndata: last\n\n", w.Body.String())
	require.Len(t, captureJSONFiles(t, dir, ".input.json"), 2)
	require.Len(t, captureJSONFiles(t, dir, ".outcome.json"), 2)
}

func TestRequestCaptureDoesNotReleasePrematureEOF(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "private")
	router, _, _ := captureAuthFixture(t, dir)
	calls := 0
	router.POST("/capture", func(c *gin.Context) { calls++; c.Status(http.StatusOK) })
	body := []byte("partial original bytes")
	req := httptest.NewRequest(http.MethodPost, "/capture", bytes.NewReader(body))
	req.ContentLength = int64(len(body) + 100)
	req.Header.Set("x-api-key", "synthetic-selected")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Zero(t, calls)
	inputs := captureJSONFiles(t, dir, ".input.json")
	require.Len(t, inputs, 1)
	require.Equal(t, false, inputs[0]["body_complete"])
	require.Equal(t, "read_error", inputs[0]["read_error"])
	require.Equal(t, float64(len(body)), inputs[0]["raw_bytes"])
	outcomes := captureJSONFiles(t, dir, ".outcome.json")
	require.Len(t, outcomes, 1)
	require.Equal(t, false, outcomes[0]["request_released"])
}

func captureJSONFiles(t *testing.T, dir, suffix string) []map[string]any {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*"+suffix))
	require.NoError(t, err)
	var result []map[string]any
	for _, path := range files {
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		var value map[string]any
		require.NoError(t, json.Unmarshal(raw, &value))
		result = append(result, value)
	}
	return result
}

func TestRequestCaptureSealsExactInputBeforeHandlerAndOnlyTrustedGroup(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "private")
	router, _, _ := captureAuthFixture(t, dir)
	body := []byte(" {\"tools\":[{\"name\":\"tool\",\"schema\":{\"n\":9007199254740993}}],\"messages\":[{\"role\":\"tool\",\"content\":\"history\"}],\"image\":\"data:image/png;base64,AA==\",\"unknown\":\"中文\\u0000\"} \n")
	calls := 0
	router.POST("/capture", func(c *gin.Context) {
		calls++
		if c.GetHeader("x-api-key") == "synthetic-selected" {
			inputs := captureJSONFiles(t, dir, ".input.json")
			require.Len(t, inputs, 1, "input must be durable before any handler dispatch")
			require.Equal(t, true, inputs[0]["body_complete"])
			require.Equal(t, float64(42), inputs[0]["group_id"])
			require.Empty(t, captureJSONFiles(t, dir, ".outcome.json"))
			file, err := os.Open(filepath.Join(dir, inputs[0]["payload_file"].(string)))
			require.NoError(t, err)
			defer file.Close()
			reader, err := gzip.NewReader(file)
			require.NoError(t, err)
			defer reader.Close()
			restored, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, body, restored)
			sum := sha256.Sum256(body)
			require.Equal(t, hex.EncodeToString(sum[:]), inputs[0]["raw_sha256"])
		}
		replayed, err := io.ReadAll(c.Request.Body)
		require.NoError(t, err)
		require.Equal(t, body, replayed)
		require.Equal(t, int64(len(body)), c.Request.ContentLength)
		c.Status(http.StatusCreated)
	})
	for _, key := range []string{"synthetic-selected", "synthetic-other", "invalid"} {
		req := httptest.NewRequest(http.MethodPost, "/capture?secret=synthetic-query-secret", bytes.NewReader(body))
		req.Header.Set("x-api-key", key)
		req.Header.Set("Authorization", "not-a-bearer synthetic-header-secret")
		req.Header.Set("Cookie", "synthetic-cookie-secret")
		req.Header.Set("X-Debug-Group-ID", "42")
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if key == "invalid" {
			require.Equal(t, http.StatusUnauthorized, w.Code)
		} else {
			require.Equal(t, http.StatusCreated, w.Code)
		}
	}
	require.Equal(t, 2, calls)
	inputs := captureJSONFiles(t, dir, ".input.json")
	require.Len(t, inputs, 1)
	outcomes := captureJSONFiles(t, dir, ".outcome.json")
	require.Len(t, outcomes, 1)
	require.Equal(t, float64(http.StatusCreated), outcomes[0]["http_status"])
	metadata, err := json.Marshal(inputs)
	require.NoError(t, err)
	for _, secret := range []string{"synthetic-selected", "synthetic-header-secret", "synthetic-cookie-secret", "synthetic-query-secret"} {
		require.NotContains(t, string(metadata), secret)
	}
}

func TestRequestCaptureHeaderJSONRemainsWithinArchiveManifestCap(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "capture")
	router, _, _ := captureAuthFixture(t, dir)
	router.POST("/capture", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	req := httptest.NewRequest(http.MethodPost, "/capture", strings.NewReader("{}"))
	req.Header.Set("x-api-key", "synthetic-selected")
	prefix := "application/json; x=\""
	safeHeader := prefix + strings.Repeat("&", 4096-len(prefix)-1) + "\""
	req.Header.Set("Content-Type", safeHeader)
	req.Header.Set("Accept", safeHeader)
	req.Header.Set("Content-Encoding", strings.Repeat("&", 4096))
	server := httptest.NewServer(router)
	defer server.Close()
	req.URL.Scheme = "http"
	req.URL.Host = strings.TrimPrefix(server.URL, "http://")
	req.RequestURI = ""
	resp, err := server.Client().Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	matches, err := filepath.Glob(filepath.Join(dir, "*.input.json"))
	require.NoError(t, err)
	require.Len(t, matches, 1)
	info, err := os.Stat(matches[0])
	require.NoError(t, err)
	require.Less(t, info.Size(), int64(65536))
	t.Logf("source input.json bytes=%d NAS input descriptor cap=65536", info.Size())
}
