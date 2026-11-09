package middleware

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/shirou/gopsutil/v4/disk"
)

// Bound open requests separately from expensive compression and disk I/O.
const (
	maxCaptureWaiters   = 512
	captureQueueTimeout = 90 * time.Second
)

var errCapturePressureBodyLimit = errors.New("capture_pressure_body_limit")

// This captures inbound entity bytes only. It never wraps the response writer,
// parses JSON, dispatches a model request, or captures upgraded WS messages.
type requestCapture struct {
	cfg       config.RequestCaptureConfig
	bodyLimit int64
	slots     chan struct{}
	waiters   chan struct{}
	rootMu    sync.Mutex
	root      *os.Root
	invalid   bool
}

type requestCaptureInput struct {
	SchemaVersion           int               `json:"schema_version"`
	CaptureID               string            `json:"capture_id"`
	CapturedAt              time.Time         `json:"captured_at"`
	SealedAt                time.Time         `json:"sealed_at"`
	GroupID                 int64             `json:"group_id"`
	APIKeyID                int64             `json:"api_key_id"`
	UserID                  int64             `json:"user_id"`
	RequestID               string            `json:"request_id"`
	Method                  string            `json:"method"`
	Route                   string            `json:"route"`
	Headers                 map[string]string `json:"headers"`
	ContentLength           int64             `json:"content_length"`
	BodyPresent             bool              `json:"body_present"`
	PayloadFile             string            `json:"payload_file"`
	RawBytes                int64             `json:"raw_bytes"`
	RawSHA256               string            `json:"raw_sha256"`
	CompressedBytes         int64             `json:"compressed_bytes"`
	CompressedSHA256        string            `json:"compressed_sha256"`
	BodyComplete            bool              `json:"body_complete"`
	ReadError               string            `json:"read_error"`
	HeadersFile             string            `json:"headers_file"`
	HeadersRawBytes         int64             `json:"headers_raw_bytes"`
	HeadersRawSHA256        string            `json:"headers_raw_sha256"`
	HeadersCompressedBytes  int64             `json:"headers_compressed_bytes"`
	HeadersCompressedSHA256 string            `json:"headers_compressed_sha256"`
	HeadersPolicy           string            `json:"headers_policy"`
	HeadersRedactedCount    int               `json:"headers_redacted_count"`
}

type requestCaptureOutcome struct {
	SchemaVersion   int       `json:"schema_version"`
	CaptureID       string    `json:"capture_id"`
	FinishedAt      time.Time `json:"finished_at"`
	HTTPStatus      int       `json:"http_status"`
	ContextCanceled bool      `json:"context_canceled"`
	ErrorCode       string    `json:"error_code"`
	RequestReleased bool      `json:"request_released"`
}

func newRequestCapture(cfg *config.Config) *requestCapture {
	if len(cfg.RequestCapture.GroupIDs) == 0 {
		return nil
	}
	concurrent := cfg.RequestCapture.MaxConcurrent
	if concurrent < 1 || concurrent > 16 {
		concurrent = 1
	}
	return &requestCapture{
		cfg:       cfg.RequestCapture,
		bodyLimit: cfg.RequestCaptureBodyLimit(),
		slots:     make(chan struct{}, concurrent),
		waiters:   make(chan struct{}, maxCaptureWaiters),
		invalid:   cfg.RequestCapture.Validate(cfg.RequestCaptureBodyLimit()) != nil,
	}
}

func (s *requestCapture) matches(key *service.APIKey) bool {
	if s == nil || key.GroupID == nil {
		return false
	}
	for _, id := range s.cfg.GroupIDs {
		if id == *key.GroupID {
			return true
		}
	}
	return false
}

func (s *requestCapture) openRoot() (*os.Root, error) {
	s.rootMu.Lock()
	defer s.rootMu.Unlock()
	if s.root != nil {
		return s.root, nil
	}
	if s.invalid {
		return nil, errors.New("invalid_config")
	}
	if err := os.MkdirAll(s.cfg.Directory, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(s.cfg.Directory)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("directory_not_private")
	}
	s.root, err = os.OpenRoot(s.cfg.Directory)
	return s.root, err
}

func (s *requestCapture) diskAvailable() bool {
	usage, err := disk.Usage(s.cfg.Directory)
	return err == nil && usage.Free >= s.cfg.MinFreeBytes
}

// Waiting requests own only a queue entry. They do not read a body, open a
// capture file or start billing until a writer is available.
func (s *requestCapture) acquireWriter(ctx context.Context) bool {
	select {
	case s.slots <- struct{}{}:
		return true
	default:
	}
	select {
	case s.waiters <- struct{}{}:
		defer func() { <-s.waiters }()
	default:
		return false
	}
	timer := time.NewTimer(captureQueueTimeout)
	defer timer.Stop()
	select {
	case s.slots <- struct{}{}:
		return true
	case <-ctx.Done():
	case <-timer.C:
	}
	return false
}

// begin returns a small outcome callback even for a retained partial input.
// The input slot is released before billing/handlers/SSE, never after a model
// response; only bounded input compression/publication holds it.
func (s *requestCapture) begin(c *gin.Context, key *service.APIKey) (finish func(bool), ok bool) {
	if !s.matches(key) {
		return nil, true
	}
	fail := func(code string, status int) {
		slog.Warn("request_capture_rejected", "code", code)
		AbortWithError(c, status, "REQUEST_CAPTURE_UNAVAILABLE", "Test group request capture is unavailable")
	}
	ctx := c.Request.Context()
	if !s.acquireWriter(ctx) {
		if ctx.Err() != nil {
			c.AbortWithStatus(499)
		} else {
			c.Header("Retry-After", "1")
			AbortWithError(c, http.StatusTooManyRequests, "REQUEST_CAPTURE_BUSY", "Test group request capture is busy. Please retry shortly.")
		}
		return nil, false
	}
	defer func() { <-s.slots }()
	if ctx.Err() != nil {
		c.AbortWithStatus(499)
		return nil, false
	}
	root, err := s.openRoot()
	if err != nil {
		fail("capture_storage_unavailable", http.StatusServiceUnavailable)
		return nil, false
	}
	usage, err := disk.Usage(s.cfg.Directory)
	if err != nil || usage.Free < s.cfg.MinFreeBytes {
		c.Header("Retry-After", "60")
		fail("capture_storage_unavailable", http.StatusServiceUnavailable)
		return nil, false
	}
	bodyLimit := s.bodyLimit
	if usage.Free < s.cfg.PressureFreeBytes {
		bodyLimit = min(bodyLimit, s.cfg.PressureMaxBodyBytes)
	}
	pressureRejected := func() {
		c.Header("Retry-After", "60")
		fail("capture_pressure_body_limit", http.StatusServiceUnavailable)
	}
	if bodyLimit < s.bodyLimit && c.Request.ContentLength > bodyLimit {
		pressureRejected()
		return nil, false
	}
	id, err := uuid.NewRandom()
	if err != nil {
		fail("capture_id_unavailable", http.StatusServiceUnavailable)
		return nil, false
	}
	input := requestCaptureInput{SchemaVersion: 2, CaptureID: id.String(), CapturedAt: time.Now().UTC(), GroupID: *key.GroupID, APIKeyID: key.ID, UserID: key.UserID, Method: c.Request.Method, Route: c.FullPath(), ContentLength: c.Request.ContentLength, BodyPresent: c.Request.Body != nil, PayloadFile: id.String() + ".request.gz", Headers: make(map[string]string), HeadersFile: id.String() + ".headers.gz", HeadersPolicy: captureHeadersPolicy}
	input.RequestID, _ = c.Request.Context().Value(ctxkey.RequestID).(string)
	for _, name := range []string{"Content-Type", "Content-Encoding", "Accept"} {
		if value := c.Request.Header.Get(name); value != "" && len(value) <= 4096 {
			input.Headers[name] = value
		}
	}
	code := ""
	released := false
	var replay io.ReadCloser
	var once sync.Once
	finish = func(panicked bool) {
		once.Do(func() {
			if replay != nil {
				_ = replay.Close()
			}
			status := c.Writer.Status()
			if panicked {
				code = "handler_panic"
				if !c.Writer.Written() {
					status = http.StatusInternalServerError
				}
			}
			outcome := requestCaptureOutcome{SchemaVersion: 1, CaptureID: input.CaptureID, FinishedAt: time.Now().UTC(), HTTPStatus: status, ContextCanceled: c.Request.Context().Err() != nil, ErrorCode: code, RequestReleased: released}
			if err := capturePublishJSON(root, input.CaptureID+".outcome.json", outcome); err != nil {
				slog.Error("request_capture_outcome_failed", "capture_id", input.CaptureID, "code", "outcome_write_failed")
			}
		})
	}
	if err := capturePublishHeaders(c.Request, root, &input); err != nil {
		code = "capture_headers_failed"
		fail(code, http.StatusServiceUnavailable)
		return finish, false
	}
	readErr, storageErr := s.spool(c, root, &input, bodyLimit)
	if errors.Is(storageErr, errCapturePressureBodyLimit) {
		// Only this request's unpublished scratch files exist. Retaining them
		// would let rejected uploads consume the space the gate protects.
		for _, name := range []string{input.HeadersFile, input.PayloadFile + ".partial"} {
			if err := root.Remove(name); err != nil {
				slog.Warn("request_capture_scratch_cleanup_failed", "capture_id", input.CaptureID)
			}
		}
		pressureRejected()
		return nil, false
	}
	if storageErr != nil {
		code = "capture_write_failed"
		fail(code, http.StatusServiceUnavailable)
		return finish, false
	}
	if readErr != nil {
		code = input.ReadError
		status := http.StatusBadRequest
		if code == "body_too_large" {
			status = http.StatusRequestEntityTooLarge
		}
		AbortWithError(c, status, "REQUEST_CAPTURE_BODY_INCOMPLETE", "Failed to read complete request body")
		return finish, false
	}
	if input.BodyPresent && c.Request.Body != http.NoBody {
		file, err := root.Open(input.PayloadFile)
		if err != nil {
			code = "capture_reopen_failed"
			fail(code, http.StatusServiceUnavailable)
			return finish, false
		}
		reader, err := gzip.NewReader(file)
		if err != nil {
			_ = file.Close()
			code = "capture_reopen_failed"
			fail(code, http.StatusServiceUnavailable)
			return finish, false
		}
		replay = &captureReplayBody{reader: reader, file: file}
		c.Request.Body = replay
	}
	released = true
	return finish, true
}

func (s *requestCapture) spool(c *gin.Context, root *os.Root, input *requestCaptureInput, bodyLimit int64) (readErr, storageErr error) {
	partial := input.PayloadFile + ".partial"
	file, err := root.OpenFile(partial, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	compressed := &captureHashWriter{writer: file, sum: sha256.New()}
	writer, _ := gzip.NewWriterLevel(compressed, gzip.BestSpeed)
	rawHash := sha256.New()
	buf := make([]byte, 64<<10)
	reader := c.Request.Body
	if reader != nil {
		defer reader.Close()
		var sinceDiskCheck int64
		for {
			if err := c.Request.Context().Err(); err != nil {
				readErr = err
				break
			}
			n, err := reader.Read(buf)
			if n > 0 {
				if int64(n) > bodyLimit-input.RawBytes {
					if bodyLimit < s.bodyLimit {
						return nil, errCapturePressureBodyLimit
					}
					n = int(bodyLimit - input.RawBytes)
					err = &http.MaxBytesError{Limit: bodyLimit}
				}
				if _, writeErr := writer.Write(buf[:n]); writeErr != nil {
					_ = writer.Close()
					return nil, writeErr
				}
				_, _ = rawHash.Write(buf[:n])
				input.RawBytes += int64(n)
				sinceDiskCheck += int64(n)
				if sinceDiskCheck >= 1<<20 {
					if !s.diskAvailable() {
						_ = writer.Close()
						return nil, errors.New("disk_reserve_reached")
					}
					sinceDiskCheck = 0
				}
			}
			if err != nil {
				if err != io.EOF {
					readErr = err
				}
				break
			}
		}
	}
	if readErr == nil {
		readErr = c.Request.Context().Err()
	}
	if readErr == nil && input.ContentLength > 0 && input.RawBytes != input.ContentLength {
		readErr = io.ErrUnexpectedEOF
	}
	input.BodyComplete = readErr == nil
	if readErr != nil {
		input.ReadError = "read_error"
		var tooLarge *http.MaxBytesError
		if errors.As(readErr, &tooLarge) {
			input.ReadError = "body_too_large"
		} else if c.Request.Context().Err() != nil {
			input.ReadError = "context_canceled"
		}
	}
	if err := writer.Close(); err != nil {
		return readErr, err
	}
	if err := file.Sync(); err != nil {
		return readErr, err
	}
	if err := file.Close(); err != nil {
		return readErr, err
	}
	input.RawSHA256 = hex.EncodeToString(rawHash.Sum(nil))
	input.CompressedSHA256 = hex.EncodeToString(compressed.sum.Sum(nil))
	input.CompressedBytes = compressed.n
	if err := captureVerify(root, partial, input); err != nil {
		return readErr, err
	}
	if err := root.Rename(partial, input.PayloadFile); err != nil {
		return readErr, err
	}
	if err := captureVerifyHeaders(root, input.HeadersFile, input); err != nil {
		return readErr, err
	}
	input.SealedAt = time.Now().UTC()
	if err := capturePublishJSON(root, input.CaptureID+".input.json", input); err != nil {
		return readErr, err
	}
	return readErr, nil
}

func captureVerify(root *os.Root, name string, input *requestCaptureInput) error {
	file, err := root.Open(name)
	if err != nil {
		return err
	}
	defer file.Close()
	reader, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer reader.Close()
	sum := sha256.New()
	n, err := io.Copy(sum, reader)
	if err != nil {
		return err
	}
	if n != input.RawBytes || hex.EncodeToString(sum.Sum(nil)) != input.RawSHA256 {
		return errors.New("capture_verification_failed")
	}
	return nil
}

func capturePublishJSON(root *os.Root, name string, value any) error {
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return err
	}
	raw := encoded.Bytes()
	file, err := root.OpenFile(name+".partial", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Write(raw); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := root.Rename(name+".partial", name); err != nil {
		return err
	}
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

type captureHashWriter struct {
	writer io.Writer
	sum    hash.Hash
	n      int64
}

func (w *captureHashWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	_, _ = w.sum.Write(p[:n])
	w.n += int64(n)
	return n, err
}

type captureReplayBody struct {
	reader *gzip.Reader
	file   *os.File
	once   sync.Once
}

func (r *captureReplayBody) Read(p []byte) (int, error) { return r.reader.Read(p) }
func (r *captureReplayBody) Close() error {
	var err error
	r.once.Do(func() {
		err = errors.Join(r.reader.Close(), r.file.Close())
	})
	return err
}
