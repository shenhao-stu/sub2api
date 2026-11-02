package middleware

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
)

const (
	captureHeadersPolicy                = "credential_names_v1"
	captureHeadersRawLimit        int64 = 8 << 20
	captureHeadersCompressedLimit int64 = 16 << 20
)

type requestCaptureHeaders struct {
	SchemaVersion int         `json:"schema_version"`
	Policy        string      `json:"policy"`
	Headers       http.Header `json:"headers"`
	RedactedCount int         `json:"redacted_count"`
}

// This policy classifies names, not arbitrary secrets in otherwise safe values.
func captureCredentialHeader(name string) bool {
	components := strings.FieldsFunc(strings.ToLower(name), func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < '0' || r > '9')
	})
	for _, component := range components {
		switch component {
		case "authorization", "cookie", "key", "apikey", "token", "secret", "password", "credential", "signature",
			"auth", "authentication", "jwt", "session", "csrf", "xsrf", "credentials":
			return true
		}
	}
	return false
}

func captureParsedHeaders(request *http.Request) requestCaptureHeaders {
	value := requestCaptureHeaders{SchemaVersion: 1, Policy: captureHeadersPolicy, Headers: make(http.Header)}
	for name, values := range request.Header {
		name = http.CanonicalHeaderKey(name)
		if _, exists := value.Headers[name]; !exists {
			value.Headers[name] = []string{}
		}
		value.Headers[name] = append(value.Headers[name], values...)
	}
	// Go keeps these parsed headers outside Header. Preserve their parsed values,
	// without inventing Content-Length: 0 for requests that did not send it.
	if request.Host != "" {
		value.Headers["Host"] = []string{request.Host}
	}
	if _, exists := value.Headers["Transfer-Encoding"]; !exists && len(request.TransferEncoding) != 0 {
		value.Headers["Transfer-Encoding"] = append([]string(nil), request.TransferEncoding...)
	}
	if _, exists := value.Headers["Content-Length"]; !exists && request.ContentLength > 0 {
		value.Headers["Content-Length"] = []string{strconv.FormatInt(request.ContentLength, 10)}
	}
	for name := range value.Headers {
		if captureCredentialHeader(name) {
			value.Headers[name] = []string{"[REDACTED]"}
			value.RedactedCount++
		}
	}
	return value
}

func capturePublishHeaders(request *http.Request, root *os.Root, input *requestCaptureInput) error {
	partial := input.HeadersFile + ".partial"
	file, err := root.OpenFile(partial, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	compressed := &captureHashWriter{writer: file, sum: sha256.New()}
	writer, _ := gzip.NewWriterLevel(&captureHeaderLimitWriter{writer: compressed, ctx: request.Context(), remaining: captureHeadersCompressedLimit}, gzip.BestSpeed)
	raw := &captureHashWriter{writer: &captureHeaderLimitWriter{writer: writer, ctx: request.Context(), remaining: captureHeadersRawLimit}, sum: sha256.New()}
	value := captureParsedHeaders(request)
	encoder := json.NewEncoder(raw)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		_ = writer.Close()
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	input.HeadersRawBytes = raw.n
	input.HeadersRawSHA256 = hex.EncodeToString(raw.sum.Sum(nil))
	input.HeadersCompressedBytes = compressed.n
	input.HeadersCompressedSHA256 = hex.EncodeToString(compressed.sum.Sum(nil))
	input.HeadersRedactedCount = value.RedactedCount
	if err := captureVerifyHeaders(root, partial, input); err != nil {
		return err
	}
	if err := request.Context().Err(); err != nil {
		return err
	}
	// A hard link publishes the immutable UUID artifact without replacing a file.
	if err := root.Link(partial, input.HeadersFile); err != nil {
		return err
	}
	return root.Remove(partial)
}

func captureVerifyHeaders(root *os.Root, name string, input *requestCaptureInput) error {
	if input.HeadersRawBytes < 0 || input.HeadersRawBytes > captureHeadersRawLimit || input.HeadersCompressedBytes < 0 || input.HeadersCompressedBytes > captureHeadersCompressedLimit {
		return errors.New("capture_headers_too_large")
	}
	file, err := root.Open(name)
	if err != nil {
		return err
	}
	defer file.Close()
	compressedHash := sha256.New()
	n, err := io.Copy(compressedHash, io.LimitReader(file, captureHeadersCompressedLimit+1))
	if err != nil {
		return err
	}
	if n != input.HeadersCompressedBytes || hex.EncodeToString(compressedHash.Sum(nil)) != input.HeadersCompressedSHA256 {
		return errors.New("capture_headers_compressed_verification_failed")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	reader, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer reader.Close()
	rawHash := sha256.New()
	n, err = io.Copy(rawHash, io.LimitReader(reader, captureHeadersRawLimit+1))
	if err != nil {
		return err
	}
	if n != input.HeadersRawBytes || hex.EncodeToString(rawHash.Sum(nil)) != input.HeadersRawSHA256 {
		return errors.New("capture_headers_raw_verification_failed")
	}
	return nil
}

type captureHeaderLimitWriter struct {
	writer    io.Writer
	ctx       context.Context
	remaining int64
}

func (w *captureHeaderLimitWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	if int64(len(p)) > w.remaining {
		return 0, errors.New("capture_headers_too_large")
	}
	n, err := w.writer.Write(p)
	w.remaining -= int64(n)
	return n, err
}
