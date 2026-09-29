package commandcode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// HTTPDoer must prohibit redirects. The caller supplies its account-bound proxy
// and transport; no endpoint or credential may be taken from the incoming client.
type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

type Client struct {
	HTTPClient HTTPDoer
	UserAgent  string
	Version    string
}

// UsageError preserves observed usage when generation subsequently fails.
// It never includes raw upstream data; callers may account for Usage once.
type UsageError struct {
	Cause error
	Usage map[string]any
}

func (e *UsageError) Error() string { return e.Cause.Error() }
func (e *UsageError) Unwrap() error { return e.Cause }

func (c *Client) ChatCompletion(ctx context.Context, key string, body []byte, stream bool) (*http.Response, error) {
	if c == nil || c.HTTPClient == nil || !validCredential(key) {
		return nil, ErrRequest
	}
	now := time.Now()
	prepared, err := prepareRequest(body, stream, now)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, Endpoint, bytes.NewReader(prepared.body))
	if err != nil {
		cancel()
		return nil, ErrRequest
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream, application/x-ndjson")
	req.Header.Set("x-cli-environment", "production")
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}
	if c.Version != "" {
		req.Header.Set("x-command-code-version", c.Version)
	}
	upstream, err := c.HTTPClient.Do(req)
	if err != nil {
		cause := ctx.Err()
		cancel()
		if cause != nil {
			return nil, cause
		}
		return nil, ErrTransport
	}
	if upstream == nil || upstream.Body == nil {
		cancel()
		return nil, ErrUpstream
	}
	// This detects accidental transport redirects; redirect prevention belongs in HTTPDoer.
	if upstream.Request != nil && (upstream.Request.URL == nil || upstream.Request.URL.String() != Endpoint) {
		upstream.Body.Close()
		cancel()
		return nil, ErrUpstream
	}
	if upstream.StatusCode != http.StatusOK {
		upstream.Body.Close()
		cancel()
		status := upstream.StatusCode
		if status < 400 || status > 599 {
			status = http.StatusBadGateway
		}
		data, _ := json.Marshal(map[string]any{"error": map[string]any{"type": "upstream_error", "code": "commandcode_upstream_error", "message": fmt.Sprintf("CommandCode upstream returned HTTP %d", upstream.StatusCode)}})
		header := safeHeaders(upstream.Header)
		header.Set("Content-Type", "application/json")
		return response(status, header, io.NopCloser(bytes.NewReader(data))), nil
	}
	id, err := uuid()
	if err != nil {
		upstream.Body.Close()
		cancel()
		return nil, ErrUpstream
	}
	identity := map[string]any{"id": "chatcmpl-cc-" + id, "created": now.Unix(), "model": prepared.model}
	header := safeHeaders(upstream.Header)
	if !stream {
		defer cancel()
		defer upstream.Body.Close()
		out, err := readEvents(upstream.Body, prepared, nil, nil)
		if err != nil {
			return nil, withUsage(err, out.usage)
		}
		message := map[string]any{"role": "assistant", "content": out.text.String()}
		if out.reasoning.Len() > 0 {
			message["reasoning_content"] = out.reasoning.String()
		}
		if len(out.calls) > 0 {
			message["tool_calls"] = out.calls
		}
		identity["object"] = "chat.completion"
		identity["choices"] = []any{map[string]any{"index": 0, "message": message, "finish_reason": out.finish}}
		identity["usage"] = out.usage
		data, err := json.Marshal(identity)
		if err != nil {
			return nil, withUsage(ErrProtocol, out.usage)
		}
		header.Set("Content-Type", "application/json")
		return response(http.StatusOK, header, io.NopCloser(bytes.NewReader(data))), nil
	}
	reader, writer := io.Pipe()
	bodyReader := &streamBody{PipeReader: reader, upstream: upstream.Body, cancel: cancel}
	bodyReader.cancelWatch = context.AfterFunc(ctx, func() { reader.CloseWithError(ctx.Err()); upstream.Body.Close() })
	go func() {
		defer bodyReader.stop()
		identity["object"] = "chat.completion.chunk"
		emit := func(delta map[string]any, finish any, usage map[string]any, usageOnly bool) error {
			if usageOnly {
				identity["choices"] = []any{}
			} else {
				identity["choices"] = []any{map[string]any{"index": 0, "delta": delta, "finish_reason": finish}}
			}
			delete(identity, "usage")
			if usage != nil {
				identity["usage"] = usage
			}
			data, err := json.Marshal(identity)
			if err != nil {
				return ErrProtocol
			}
			_, err = fmt.Fprintf(writer, "data: %s\n\n", data)
			return err
		}
		err := emit(map[string]any{"role": "assistant"}, nil, nil, false)
		var out completion
		if err == nil {
			out, err = readEvents(upstream.Body, prepared,
				func(delta map[string]any) error { return emit(delta, nil, nil, false) },
				func(usage map[string]any) error { return emit(nil, nil, usage, true) })
		}
		if err == nil {
			err = emit(map[string]any{}, out.finish, nil, false)
		}
		if err == nil {
			_, err = io.WriteString(writer, "data: [DONE]\n\n")
		}
		writer.CloseWithError(withUsage(err, out.usage))
	}()
	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-cache")
	return response(http.StatusOK, header, bodyReader), nil
}

func withUsage(err error, usage map[string]any) error {
	if err == nil || usage == nil {
		return err
	}
	return &UsageError{Cause: err, Usage: usage}
}

type streamBody struct {
	*io.PipeReader
	upstream    io.ReadCloser
	cancel      context.CancelFunc
	once        sync.Once
	cancelWatch func() bool
}

func (b *streamBody) stop()        { b.once.Do(func() { b.cancelWatch(); b.cancel(); b.upstream.Close() }) }
func (b *streamBody) Close() error { b.stop(); return b.PipeReader.Close() }

func response(status int, header http.Header, body io.ReadCloser) *http.Response {
	return &http.Response{StatusCode: status, Header: header, Body: body, ContentLength: -1}
}
func safeHeaders(upstream http.Header) http.Header {
	result := make(http.Header)
	for _, key := range []string{"Retry-After", "X-Request-ID", "Request-ID"} {
		if value := upstream.Get(key); len(value) <= 512 && !strings.ContainsAny(value, "\r\n") && value != "" {
			result.Set(key, value)
		}
	}
	return result
}
func validCredential(key string) bool {
	if len(key) == 0 || len(key) > 16384 {
		return false
	}
	for _, ch := range key {
		if ch <= 32 || ch >= 127 {
			return false
		}
	}
	return true
}
