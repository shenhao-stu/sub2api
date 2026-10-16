package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

const anthropicNonstreamRequestBudget = 20 * time.Minute

const anthropicKeepaliveKey = "anthropic_keepalive"
const anthropicPingFrame = "event: ping\ndata: {\"type\": \"ping\"}\n\n"

// All response writes and state changes belong to the request goroutine.
type anthropicKeepalive struct {
	stream          bool
	interval        time.Duration
	bytes           int
	deadline        time.Time
	upstreamContext context.Context
}

func (s *GatewayService) initAnthropicKeepalive(c *gin.Context, account *Account, stream bool) bool {
	if c == nil || c.Request == nil || s.cfg == nil || account == nil || account.Platform != PlatformAnthropic || account.IsBedrock() || !strings.HasSuffix(strings.TrimRight(c.Request.URL.Path, "/"), "/messages") {
		return false
	}
	if anthropicKeepaliveFromContext(c) != nil {
		return true
	}
	seconds := s.cfg.Gateway.StreamKeepaliveInterval
	if !stream {
		seconds = s.cfg.Gateway.AnthropicNonstreamKeepaliveInterval
	}
	if seconds > 0 {
		k := &anthropicKeepalive{stream: stream, interval: time.Duration(seconds) * time.Second}
		if !stream {
			k.deadline = time.Now().Add(anthropicNonstreamRequestBudget)
		}
		c.Set(anthropicKeepaliveKey, k)
		return true
	}
	return false
}

func anthropicKeepaliveFromContext(c *gin.Context) *anthropicKeepalive {
	if c == nil {
		return nil
	}
	value, _ := c.Get(anthropicKeepaliveKey)
	k, _ := value.(*anthropicKeepalive)
	return k
}

// The absolute deadline belongs to the request, so account failover cannot
// restart its budget. Per-forward cancellation releases its timer on every exit;
// client cancellation still propagates normally for nonstream requests.
func anthropicKeepaliveForwardContext(ctx context.Context, c *gin.Context) (context.Context, context.CancelFunc) {
	k := anthropicKeepaliveFromContext(c)
	if k == nil {
		return ctx, func() {}
	}
	bounded, cancel := ctx, context.CancelFunc(func() {})
	if !k.stream {
		bounded, cancel = context.WithDeadline(ctx, k.deadline)
	}
	previous := k.upstreamContext
	k.upstreamContext = bounded
	return bounded, func() {
		k.upstreamContext = previous
		cancel()
	}
}

// A response body can stall after headers. Closing it on cancellation unblocks
// the reader; await joins that read before returning or releasing the account.
func awaitAnthropicBody[T any](c *gin.Context, reader io.Reader, operation func() (T, error)) (T, error) {
	k := anthropicKeepaliveFromContext(c)
	if k != nil && !k.stream && k.upstreamContext != nil {
		if closer, ok := reader.(io.Closer); ok {
			closed := make(chan struct{})
			stop := context.AfterFunc(k.upstreamContext, func() {
				_ = closer.Close()
				close(closed)
			})
			defer func() {
				if !stop() {
					<-closed
				}
			}()
		}
	}
	return awaitAnthropicUpstream(c, operation)
}

// The operation may read upstream bytes, but must never access the gin writer.
// Join the existing operation on cancellation; its own context determines
// whether it is canceled (nonstream) or follows the existing streaming drain.
func awaitAnthropicUpstream[T any](c *gin.Context, operation func() (T, error)) (T, error) {
	k := anthropicKeepaliveFromContext(c)
	if k == nil || k.interval <= 0 || k.upstreamContext == nil {
		return operation()
	}
	type result struct {
		value T
		err   error
	}
	done := make(chan result, 1)
	go func() { value, err := operation(); done <- result{value, err} }()
	timer := time.NewTicker(k.interval)
	defer timer.Stop()
	ticks := timer.C
	canceled := c.Request.Context().Done()
	for {
		select {
		case result := <-done:
			return result.value, result.err
		case <-canceled:
			ticks, canceled = nil, nil
		case <-ticks:
			// Prefer an already available response to committing a heartbeat.
			select {
			case result := <-done:
				return result.value, result.err
			default:
			}
			if c.Request.Context().Err() != nil || (!k.deadline.IsZero() && !time.Now().Before(k.deadline)) {
				ticks = nil
				continue
			}
			if !c.Writer.Written() {
				contentType := "application/json; charset=utf-8"
				if k.stream {
					contentType = "text/event-stream"
				}
				c.Header("Content-Type", contentType)
				c.Header("Cache-Control", "no-cache")
				c.Header("X-Accel-Buffering", "no")
			}
			frame := " \n"
			if k.stream {
				frame = anthropicPingFrame
			}
			n, err := c.Writer.WriteString(frame)
			k.bytes += n
			if err != nil {
				ticks = nil
				continue
			}
			c.Writer.Flush()
		}
	}
}

func recordAnthropicKeepalive(c *gin.Context, written int) {
	if k := anthropicKeepaliveFromContext(c); k != nil && written > 0 {
		k.bytes += written
	}
}

// Exclude only locally generated transport bytes, never upstream response data.
func AnthropicKeepaliveAdjustedWrittenSize(c *gin.Context) int {
	if c == nil || c.Writer == nil {
		return -1
	}
	size := c.Writer.Size()
	if k := anthropicKeepaliveFromContext(c); k != nil && k.bytes > 0 && size >= 0 {
		if size -= k.bytes; size <= 0 {
			return -1
		}
	}
	return size
}

func AnthropicJSONKeepaliveCommitted(c *gin.Context) bool {
	k := anthropicKeepaliveFromContext(c)
	return k != nil && !k.stream && k.bytes > 0
}

func AnthropicStreamKeepaliveCommitted(c *gin.Context) bool {
	k := anthropicKeepaliveFromContext(c)
	return k != nil && k.stream && k.bytes > 0
}

// Before the first beat preserve HTTP status. Afterwards retain one JSON
// document or a named Anthropic SSE error, and mark the real failure for Ops.
func WriteAnthropicErrorResponse(c *gin.Context, status int, payload any) {
	body, _ := json.Marshal(payload)
	writeAnthropicErrorBody(c, status, body)
}

func writeAnthropicErrorBody(c *gin.Context, status int, body []byte) {
	if !c.Writer.Written() {
		c.Data(status, "application/json", body)
		return
	}
	MarkResponseCommitted(c)
	message := sanitizeUpstreamErrorMessage(extractUpstreamErrorMessage(body))
	if message == "" {
		message = "Upstream request failed"
	}
	MarkOpsStreamError(c, "upstream_error", message, status)
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil || payload == nil {
		payload = make(map[string]any)
	}
	errBody, _ := payload["error"].(map[string]any)
	if errBody == nil {
		errBody = make(map[string]any)
	}
	if kind, _ := errBody["type"].(string); kind == "" {
		errBody["type"] = "upstream_error"
	}
	if text, _ := errBody["message"].(string); text == "" {
		errBody["message"] = message
	}
	payload["type"], payload["error"] = "error", errBody
	encoded, _ := json.Marshal(payload)
	if strings.HasPrefix(c.Writer.Header().Get("Content-Type"), "text/event-stream") {
		_, _ = fmt.Fprintf(c.Writer, "event: error\ndata: %s\n\n", encoded)
		c.Writer.Flush()
		return
	}
	_, _ = c.Writer.Write(encoded)
	c.Writer.Flush()
}

func anthropicDataIsHeartbeat(data string) bool {
	return gjson.Get(data, "type").String() == "ping"
}
