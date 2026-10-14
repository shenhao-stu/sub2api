package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

const grokFirstSemanticBudgetKey = "grok_first_semantic_budget"
const grokFirstSemanticTimeoutCode = "grok_first_semantic_timeout"
const grokFirstSemanticTimeoutMessage = "Grok produced no text, reasoning or tool output before the request deadline"

type grokFirstSemanticContextKey struct{}

// The deadline belongs to the client request, not an account or retry attempt.
// Observing real output disables this first-output limit; it is not a generation limit.
type grokFirstSemanticBudget struct {
	deadline time.Time
	timeout  time.Duration
	observed atomic.Bool
}

type grokFirstSemanticTimeoutError struct{ budget *grokFirstSemanticBudget }

func (e *grokFirstSemanticTimeoutError) Error() string { return grokFirstSemanticTimeoutMessage }
func (e *grokFirstSemanticTimeoutError) Unwrap() error { return context.DeadlineExceeded }

func withGrokFirstSemanticBudget(req *http.Request, c *gin.Context, account *Account, cfg *config.Config, stream bool) *http.Request {
	if account == nil || account.Platform != PlatformGrok || !stream || c == nil || cfg == nil {
		return req
	}
	seconds := cfg.Gateway.GrokFirstSemanticTimeoutSeconds
	if seconds <= 0 {
		return req
	}
	value, exists := c.Get(grokFirstSemanticBudgetKey)
	budget, _ := value.(*grokFirstSemanticBudget)
	if !exists || budget == nil {
		timeout := time.Duration(seconds) * time.Second
		budget = &grokFirstSemanticBudget{deadline: time.Now().Add(timeout), timeout: timeout}
		c.Set(grokFirstSemanticTimeoutCode, false)
		c.Set(grokFirstSemanticBudgetKey, budget)
	}
	return req.WithContext(context.WithValue(req.Context(), grokFirstSemanticContextKey{}, budget))
}

// A WebSocket proxy attempt renumbers turns when it retries on another account.
// Only a newly read and parsed client frame starts a new logical request budget.
func resetGrokFirstSemanticBudget(c *gin.Context) {
	if c != nil {
		c.Set(grokFirstSemanticBudgetKey, (*grokFirstSemanticBudget)(nil))
		c.Set(grokFirstSemanticTimeoutCode, false)
	}
}

// ShouldRecordGrokFirstSemanticTimeoutUsage preserves only authoritative usage
// already parsed from upstream; a heartbeat-only failure must not create a bill.
func ShouldRecordGrokFirstSemanticTimeoutUsage(account *Account, result *OpenAIForwardResult, err error) bool {
	var timeout *grokFirstSemanticTimeoutError
	return account != nil && account.Platform == PlatformGrok && result != nil &&
		openAIUsageHasTokens(&result.Usage) && errors.As(err, &timeout)
}

type grokFirstSemanticGuard struct {
	mu       sync.Mutex
	budget   *grokFirstSemanticBudget
	timer    *time.Timer
	cancel   context.CancelFunc
	source   io.ReadCloser
	expired  bool
	closed   bool
	progress grokSemanticSSEObserver
}

func doGrokFirstSemanticUpstream(req *http.Request, send func(*http.Request) (*http.Response, error)) (*http.Response, error) {
	budget, _ := req.Context().Value(grokFirstSemanticContextKey{}).(*grokFirstSemanticBudget)
	if budget == nil || budget.observed.Load() {
		return send(req)
	}
	if req.Context().Err() != nil {
		return nil, req.Context().Err()
	}
	if !time.Now().Before(budget.deadline) {
		return nil, &grokFirstSemanticTimeoutError{budget}
	}
	ctx, cancel := context.WithCancel(req.Context())
	g := &grokFirstSemanticGuard{budget: budget, cancel: cancel}
	g.timer = time.AfterFunc(time.Until(budget.deadline), func() {
		g.mu.Lock()
		if g.closed || budget.observed.Load() || req.Context().Err() != nil {
			g.mu.Unlock()
			return
		}
		g.expired = true
		source := g.source
		g.mu.Unlock()
		cancel()
		if source != nil {
			_ = source.Close()
		}
	})
	resp, err := send(req.WithContext(ctx))
	stopGuard := err != nil || resp == nil || resp.Body == nil || resp.StatusCode >= 400
	g.mu.Lock()
	expired := g.expired
	if resp != nil {
		g.source = resp.Body
	}
	if !expired && stopGuard {
		g.closed = true
		g.timer.Stop()
	}
	g.mu.Unlock()
	if expired {
		_ = g.Close()
		return nil, &grokFirstSemanticTimeoutError{budget}
	}
	if stopGuard {
		// Error responses retain the existing error policy and own their context until close.
		if resp != nil && resp.Body != nil {
			resp.Body = &openAIRequestContextReadCloser{ReadCloser: resp.Body, cleanup: cancel}
		} else {
			cancel()
		}
		return resp, err
	}
	resp.Body = g
	return resp, nil
}

func (g *grokFirstSemanticGuard) Read(p []byte) (int, error) {
	n, err := g.source.Read(p)
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.expired {
		return n, &grokFirstSemanticTimeoutError{g.budget}
	}
	if n > 0 && !g.budget.observed.Load() && g.progress.feed(p[:n]) {
		g.budget.observed.Store(true)
		g.progress = grokSemanticSSEObserver{}
		g.timer.Stop()
	}
	return n, err
}
func (g *grokFirstSemanticGuard) Close() error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil
	}
	g.closed = true
	g.timer.Stop()
	source := g.source
	g.mu.Unlock()
	g.cancel()
	if source != nil {
		return source.Close()
	}
	return nil
}

// Observe complete SSE frames without modifying wire bytes. Bound only the
// observer's memory; the existing stream parser retains its own framing limits.
type grokSemanticSSEObserver struct {
	frame   []byte
	discard bool
}

func (o *grokSemanticSSEObserver) feed(chunk []byte) bool {
	for _, b := range chunk {
		if len(o.frame) >= openAIFirstOutputStageMaxBytes {
			o.discard = true
			o.frame = o.frame[:0]
		}
		o.frame = append(o.frame, b)
		if b != '\n' {
			continue
		}
		frame := o.frame
		complete := bytes.HasSuffix(frame, []byte("\n\n")) || bytes.HasSuffix(frame, []byte("\r\n\r\n"))
		if !complete {
			continue
		}
		if !o.discard && grokSSEFrameHasSemanticOutput(frame) {
			return true
		}
		o.frame = o.frame[:0]
		o.discard = false
	}
	return false
}
func grokSSEFrameHasSemanticOutput(frame []byte) bool {
	var event string
	var data []string
	for _, line := range strings.Split(string(frame), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if v, ok := extractOpenAISSEEventLine(line); ok {
			event = v
		}
		if v, ok := extractOpenAISSEDataLine(line); ok {
			data = append(data, v)
		}
	}
	payload := strings.Join(data, "\n")
	if !gjson.Valid(payload) {
		return false
	}
	root := gjson.Parse(payload)
	if typ := root.Get("type").String(); typ != "" {
		event = typ
	}
	nonempty := grokSemanticString
	switch event {
	case "response.output_text.delta", "response.reasoning_text.delta", "response.reasoning_summary_text.delta", "response.function_call_arguments.delta", "response.custom_tool_call_input.delta", "response.refusal.delta", "response.audio.delta", "response.audio_transcript.delta":
		return nonempty(root.Get("delta"))
	case "response.output_text.done", "response.reasoning_text.done", "response.reasoning_summary_text.done", "response.audio_transcript.done":
		return nonempty(root.Get("text"))
	case "response.function_call_arguments.done":
		return nonempty(root.Get("arguments"))
	case "response.custom_tool_call_input.done":
		return nonempty(root.Get("input"))
	case "response.output_item.added", "response.output_item.done":
		return grokItemHasSemanticOutput(root.Get("item"))
	case "response.content_part.added", "response.content_part.done", "response.reasoning_summary_part.added", "response.reasoning_summary_part.done":
		return nonempty(root.Get("part.text")) || nonempty(root.Get("part.refusal"))
	case "response.completed", "response.done":
		for _, item := range root.Get("response.output").Array() {
			if grokItemHasSemanticOutput(item) {
				return true
			}
		}
	}
	// Raw Chat Completions has no Responses event type.
	if event != "" && (event != "message" || root.Get("type").Exists()) {
		return false
	}
	for _, choice := range root.Get("choices").Array() {
		delta := choice.Get("delta")
		for _, key := range []string{"content", "reasoning_content", "reasoning", "refusal"} {
			if nonempty(delta.Get(key)) {
				return true
			}
		}
		if nonempty(delta.Get("function_call.arguments")) {
			return true
		}
		for _, tool := range delta.Get("tool_calls").Array() {
			if nonempty(tool.Get("function.arguments")) {
				return true
			}
		}
	}
	return false
}
func grokSemanticString(value gjson.Result) bool {
	return value.Type == gjson.String && value.String() != ""
}

func grokItemHasSemanticOutput(item gjson.Result) bool {
	switch item.Get("type").String() {
	case "message", "reasoning":
		for _, key := range []string{"content", "summary"} {
			for _, part := range item.Get(key).Array() {
				if grokSemanticString(part.Get("text")) || grokSemanticString(part.Get("refusal")) {
					return true
				}
			}
		}
		return grokSemanticString(item.Get("encrypted_content"))
	case "function_call":
		return grokSemanticString(item.Get("arguments"))
	case "custom_tool_call":
		return grokSemanticString(item.Get("input"))
	case "web_search_call", "x_search_call", "code_interpreter_call":
		return item.Get("status").String() == "completed" && (grokSemanticString(item.Get("result")) || len(item.Get("results").Array()) > 0 || len(item.Get("outputs").Array()) > 0)
	}
	return false
}

// Run on the handler goroutine, never in the transport timer. Request timeout
// is neither account exhaustion nor a retryable transport failure.
func recordGrokFirstSemanticTimeout(c *gin.Context, account *Account, err error) bool {
	var timeout *grokFirstSemanticTimeoutError
	if account == nil || account.Platform != PlatformGrok || !errors.As(err, &timeout) {
		return false
	}
	if c == nil {
		return true
	}
	if handled, _ := c.Get(grokFirstSemanticTimeoutCode); handled == true {
		return true
	}
	c.Set(grokFirstSemanticTimeoutCode, true)
	detail := fmt.Sprintf("timeout_ms=%d elapsed_ms=%d", timeout.budget.timeout.Milliseconds(), time.Since(timeout.budget.deadline.Add(-timeout.budget.timeout)).Milliseconds())
	setOpsUpstreamError(c, http.StatusGatewayTimeout, grokFirstSemanticTimeoutMessage, detail)
	appendOpsUpstreamError(c, OpsUpstreamErrorEvent{Platform: account.Platform, AccountID: account.ID, AccountName: account.Name, ProxyID: opsUpstreamProxyID(account), ProxyName: opsUpstreamProxyName(account), UpstreamStatusCode: http.StatusGatewayTimeout, Kind: grokFirstSemanticTimeoutCode, Message: grokFirstSemanticTimeoutMessage, Detail: detail})
	return true
}

func handleGrokFirstSemanticTimeout(c *gin.Context, account *Account, err error, usage *OpenAIUsage) bool {
	if c == nil {
		return recordGrokFirstSemanticTimeout(c, account, err)
	}
	handled, _ := c.Get(grokFirstSemanticTimeoutCode)
	if !recordGrokFirstSemanticTimeout(c, account, err) {
		return false
	}
	if handled == true {
		return true
	}
	payload := openAIErrorBodyWithUsage(gin.H{"error": gin.H{"type": "upstream_error", "code": grokFirstSemanticTimeoutCode, "message": grokFirstSemanticTimeoutMessage}}, usage)
	path := ""
	if c.Request != nil && c.Request.URL != nil {
		path = c.Request.URL.Path
	}
	if !c.Writer.Written() {
		if strings.Contains(path, "/messages") {
			payload = anthropicErrorBodyWithOpenAIUsage(gin.H{"type": "error", "error": gin.H{"type": "api_error", "code": grokFirstSemanticTimeoutCode, "message": grokFirstSemanticTimeoutMessage}}, usage)
		}
		MarkResponseCommitted(c)
		c.JSON(http.StatusGatewayTimeout, openAIMeteredErrorMetadata(c, payload))
		return true
	}
	MarkResponseCommitted(c)
	switch {
	case strings.Contains(path, "/messages"):
		_, _ = fmt.Fprint(c.Writer, buildAnthropicStreamErrorSSE("api_error", grokFirstSemanticTimeoutMessage, usage))
	case strings.Contains(path, "/chat/completions"):
		data, _ := json.Marshal(payload)
		_, _ = fmt.Fprintf(c.Writer, "data: %s\n\n", data)
	default:
		data, _ := json.Marshal(gin.H{"type": "error", "code": grokFirstSemanticTimeoutCode, "message": grokFirstSemanticTimeoutMessage, "param": nil})
		_, _ = fmt.Fprintf(c.Writer, "event: error\ndata: %s\n\n", openAIErrorSSEWithUsage(data, usage))
	}
	c.Writer.Flush()
	return true
}

func handleGrokWSFirstSemanticTimeout(c *gin.Context, account *Account, err error, usage *OpenAIUsage, write func([]byte) error) bool {
	if c == nil {
		return recordGrokFirstSemanticTimeout(c, account, err)
	}
	handled, _ := c.Get(grokFirstSemanticTimeoutCode)
	if !recordGrokFirstSemanticTimeout(c, account, err) {
		return false
	}
	if handled == true {
		return true
	}
	payload := openAIErrorBodyWithUsage(gin.H{"type": "error", "status": http.StatusGatewayTimeout, "sequence_number": 0, "error": gin.H{"type": "upstream_error", "code": grokFirstSemanticTimeoutCode, "message": grokFirstSemanticTimeoutMessage}}, usage)
	message, _ := json.Marshal(payload)
	if write(message) == nil {
		markOpenAIWSClientVisibleFailure(c, "error", message)
	}
	return true
}
