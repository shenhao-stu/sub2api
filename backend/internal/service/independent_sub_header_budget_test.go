//go:build unit

package service

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Independent public-forwarder fixture: both network hops are real loopback
// HTTP. The downstream transport enforces its own response-header deadline.
// All request/response bytes are synthetic; no paid upstream is contacted.
type independentHeaderHTTP struct {
	client *http.Client
	calls  atomic.Int32
}

func (u *independentHeaderHTTP) Do(r *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	u.calls.Add(1)
	return u.client.Do(r)
}
func (u *independentHeaderHTTP) DoWithTLS(r *http.Request, p string, a int64, n int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(r, p, a, n)
}

type independentHeaderOutcome struct {
	result *OpenAIForwardResult
	err    error
}

const independentHeaderCreated = "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_synthetic\",\"status\":\"in_progress\",\"model\":\"gemini-4-argon\"}}\n\n"
const independentHeaderProgress = "data: {\"type\":\"response.in_progress\",\"response\":{\"id\":\"resp_synthetic\",\"status\":\"in_progress\",\"model\":\"gemini-4-argon\"}}\n\n"
const independentHeaderOpaque = "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"opaque_synthetic\",\"type\":\"reasoning\",\"encrypted_content\":\"SYNTHETIC_OPAQUE_PRIVATE\",\"summary\":[]}}\n\n"
const independentHeaderDelta = "data: {\"type\":\"response.output_text.delta\",\"output_index\":1,\"content_index\":0,\"delta\":\"synthetic visible\"}\n\n"
const independentHeaderUsage = `"usage":{"input_tokens":1000,"output_tokens":7,"input_tokens_details":{"cached_tokens":900}}`
const independentHeaderTerminal = "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_synthetic\",\"status\":\"completed\",\"model\":\"gemini-4-argon\",\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"synthetic visible\"}]}]," + independentHeaderUsage + "}}\n\n"
const independentHeaderEmpty = "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_synthetic\",\"status\":\"completed\",\"model\":\"gemini-4-argon\",\"output\":[],\"usage\":{\"input_tokens\":1000,\"output_tokens\":0,\"input_tokens_details\":{\"cached_tokens\":900}}}}\n\n"

func TestIndependentSubResponseHeaderBudget(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const headerBudget = 500 * time.Millisecond
	for _, mode := range []string{"source_comment", "in_progress", "opaque_reasoning", "visible_control", "true_silence", "prebootstrap_comments", "metered_empty", "activity_then_client_close", "http400_before_stream"} {
		t.Run(mode, func(t *testing.T) {
			sourceDone := make(chan struct{})
			var sourceCanceledBeforeTerminal atomic.Bool
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(sourceDone)
				_, _ = io.Copy(io.Discard, r.Body)
				if r.URL.Path != "/v1/responses" {
					http.Error(w, "synthetic wrong path", 400)
					return
				}
				if mode == "http400_before_stream" {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(400)
					_, _ = io.WriteString(w, `{"error":{"type":"invalid_request_error","message":"synthetic rejected parameter"}}`)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				send := func(s string) bool {
					if _, e := io.WriteString(w, s); e != nil {
						return false
					}
					w.(http.Flusher).Flush()
					return true
				}
				if mode != "prebootstrap_comments" && !send(independentHeaderCreated) {
					return
				}
				wait := func(d time.Duration) bool {
					select {
					case <-r.Context().Done():
						sourceCanceledBeforeTerminal.Store(true)
						return false
					case <-time.After(d):
						return true
					}
				}
				if mode == "true_silence" {
					wait(3 * time.Second)
					return
				}
				if !wait(100 * time.Millisecond) {
					return
				}
				progress := ": upstream-activity\n\n"
				switch mode {
				case "in_progress":
					progress = independentHeaderProgress
				case "opaque_reasoning":
					progress = independentHeaderOpaque
				case "visible_control":
					progress = independentHeaderDelta
				}
				if !send(progress) {
					return
				}
				// The terminal arrives well after the downstream header budget.
				// A valid comment must release headers without releasing content.
				if !wait(800 * time.Millisecond) {
					return
				}
				if mode == "prebootstrap_comments" {
					return
				}
				if mode == "metered_empty" {
					send(independentHeaderEmpty)
					return
				}
				if mode != "visible_control" && !send(independentHeaderDelta) {
					return
				}
				send(independentHeaderTerminal)
			}))
			defer source.Close()
			upstream := &independentHeaderHTTP{client: source.Client()}
			cfg := structuredInputRetryTestConfig()
			cfg.Gateway.StreamDataIntervalTimeout = 1
			cfg.Gateway.StreamKeepaliveInterval = 0
			svc := &OpenAIGatewayService{cfg: cfg, httpUpstream: upstream}
			account := structuredInputRetryTestAccount()
			account.Credentials["base_url"] = source.URL
			account.Credentials["model_mapping"] = map[string]any{"gemini-4-argon": "ultra/gemini-4-argon-high"}
			outcomes := make(chan independentHeaderOutcome, 1)
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, e := io.ReadAll(r.Body)
				if e != nil {
					http.Error(w, "synthetic read failure", 400)
					return
				}
				c, _ := gin.CreateTestContext(w)
				c.Request = r
				result, err := svc.ForwardAsChatCompletions(r.Context(), c, account, body, "", "")
				outcomes <- independentHeaderOutcome{result, err}
			}))
			defer gateway.Close()
			body, e := json.Marshal(map[string]any{"model": "gemini-4-argon", "messages": []map[string]any{{"role": "user", "content": strings.Repeat("synthetic-", 16384)}}, "stream": true})
			require.NoError(t, e)
			require.Greater(t, len(body), 64*1024)
			transport := &http.Transport{ResponseHeaderTimeout: headerBudget}
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, Timeout: 4 * time.Second}
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			req, e := http.NewRequestWithContext(ctx, http.MethodPost, gateway.URL+"/v1/chat/completions", strings.NewReader(string(body)))
			require.NoError(t, e)
			req.Header.Set("Content-Type", "application/json")
			started := time.Now()
			resp, clientErr := client.Do(req)
			headersElapsed := time.Since(started)
			wantTimeout := mode == "true_silence" || mode == "prebootstrap_comments"
			var wire string
			if resp != nil && mode == "activity_then_client_close" {
				_ = resp.Body.Close()
				cancel()
			} else if resp != nil {
				p, readErr := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				wire = string(p)
				require.NoError(t, readErr)
			}
			var outcome independentHeaderOutcome
			select {
			case outcome = <-outcomes:
			case <-time.After(4 * time.Second):
				t.Fatal("forwarder did not terminate")
			}
			select {
			case <-sourceDone:
			case <-time.After(time.Second):
				t.Fatal("upstream producer did not terminate")
			}
			require.EqualValues(t, 1, upstream.calls.Load(), "one input request must cause exactly one upstream attempt")
			if wantTimeout {
				require.Error(t, clientErr)
				var timeoutErr net.Error
				require.ErrorAs(t, clientErr, &timeoutErr)
				require.True(t, timeoutErr.Timeout())
				require.Contains(t, clientErr.Error(), "timeout awaiting response headers")
				require.GreaterOrEqual(t, headersElapsed, headerBudget)
				require.Error(t, outcome.err)
				if outcome.result != nil {
					require.False(t, openAIUsageHasTokens(&outcome.result.Usage))
				}
				require.Empty(t, wire, "no synthetic downstream keepalive or metering")
				t.Logf("headers_timeout=true request_bytes=%d upstream_attempts=1", len(body))
				return
			}
			require.NoError(t, clientErr, "real source activity must arrive before the downstream response-header deadline")
			require.Less(t, headersElapsed, headerBudget)
			if mode == "http400_before_stream" {
				require.Equal(t, 400, resp.StatusCode)
				require.Error(t, outcome.err)
				require.NotContains(t, wire, "upstream-activity")
				require.NotContains(t, wire, "[DONE]")
				return
			}
			require.Equal(t, 200, resp.StatusCode)
			require.NotNil(t, outcome.result)
			require.Equal(t, 1000, outcome.result.Usage.InputTokens)
			require.Equal(t, 900, outcome.result.Usage.CacheReadInputTokens)
			if mode == "activity_then_client_close" {
				require.False(t, sourceCanceledBeforeTerminal.Load(), "downstream close must not cancel the paid attempt before late usage")
				require.Equal(t, 7, outcome.result.Usage.OutputTokens)
				require.Empty(t, wire)
				t.Logf("activity_headers_ms=%d real_client_closed=true late_usage_preserved=true upstream_attempts=1", headersElapsed.Milliseconds())
				return
			}
			require.NotContains(t, wire, "SYNTHETIC_OPAQUE_PRIVATE")
			require.NotContains(t, wire, "opaque_synthetic")
			usageFrames, visibleFrames := 0, 0
			for _, line := range strings.Split(wire, "\n") {
				if !strings.HasPrefix(line, "data: ") {
					continue
				}
				payload := strings.TrimPrefix(line, "data: ")
				if !gjson.Valid(payload) {
					continue
				}
				require.NotContains(t, payload, "upstream-activity", "transport activity must not be visible Chat content")
				if strings.Contains(payload, "synthetic visible") {
					visibleFrames++
				}
				u := gjson.Get(payload, "usage")
				if u.Exists() && u.Type != gjson.Null {
					usageFrames++
					if mode == "metered_empty" {
						// Metered errors retain the established OpenAIUsage schema;
						// successful Chat chunks use Chat Completions usage fields.
						require.EqualValues(t, 1000, u.Get("input_tokens").Int())
						require.EqualValues(t, 900, u.Get("cache_read_input_tokens").Int())
						require.EqualValues(t, 0, u.Get("output_tokens").Int())
						require.EqualValues(t, 502, gjson.Get(payload, "error.status_code").Int())
					} else {
						require.EqualValues(t, 1000, u.Get("prompt_tokens").Int())
						require.EqualValues(t, 900, u.Get("prompt_tokens_details.cached_tokens").Int())
						require.EqualValues(t, 7, u.Get("completion_tokens").Int())
					}
				}
			}
			require.Equal(t, 1, usageFrames, "one authoritative positive usage frame; activity cannot invent zero usage")
			if mode == "metered_empty" {
				require.Error(t, outcome.err)
				require.Equal(t, 0, outcome.result.Usage.OutputTokens)
				require.Contains(t, wire, "upstream_empty_completion")
				require.Zero(t, visibleFrames)
				require.NotContains(t, wire, "[DONE]", "empty output cannot become successful because headers were released")
			} else {
				require.NoError(t, outcome.err)
				require.Equal(t, 7, outcome.result.Usage.OutputTokens)
				require.Equal(t, 1, visibleFrames)
				require.Equal(t, 1, strings.Count(wire, "data: [DONE]"))
			}
			t.Logf("headers_ms=%d total_ms=%d request_bytes=%d upstream_attempts=1 input=1000 cache=900 output=%d", headersElapsed.Milliseconds(), time.Since(started).Milliseconds(), len(body), outcome.result.Usage.OutputTokens)
		})
	}
}

// A real downstream socket read deadline detects post-header silence. It is
// longer than the activity limiter's one-second interval, and only reset when
// real response bytes are read; a final batch of comments cannot pass this test.
type independentHeaderIdleConn struct {
	net.Conn
	bodyPhase *atomic.Bool
	idle      time.Duration
}

func (c *independentHeaderIdleConn) Read(p []byte) (int, error) {
	if c.bodyPhase.Load() {
		_ = c.Conn.SetReadDeadline(time.Now().Add(c.idle))
	}
	return c.Conn.Read(p)
}

func TestIndependentSubPostVisibleBodyIdle(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const readIdle = 1500 * time.Millisecond
	for _, mode := range []string{"opaque_activity", "in_progress_activity", "true_silence", "visible_control"} {
		t.Run(mode, func(t *testing.T) {
			sourceDone := make(chan struct{})
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(sourceDone)
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "text/event-stream")
				send := func(s string) bool {
					if _, e := io.WriteString(w, s); e != nil {
						return false
					}
					w.(http.Flusher).Flush()
					return true
				}
				if !send(independentHeaderCreated) || !send(independentHeaderDelta) {
					return
				}
				for i := 0; i < 12; i++ {
					select {
					case <-r.Context().Done():
						return
					case <-time.After(250 * time.Millisecond):
					}
					switch mode {
					case "opaque_activity":
						if !send(independentHeaderOpaque) {
							return
						}
					case "in_progress_activity":
						if !send(independentHeaderProgress) {
							return
						}
					case "visible_control":
						if !send(independentHeaderDelta) {
							return
						}
					}
				}
				send(independentHeaderTerminal)
			}))
			defer source.Close()
			upstream := &independentHeaderHTTP{client: source.Client()}
			cfg := structuredInputRetryTestConfig()
			cfg.Gateway.StreamDataIntervalTimeout = 4
			cfg.Gateway.StreamKeepaliveInterval = 0
			svc := &OpenAIGatewayService{cfg: cfg, httpUpstream: upstream}
			account := structuredInputRetryTestAccount()
			account.Credentials["base_url"] = source.URL
			account.Credentials["model_mapping"] = map[string]any{"gemini-4-argon": "ultra/gemini-4-argon-high"}
			outcomes := make(chan independentHeaderOutcome, 1)
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, e := io.ReadAll(r.Body)
				if e != nil {
					http.Error(w, "synthetic read failure", 400)
					return
				}
				c, _ := gin.CreateTestContext(w)
				c.Request = r
				result, err := svc.ForwardAsChatCompletions(r.Context(), c, account, body, "", "")
				outcomes <- independentHeaderOutcome{result, err}
			}))
			defer gateway.Close()
			body, e := json.Marshal(map[string]any{"model": "gemini-4-argon", "messages": []map[string]any{{"role": "user", "content": strings.Repeat("synthetic-", 16384)}}, "stream": true})
			require.NoError(t, e)
			var bodyPhase atomic.Bool
			transport := &http.Transport{ResponseHeaderTimeout: 500 * time.Millisecond, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				conn, e := (&net.Dialer{}).DialContext(ctx, network, address)
				if e != nil {
					return nil, e
				}
				return &independentHeaderIdleConn{Conn: conn, bodyPhase: &bodyPhase, idle: readIdle}, nil
			}}
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, Timeout: 6 * time.Second}
			req, e := http.NewRequest(http.MethodPost, gateway.URL+"/v1/chat/completions", strings.NewReader(string(body)))
			require.NoError(t, e)
			req.Header.Set("Content-Type", "application/json")
			started := time.Now()
			resp, e := client.Do(req)
			require.NoError(t, e, "the initial visible delta must release response headers")
			require.Equal(t, 200, resp.StatusCode)
			bodyPhase.Store(true)
			data, readErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			readElapsed := time.Since(started)
			wire := string(data)
			var outcome independentHeaderOutcome
			select {
			case outcome = <-outcomes:
			case <-time.After(5 * time.Second):
				t.Fatal("forwarder did not finish draining the synthetic terminal")
			}
			select {
			case <-sourceDone:
			case <-time.After(time.Second):
				t.Fatal("synthetic source did not finish")
			}
			require.EqualValues(t, 1, upstream.calls.Load())
			require.NotNil(t, outcome.result)
			require.Equal(t, 1000, outcome.result.Usage.InputTokens)
			require.Equal(t, 900, outcome.result.Usage.CacheReadInputTokens)
			require.Equal(t, 7, outcome.result.Usage.OutputTokens, "late positive usage remains even if downstream closes")
			require.NotContains(t, wire, "SYNTHETIC_OPAQUE_PRIVATE")
			if mode == "true_silence" {
				require.Error(t, readErr)
				var timeoutErr net.Error
				require.ErrorAs(t, readErr, &timeoutErr)
				require.True(t, timeoutErr.Timeout())
				require.GreaterOrEqual(t, readElapsed, readIdle)
				require.Less(t, readElapsed, 2700*time.Millisecond)
				require.NotContains(t, wire, "upstream-activity", "no timer may hide a silent upstream")
				require.NotContains(t, wire, "[DONE]")
				t.Logf("real_body_idle_timeout=true elapsed_ms=%d late_usage_preserved=true upstream_attempts=1", readElapsed.Milliseconds())
				return
			}
			require.NoError(t, readErr, "post-visible native activity must reach the real downstream reader before its idle deadline")
			require.NoError(t, outcome.err)
			require.GreaterOrEqual(t, readElapsed, 3*time.Second)
			comments := strings.Count(wire, ": upstream-activity\n\n")
			if mode == "visible_control" {
				require.Zero(t, comments, "visible deltas must not acquire extra activity frames")
			} else {
				require.GreaterOrEqual(t, comments, 2)
				require.LessOrEqual(t, comments, 4)
			}
			usageFrames, visibleFrames := 0, 0
			for _, line := range strings.Split(wire, "\n") {
				if !strings.HasPrefix(line, "data: ") {
					continue
				}
				payload := strings.TrimPrefix(line, "data: ")
				if !gjson.Valid(payload) {
					continue
				}
				require.NotContains(t, payload, "upstream-activity")
				if strings.Contains(payload, "synthetic visible") {
					visibleFrames++
				}
				u := gjson.Get(payload, "usage")
				if u.Exists() && u.Type != gjson.Null {
					usageFrames++
					require.EqualValues(t, 1000, u.Get("prompt_tokens").Int())
					require.EqualValues(t, 900, u.Get("prompt_tokens_details.cached_tokens").Int())
					require.EqualValues(t, 7, u.Get("completion_tokens").Int())
				}
			}
			require.Equal(t, 1, usageFrames)
			if mode == "visible_control" {
				require.Equal(t, 13, visibleFrames)
			} else {
				require.Equal(t, 1, visibleFrames)
			}
			require.Equal(t, 1, strings.Count(wire, "data: [DONE]"))
			t.Logf("read_ms=%d progress_comments=%d semantic_chunks=%d usage_frames=%d upstream_attempts=1", readElapsed.Milliseconds(), comments, visibleFrames, usageFrames)
		})
	}
}
