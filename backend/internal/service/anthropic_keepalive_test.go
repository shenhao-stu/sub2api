package service

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type anthropicWaitingUpstream struct {
	release     <-chan struct{}
	response    *http.Response
	calls       atomic.Int32
	requestBody []byte
}

func (u *anthropicWaitingUpstream) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	u.calls.Add(1)
	if req.Body != nil {
		u.requestBody, _ = io.ReadAll(req.Body)
		_ = req.Body.Close()
	}
	select {
	case <-u.release:
		return u.response, nil
	case <-req.Context().Done():
		return nil, req.Context().Err()
	}
}

func TestAnthropicNonstreamKeepalivePreservesOneJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, passthrough := range []bool{false, true} {
		for _, stage := range []string{"headers", "body"} {
			for _, upstreamError := range []bool{false, true} {
				name := fmt.Sprintf("passthrough=%t/%s/error=%t", passthrough, stage, upstreamError)
				t.Run(name, func(t *testing.T) {
					release := make(chan struct{})
					var once sync.Once
					unblock := func() { once.Do(func() { close(release) }) }
					payload := `{"type":"message","id":"msg_real","model":"claude-sonnet-4-5","content":[{"type":"text","text":"hello"}],"usage":{"input_tokens":7,"output_tokens":4}}`
					status := http.StatusOK
					if upstreamError {
						payload = `{"type":"error","error":{"type":"invalid_request_error","message":"invalid parameter"}}`
						status = http.StatusBadRequest
					}
					body := io.NopCloser(strings.NewReader(payload))
					headers := release
					if stage == "body" {
						ready := make(chan struct{})
						close(ready)
						headers = ready
						reader, writer := io.Pipe()
						body = reader
						go func() {
							defer writer.Close()
							_, _ = io.WriteString(writer, payload[:10])
							<-release
							_, _ = io.WriteString(writer, payload[10:])
						}()
					}
					upstream := &anthropicWaitingUpstream{release: headers, response: &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, Body: body}}
					svc := newForwardPartialUsageServiceForTest(&anthropicHTTPUpstreamRecorder{})
					svc.cfg.Gateway.AnthropicNonstreamKeepaliveInterval = 1
					svc.httpUpstream = upstream
					account := newAnthropicOAuthAccountForPartialUsageTest()
					if passthrough {
						account = newAnthropicAPIKeyAccountForTest()
					}
					type outcome struct {
						result    *ForwardResult
						err       error
						adjusted  int
						committed bool
					}
					finished := make(chan outcome, 1)
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						c, _ := gin.CreateTestContext(w)
						c.Request = r
						parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(`{"model":"claude-sonnet-4-5","stream":false,"messages":[{"role":"user","content":"hello"}]}`)), PlatformAnthropic)
						if err != nil {
							finished <- outcome{err: err}
							return
						}
						result, err := svc.Forward(r.Context(), c, account, parsed)
						finished <- outcome{result, err, AnthropicKeepaliveAdjustedWrittenSize(c), IsResponseCommitted(c)}
					}))
					defer server.Close()
					defer unblock()
					resp, err := (&http.Client{Timeout: 3 * time.Second}).Post(server.URL+"/v1/messages", "application/json", nil)
					require.NoError(t, err)
					defer resp.Body.Close()
					require.Equal(t, 200, resp.StatusCode)
					require.Contains(t, resp.Header.Get("Content-Type"), "application/json")
					reader := bufio.NewReader(resp.Body)
					first, err := reader.ReadString('\n')
					require.NoError(t, err)
					require.Equal(t, " \n", first)
					unblock()
					tail, err := io.ReadAll(reader)
					require.NoError(t, err)
					wire := first + string(tail)
					require.NotContains(t, wire, "event:")
					decoder := json.NewDecoder(strings.NewReader(wire))
					var document map[string]any
					require.NoError(t, decoder.Decode(&document))
					require.ErrorIs(t, decoder.Decode(new(any)), io.EOF)
					got := <-finished
					require.False(t, gjson.GetBytes(upstream.requestBody, "stream").Bool(), "must not convert nonstream into stream")
					require.Equal(t, int32(1), upstream.calls.Load())
					if upstreamError {
						require.Error(t, got.err)
						require.Nil(t, got.result)
						require.True(t, got.committed)
						require.Equal(t, "error", document["type"])
					} else {
						require.NoError(t, got.err)
						require.NotNil(t, got.result)
						require.Equal(t, 7, got.result.Usage.InputTokens)
						require.Equal(t, 4, got.result.Usage.OutputTokens)
					}
				})
			}
		}
	}
}

func TestAnthropicKeepaliveFastErrorPreservesHTTP(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, passthrough := range []bool{false, true} {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			payload := `{"type":"error","error":{"type":"invalid_request_error","message":"invalid parameter"}}`
			upstream := &anthropicHTTPUpstreamRecorder{resp: &http.Response{StatusCode: 400, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(payload))}}
			svc := newForwardPartialUsageServiceForTest(upstream)
			svc.cfg.Gateway.StreamKeepaliveInterval = 1
			svc.cfg.Gateway.AnthropicNonstreamKeepaliveInterval = 1
			account := newAnthropicOAuthAccountForPartialUsageTest()
			if passthrough {
				account = newAnthropicAPIKeyAccountForTest()
			}
			parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(fmt.Sprintf(`{"model":"claude-sonnet-4-5","stream":%t,"messages":[{"role":"user","content":"hello"}]}`, stream))), PlatformAnthropic)
			require.NoError(t, err)
			result, err := svc.Forward(context.Background(), c, account, parsed)
			require.Error(t, err)
			require.Nil(t, result)
			require.Equal(t, 400, rec.Code)
			require.JSONEq(t, payload, rec.Body.String())
			require.False(t, AnthropicJSONKeepaliveCommitted(c))
			require.False(t, AnthropicStreamKeepaliveCommitted(c))
		}
	}
}

func (u *anthropicWaitingUpstream) DoWithTLS(req *http.Request, proxy string, id int64, concurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(req, proxy, id, concurrency)
}

func TestAnthropicKeepaliveBeforeUpstreamHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, passthrough := range []bool{false, true} {
		name := "native"
		if passthrough {
			name = "apikey_passthrough"
		}
		t.Run(name, func(t *testing.T) {
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			upstream := &anthropicWaitingUpstream{release: release, response: &http.Response{
				StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}},
				Body: io.NopCloser(strings.NewReader("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":3}}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":2}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")),
			}}
			svc := newForwardPartialUsageServiceForTest(&anthropicHTTPUpstreamRecorder{})
			svc.cfg.Gateway.StreamKeepaliveInterval = 1
			svc.httpUpstream = upstream
			account := newAnthropicOAuthAccountForPartialUsageTest()
			if passthrough {
				account = newAnthropicAPIKeyAccountForTest()
			}
			type outcome struct {
				result *ForwardResult
				err    error
			}
			finished := make(chan outcome, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c, _ := gin.CreateTestContext(w)
				c.Request = r
				parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(`{"model":"claude-sonnet-4-5","stream":true,"messages":[{"role":"user","content":"hello"}]}`)), PlatformAnthropic)
				if err != nil {
					finished <- outcome{err: err}
					return
				}
				result, err := svc.Forward(r.Context(), c, account, parsed)
				finished <- outcome{result, err}
			}))
			defer server.Close()
			defer unblock()
			client := &http.Client{Timeout: 2500 * time.Millisecond}
			resp, err := client.Post(server.URL+"/v1/messages", "application/json", nil)
			require.NoError(t, err, "the downstream must receive a ping while the upstream has sent no headers")
			defer resp.Body.Close()
			reader := bufio.NewReader(resp.Body)
			first, err := reader.ReadString('\n')
			require.NoError(t, err)
			require.Equal(t, "event: ping\n", first)
			unblock()
			tail, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Contains(t, string(tail), "event: message_stop\n")
			got := <-finished
			require.NoError(t, got.err)
			require.NotNil(t, got.result)
			require.Equal(t, 3, got.result.Usage.InputTokens)
			require.Equal(t, 2, got.result.Usage.OutputTokens)
			require.Equal(t, int32(1), upstream.calls.Load())
		})
	}
}

func TestAnthropicUpstreamPingDoesNotSetFirstToken(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, passthrough := range []bool{false, true} {
		svc := newStreamingResponseTestGatewayService()
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
		resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("event: ping\ndata: {\"type\":\"ping\"}\n\n"))}
		var result *streamingResult
		if passthrough {
			result, _ = svc.handleStreamingResponseAnthropicAPIKeyPassthrough(context.Background(), resp, c, &Account{ID: 1}, time.Now(), "claude-sonnet-4-5")
		} else {
			result, _ = svc.handleStreamingResponse(context.Background(), resp, c, &Account{ID: 1}, time.Now(), "claude-sonnet-4-5", "claude-sonnet-4-5", false)
		}
		require.NotNil(t, result)
		require.Nil(t, result.firstTokenMs, "upstream transport ping is not the first model response")
		require.False(t, result.usage.hasObservedTokens())
	}
}

func TestAnthropicKeepaliveLongNonstreamHeaderWait(t *testing.T) {
	if os.Getenv("SUB2API_TEST_LONG_KEEPALIVE") != "1" {
		t.Skip("explicit 130-second loopback experiment")
	}
	gin.SetMode(gin.TestMode)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	upstream := &anthropicWaitingUpstream{release: release, response: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"type":"message","content":[{"type":"text","text":"hello"}],"usage":{"input_tokens":7,"output_tokens":4}}`))}}
	svc := newForwardPartialUsageServiceForTest(&anthropicHTTPUpstreamRecorder{})
	svc.httpUpstream = upstream
	svc.cfg.Gateway.AnthropicNonstreamKeepaliveInterval = 10
	account := newAnthropicAPIKeyAccountForTest()
	account.Extra["anthropic_passthrough"] = false
	type outcome struct {
		result *ForwardResult
		err    error
	}
	done := make(chan outcome, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, _ := gin.CreateTestContext(w)
		c.Request = r
		parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(`{"model":"claude-sonnet-4-5","stream":false,"messages":[{"role":"user","content":"hello"}]}`)), PlatformAnthropic)
		if err != nil {
			done <- outcome{err: err}
			return
		}
		result, err := svc.Forward(r.Context(), c, account, parsed)
		done <- outcome{result, err}
	}))
	defer server.Close()
	defer unblock()
	started := time.Now()
	timer := time.AfterFunc(130*time.Second, unblock)
	defer timer.Stop()
	resp, err := (&http.Client{Timeout: 145 * time.Second}).Post(server.URL+"/v1/messages", "application/json", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	headerDelay := time.Since(started)
	require.Less(t, headerDelay, 30*time.Second)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.GreaterOrEqual(t, time.Since(started), 130*time.Second)
	require.True(t, json.Valid(body))
	require.True(t, strings.HasPrefix(string(body), " \n"))
	got := <-done
	require.NoError(t, got.err)
	require.NotNil(t, got.result)
	require.Equal(t, 7, got.result.Usage.InputTokens)
	require.Equal(t, 4, got.result.Usage.OutputTokens)
	require.Equal(t, int32(1), upstream.calls.Load())
	require.False(t, gjson.GetBytes(upstream.requestBody, "stream").Bool())
	t.Logf("actual header wait=%s total=%s bytes=%d calls=1 input=7 output=4", headerDelay, time.Since(started), len(body))
}

func TestAnthropicLateErrorAlwaysOneTypedDocument(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, body := range []string{"", "<html>upstream failed</html>", "{bad", `{"error":"invalid request"}`, "{\n \"type\": \"error\", \"error\": {\"type\": \"invalid_request_error\", \"message\": \"bad\"}}"} {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			prefix := " \n"
			contentType := "application/json"
			if stream {
				prefix = anthropicPingFrame
				contentType = "text/event-stream"
			}
			c.Header("Content-Type", contentType)
			_, _ = c.Writer.WriteString(prefix)
			c.Writer.Flush()
			writeAnthropicErrorBody(c, 400, []byte(body))
			wire := strings.TrimPrefix(rec.Body.String(), prefix)
			if stream {
				require.True(t, strings.HasPrefix(wire, "event: error\ndata: "))
				wire = strings.TrimPrefix(wire, "event: error\ndata: ")
			}
			var payload map[string]any
			decoder := json.NewDecoder(strings.NewReader(wire))
			require.NoError(t, decoder.Decode(&payload))
			require.ErrorIs(t, decoder.Decode(new(any)), io.EOF)
			require.Equal(t, "error", payload["type"])
			require.NotEmpty(t, payload["error"].(map[string]any)["type"])
			require.Equal(t, 200, rec.Code)
			require.True(t, IsResponseCommitted(c))
		}
	}
}

func TestAnthropicKeepaliveScopeAndWrittenBytes(t *testing.T) {
	svc := newStreamingResponseTestGatewayService()
	svc.cfg.Gateway.StreamKeepaliveInterval = 10
	svc.cfg.Gateway.AnthropicNonstreamKeepaliveInterval = 10
	for _, tc := range []struct {
		path, platform  string
		stream, enabled bool
	}{
		{"/v1/messages", PlatformAnthropic, true, true}, {"/v1/messages", PlatformAnthropic, false, true},
		{"/v1/responses", PlatformAnthropic, true, false}, {"/v1/messages/count_tokens", PlatformAnthropic, false, false}, {"/v1/messages", PlatformGrok, true, false},
	} {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, tc.path, nil)
		svc.initAnthropicKeepalive(c, &Account{Platform: tc.platform}, tc.stream)
		require.Equal(t, tc.enabled, anthropicKeepaliveFromContext(c) != nil)
		if !tc.enabled {
			continue
		}
		before := AnthropicKeepaliveAdjustedWrittenSize(c)
		n, _ := c.Writer.WriteString(anthropicPingFrame)
		recordAnthropicKeepalive(c, n)
		require.Equal(t, before, AnthropicKeepaliveAdjustedWrittenSize(c), "own ping must not suppress safe failover")
		_, _ = c.Writer.WriteString(anthropicPingFrame)
		require.Greater(t, AnthropicKeepaliveAdjustedWrittenSize(c), 0, "upstream bytes must never be deducted")
	}
}
