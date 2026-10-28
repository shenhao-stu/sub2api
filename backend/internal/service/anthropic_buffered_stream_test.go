//go:build unit

package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const bufferedAnthropicFixture = `event: message_start
data: {"type":"message_start","message":{"id":"msg_test","type":"message","role":"assistant","model":"claude-fable-5-1","content":[],"usage":{"input_tokens":11,"cache_read_input_tokens":17,"cache_creation_input_tokens":5,"cache_creation":{"ephemeral_5m_input_tokens":2,"ephemeral_1h_input_tokens":3}}}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":""}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"synthetic thought"}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"signed-fixture"}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: content_block_start
data: {"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"tool_fixture","name":"lookup","input":{}}}

event: content_block_delta
data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"n\":"}}

event: content_block_delta
data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"9007199254740993}"}}

event: content_block_stop
data: {"type":"content_block_stop","index":1}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"output_tokens":23,"output_tokens_details":{"thinking_tokens":7}}}

event: message_stop
data: {"type":"message_stop"}

`

type bufferedOnlyUpstream struct {
	anthropicHTTPUpstreamRecorder
	calls int
}

func (u *bufferedOnlyUpstream) DoWithTLS(req *http.Request, proxy string, id int64, concurrency int, profile *tlsfingerprint.Profile) (*http.Response, error) {
	u.calls++
	resp, err := u.Do(req, proxy, id, concurrency)
	if !gjson.GetBytes(u.lastBody, "stream").Bool() {
		return nil, &HTTPUpstreamAttemptError{Err: io.EOF, RequestMayHaveBeenSent: true}
	}
	return resp, err
}

func TestAnthropicBufferedStreamForwardKeepsJSONAndKnownUsage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	account := newAnthropicAPIKeyAccountForTest()
	account.Extra["anthropic_buffered_stream_models"] = []any{"claude-fable-5-1"}
	upstream := &bufferedOnlyUpstream{anthropicHTTPUpstreamRecorder: anthropicHTTPUpstreamRecorder{resp: &http.Response{
		StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(bufferedAnthropicFixture)),
	}}}
	svc := &GatewayService{cfg: &config.Config{}, httpUpstream: upstream, rateLimitService: &RateLimitService{}}
	body := []byte(`{"model":"claude-fable-5-1","stream":false,"max_tokens":1024,"messages":[{"role":"user","content":"synthetic"}]}`)
	result, err := svc.forwardAnthropicAPIKeyPassthrough(context.Background(), c, account, body, "claude-fable-5-1", "public-fable", false, time.Now())
	require.NoError(t, err)
	require.Equal(t, 1, upstream.calls, "only one paid dispatch")
	require.NotNil(t, result)
	require.False(t, result.Stream, "client still requested JSON")
	require.Equal(t, 11, result.Usage.InputTokens)
	require.Equal(t, 23, result.Usage.OutputTokens)
	require.Equal(t, 17, result.Usage.CacheReadInputTokens)
	require.Equal(t, 5, result.Usage.CacheCreationInputTokens)
	require.Contains(t, rec.Header().Get("Content-Type"), "application/json")
	require.True(t, gjson.Valid(rec.Body.String()))
	require.Equal(t, "signed-fixture", gjson.Get(rec.Body.String(), "content.0.signature").String())
	require.Equal(t, "9007199254740993", gjson.Get(rec.Body.String(), "content.1.input.n").Raw)
	require.Equal(t, int64(7), gjson.Get(rec.Body.String(), "usage.output_tokens_details.thinking_tokens").Int())
}

func TestAnthropicBufferedStreamDoesNotChangeUnselectedModels(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	a := newAnthropicAPIKeyAccountForTest()
	a.Extra["anthropic_buffered_stream_models"] = []any{"claude-fable-5-1"}
	u := &bufferedOnlyUpstream{}
	s := &GatewayService{cfg: &config.Config{}, httpUpstream: u, rateLimitService: &RateLimitService{}}
	_, err := s.forwardAnthropicAPIKeyPassthrough(context.Background(), c, a, []byte(`{"model":"other","stream":false}`), "other", "other", false, time.Now())
	require.Error(t, err)
	require.Equal(t, 1, u.calls)
	require.False(t, gjson.GetBytes(u.lastBody, "stream").Bool())
	var failover *UpstreamFailoverError
	require.False(t, errors.As(err, &failover))
}

func TestAnthropicBufferedStreamRejectsIncompleteOrInvalidFinals(t *testing.T) {
	for name, payload := range map[string]string{
		"no terminal":          strings.ReplaceAll(bufferedAnthropicFixture, "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n", ""),
		"no final output":      strings.ReplaceAll(bufferedAnthropicFixture, `"output_tokens":23,`, ""),
		"null final output":    strings.ReplaceAll(bufferedAnthropicFixture, `"output_tokens":23`, `"output_tokens":null`),
		"missing input":        strings.ReplaceAll(bufferedAnthropicFixture, `"input_tokens":11,`, ""),
		"null input":           strings.ReplaceAll(bufferedAnthropicFixture, `"input_tokens":11`, `"input_tokens":null`),
		"invalid tool JSON":    strings.ReplaceAll(bufferedAnthropicFixture, `9007199254740993}`, `broken}`),
		"unclosed block":       strings.ReplaceAll(bufferedAnthropicFixture, "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":1}\n\n", ""),
		"invalid index":        strings.ReplaceAll(bufferedAnthropicFixture, `"index":1`, `"index":3`),
		"identity replacement": strings.ReplaceAll(bufferedAnthropicFixture, `"stop_reason":"tool_use"`, `"model":"different","stop_reason":"tool_use"`),
	} {
		t.Run(name, func(t *testing.T) {
			m, err := collectAnthropicBufferedStream(strings.NewReader(payload), 1<<20)
			require.Error(t, err)
			require.False(t, m.terminal)
			require.True(t, m.usage.hasObservedTokens(), "known provider usage survives a failed final")
		})
	}
}

func TestAnthropicBufferedStreamCompletesWithoutWaitingForTransportEOF(t *testing.T) {
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	go func() { _, _ = io.WriteString(writer, bufferedAnthropicFixture) }()
	done := make(chan error, 1)
	go func() { _, err := collectAnthropicBufferedStream(reader, 1<<20); done <- err }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("terminal response waited for an unnecessary HTTP EOF")
	}
}

func TestAnthropicBufferedStreamLimitsMemoryAndKeepsPartialUsage(t *testing.T) {
	m, err := collectAnthropicBufferedStream(strings.NewReader(bufferedAnthropicFixture), 600)
	require.ErrorIs(t, err, ErrUpstreamResponseBodyTooLarge)
	require.Equal(t, 11, m.usage.InputTokens)
}

func TestAnthropicBufferedStreamErrorKeepsStatusNoReplayAndHidesProvider(t *testing.T) {
	start := strings.Split(bufferedAnthropicFixture, "event: content_block_start")[0]
	payload := start + "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"server_error\",\"message\":\"http://private-provider.invalid/sensitive\"},\"status_code\":503}\n\n"
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	a := newAnthropicAPIKeyAccountForTest()
	a.Extra["anthropic_buffered_stream_models"] = []any{"claude-fable-5-1"}
	u := &bufferedOnlyUpstream{anthropicHTTPUpstreamRecorder: anthropicHTTPUpstreamRecorder{resp: &http.Response{
		StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(payload)),
	}}}
	s := &GatewayService{cfg: &config.Config{}, httpUpstream: u, rateLimitService: &RateLimitService{}}
	result, err := s.forwardAnthropicAPIKeyPassthrough(context.Background(), c, a, []byte(`{"model":"claude-fable-5-1","stream":false}`), "claude-fable-5-1", "claude-fable-5-1", false, time.Now())
	require.Error(t, err)
	require.Equal(t, 1, u.calls)
	require.Equal(t, 503, rec.Code)
	require.True(t, IsResponseCommitted(c))
	require.NotContains(t, rec.Body.String(), "private-provider")
	require.NotContains(t, rec.Body.String(), "http:")
	require.NotNil(t, result)
	require.Equal(t, 11, result.Usage.InputTokens)
	require.False(t, result.Stream)
}

func TestAnthropicBufferedStreamKeepsParsedClientModeAndCacheBilling(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	a := newAnthropicAPIKeyAccountForTest()
	a.Extra["anthropic_buffered_stream_models"] = []string{"claude-fable-5-1"}
	u := &bufferedOnlyUpstream{anthropicHTTPUpstreamRecorder: anthropicHTTPUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(bufferedAnthropicFixture))}}}
	s := &GatewayService{cfg: &config.Config{}, httpUpstream: u, rateLimitService: &RateLimitService{}}
	body := []byte(`{"model":"claude-fable-5-1","stream":false,"messages":[{"role":"user","content":"synthetic"}]}`)
	parsed, err := ParseGatewayRequest(NewRequestBodyRef(body), PlatformAnthropic)
	require.NoError(t, err)
	result, err := s.forwardAnthropicAPIKeyPassthroughWithInput(WithForceCacheBilling(context.Background()), c, a, anthropicPassthroughForwardInput{Body: body, Parsed: parsed, RequestModel: parsed.Model, OriginalModel: parsed.Model, StartTime: time.Now()})
	require.NoError(t, err)
	require.False(t, parsed.Stream)
	require.False(t, result.Stream)
	require.True(t, gjson.GetBytes(u.lastBody, "stream").Bool())
	require.Equal(t, 11, result.Usage.InputTokens)
	require.Equal(t, 17, result.Usage.CacheReadInputTokens)
	require.EqualValues(t, 28, gjson.Get(rec.Body.String(), "usage.cache_read_input_tokens").Int())
}

func TestAnthropicBufferedStreamRejectsInvalidLateCounterAndMismatchedDelta(t *testing.T) {
	for _, payload := range []string{
		strings.Replace(bufferedAnthropicFixture, "event: message_stop", "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{},\"usage\":{\"output_tokens\":null}}\n\nevent: message_stop", 1),
		strings.Replace(bufferedAnthropicFixture, `"type":"thinking_delta","thinking":"synthetic thought"`, `"type":"text_delta","text":"wrong block"`, 1),
	} {
		m, err := collectAnthropicBufferedStream(strings.NewReader(payload), 1<<20)
		require.Error(t, err)
		require.False(t, m.terminal)
	}
}

func TestAnthropicBufferedStreamCancellationClosesBody(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil).WithContext(ctx)
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	a := newAnthropicAPIKeyAccountForTest()
	a.Extra["anthropic_buffered_stream_models"] = []string{"claude-fable-5-1"}
	u := &bufferedOnlyUpstream{anthropicHTTPUpstreamRecorder: anthropicHTTPUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: reader}}}
	s := newForwardPartialUsageServiceForTest(&anthropicHTTPUpstreamRecorder{})
	s.httpUpstream = u
	s.cfg.Gateway.AnthropicNonstreamKeepaliveInterval = 1
	done := make(chan error, 1)
	go func() {
		_, err := s.forwardAnthropicAPIKeyPassthrough(ctx, c, a, []byte(`{"model":"claude-fable-5-1","stream":false}`), "claude-fable-5-1", "claude-fable-5-1", false, time.Now())
		done <- err
	}()
	start := strings.Split(bufferedAnthropicFixture, "event: content_block_start")[0]
	_, err := io.WriteString(writer, start)
	require.NoError(t, err)
	cancel()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("canceled reader did not close")
	}
	require.Equal(t, 1, u.calls)
}
