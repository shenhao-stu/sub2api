//go:build unit

package service

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type accountTestEOFReader struct {
	reader io.Reader
	eof    bool
}

func (r *accountTestEOFReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if err == io.EOF {
		r.eof = true
	}
	return n, err
}

type accountTestReadError struct{}

func (accountTestReadError) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestAccountTestClaudeStreamDrainsBeforeCompletion(t *testing.T) {
	for _, terminal := range []string{`{"type":"message_stop"}`, `[DONE]`} {
		t.Run(terminal, func(t *testing.T) {
			wire := "data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"hello\"}}\n\ndata: " + terminal + "\n\n"
			reader := &accountTestEOFReader{reader: strings.NewReader(wire)}
			c, recorder := newTestContext()
			err := (&AccountTestService{}).processClaudeStream(c, io.NopCloser(reader))
			require.NoError(t, err)
			require.True(t, reader.eof, "returning before HTTP EOF closes the upstream before its final usage/trailers")
			require.Equal(t, 1, strings.Count(recorder.Body.String(), `"test_complete"`))
			require.Contains(t, recorder.Body.String(), `"hello"`)
		})
	}
}

func TestAccountTestClaudeStreamRejectsMissingTerminal(t *testing.T) {
	for _, wire := range []string{"", "<html>not an SSE response</html>\n", "data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"partial\"}}\n\n"} {
		t.Run(wire, func(t *testing.T) {
			c, recorder := newTestContext()
			err := (&AccountTestService{}).processClaudeStream(c, io.NopCloser(strings.NewReader(wire)))
			require.Error(t, err)
			require.NotContains(t, recorder.Body.String(), `"test_complete"`)
		})
	}
}

func TestAccountTestClaudeStreamChecksTailErrors(t *testing.T) {
	for _, tail := range []io.Reader{
		strings.NewReader("data: {\"type\":\"error\",\"error\":{\"message\":\"late upstream failure\"}}\n\n"),
		accountTestReadError{},
	} {
		c, recorder := newTestContext()
		body := io.NopCloser(io.MultiReader(strings.NewReader("data: {\"type\":\"message_stop\"}\n\n"), tail))
		err := (&AccountTestService{}).processClaudeStream(c, body)
		require.Error(t, err)
		require.NotContains(t, recorder.Body.String(), `"test_complete"`)
	}
}

func TestAccountTestClaudeStreamHandlesFinalLineWithoutNewline(t *testing.T) {
	c, recorder := newTestContext()
	err := (&AccountTestService{}).processClaudeStream(c, io.NopCloser(strings.NewReader("data: {\"type\":\"message_stop\"}")))
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(recorder.Body.String(), `"test_complete"`))
}

func TestAccountTestClaudeStreamReadFailureBeforeTerminal(t *testing.T) {
	c, recorder := newTestContext()
	err := (&AccountTestService{}).processClaudeStream(c, io.NopCloser(accountTestReadError{}))
	require.Error(t, err)
	require.Contains(t, recorder.Body.String(), `"error"`)
	require.NotContains(t, recorder.Body.String(), `"test_complete"`)
}

func TestAccountTestClaudeStreamBoundsTerminalTail(t *testing.T) {
	for _, complete := range []bool{false, true} {
		t.Run(map[bool]string{false: "timeout", true: "delayed_usage"}[complete], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				reader, writer := io.Pipe()
				defer reader.Close()
				defer writer.Close()
				go func() {
					_, _ = io.WriteString(writer, "data: {\"type\":\"message_stop\"}\n\n")
					if complete {
						time.Sleep(time.Second)
						_, _ = io.WriteString(writer, "data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":12}}\n\n")
						_ = writer.Close()
					}
				}()
				c, recorder := newTestContext()
				start := time.Now()
				err := (&AccountTestService{}).processClaudeStream(c, reader)
				if complete {
					require.NoError(t, err)
					require.Equal(t, time.Second, time.Since(start))
					require.Contains(t, recorder.Body.String(), `"test_complete"`)
				} else {
					require.ErrorContains(t, err, "finalization timed out")
					require.Equal(t, accountTestClaudeFinalizeTimeout, time.Since(start))
					require.NotContains(t, recorder.Body.String(), `"test_complete"`)
				}
			})
		})
	}
}

type accountTestTrackedBody struct {
	accountTestEOFReader
	closed          bool
	closedBeforeEOF bool
}

func (r *accountTestTrackedBody) Close() error {
	r.closed = true
	r.closedBeforeEOF = !r.eof
	return nil
}

func TestAccountTestClaudeConnectionClosesAfterFinalUsage(t *testing.T) {
	body := &accountTestTrackedBody{accountTestEOFReader: accountTestEOFReader{
		reader: strings.NewReader("data: {\"type\":\"message_stop\"}\n\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":12}}\n\n"),
	}}
	upstream := &queuedHTTPUpstream{responses: []*http.Response{{StatusCode: http.StatusOK, Header: http.Header{}, Body: body}}}
	c, recorder := newTestContext()
	account := &Account{ID: 1, Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Concurrency: 1,
		Credentials: map[string]any{"api_key": "test-only", "base_url": "https://api.anthropic.com"}}
	err := (&AccountTestService{httpUpstream: upstream, cfg: &config.Config{}}).testClaudeAccountConnection(c, account, "claude-sonnet-4-6")
	require.NoError(t, err)
	require.Len(t, upstream.requests, 1, "finalization must not retry a paid request")
	require.True(t, body.eof)
	require.True(t, body.closed)
	require.False(t, body.closedBeforeEOF)
	require.Contains(t, recorder.Body.String(), `"test_complete"`)
}
