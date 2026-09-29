//go:build unit

package service

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestAntigravityPreContentFailuresPreserveObservedUsage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	usageLine := `data: {"response":{"usageMetadata":{"promptTokenCount":8,"candidatesTokenCount":3}}}`
	for _, responses := range []bool{false, true} {
		for _, keepalive := range []bool{false, true} {
			for _, terminal := range []string{"eof", "read_error", "deadline"} {
				t.Run(fmt.Sprintf("responses=%v/keepalive=%v/%s", responses, keepalive, terminal), func(t *testing.T) {
					c, recorder := newAntigravityCompatContext(http.MethodPost, "/", nil)
					var adapter antigravityCompatStreamAdapter = newAntigravityChatStreamAdapter("gemini-3.1-pro", true)
					if responses {
						adapter = newAntigravityResponsesStreamAdapter("gemini-3.1-pro")
					}
					writer := newAntigravityClientWriter(c.Writer, c.Writer, "fixture")
					start := time.Now()
					session := newAntigravityCompatStreamSession("gemini-3.1-pro", start, adapter, writer)
					if keepalive {
						session.writePreContentKeepalive(start.Add(15 * time.Second))
					}
					var result *antigravityStreamResult
					var err error
					if terminal == "deadline" {
						// The scanner has delivered the complete line, but the session has not parsed it yet.
						result, err = session.preContentTimeout(c, usageLine)
					} else {
						session.consume(usageLine)
						if terminal == "eof" {
							result, err = handleAntigravityCompatEmptyStream(c, session)
						} else {
							result, err = (&AntigravityGatewayService{}).handleAntigravityCompatReadError(
								c, session, io.ErrUnexpectedEOF, defaultMaxLineSize, "fixture")
						}
					}
					require.Error(t, err)
					var failover *UpstreamFailoverError
					require.NotErrorAs(t, err, &failover, "observed usage must never cause regeneration")
					require.NotNil(t, result)
					require.Equal(t, 8, result.usage.InputTokens)
					require.Equal(t, 3, result.usage.OutputTokens)
					require.Equal(t, keepalive, IsResponseCommitted(c))
					if keepalive {
						require.Contains(t, recorder.Body.String(), ": ping\n\n")
						require.Equal(t, 1, strings.Count(recorder.Body.String(), `"upstream_error"`))
					} else {
						require.Empty(t, recorder.Body.String())
					}
					require.NotContains(t, recorder.Body.String(), "[DONE]")
				})
			}
		}
	}
}

func TestAntigravityPreContentDeadlineDoesNotEmitLateContent(t *testing.T) {
	c, recorder := newAntigravityCompatContext(http.MethodPost, "/", nil)
	writer := newAntigravityClientWriter(c.Writer, c.Writer, "fixture")
	session := newAntigravityCompatStreamSession("gemini-3.1-pro", time.Now(),
		newAntigravityResponsesStreamAdapter("gemini-3.1-pro"), writer)
	line := `data: {"response":{"usageMetadata":{"promptTokenCount":8,"candidatesTokenCount":3},"candidates":[{"content":{"parts":[{"text":"late answer"}]}}]}}`
	result, err := session.preContentTimeout(c, line)
	require.Error(t, err)
	var failover *UpstreamFailoverError
	require.NotErrorAs(t, err, &failover)
	require.NotNil(t, result)
	require.Equal(t, 8, result.usage.InputTokens)
	require.Equal(t, 3, result.usage.OutputTokens)
	require.Empty(t, recorder.Body.String())
	require.Nil(t, result.firstTokenMs)
}
