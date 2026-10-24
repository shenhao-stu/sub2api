package service

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const upstreamUnavailableBody = `{"error":{"type":"upstream_unavailable","message":"No eligible upstream account is available"}}`

func TestOpenAIUpstreamUnavailable_OnlyAcceptsExplicit503(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"client_error", 400, upstreamUnavailableBody},
		{"rate_limit", 429, upstreamUnavailableBody},
		{"bad_gateway", 502, upstreamUnavailableBody},
		{"wrong_type", 503, `{"error":{"type":"server_error"}}`},
		{"uppercase_type_key", 503, `{"error":{"TYPE":"upstream_unavailable"}}`},
		{"uppercase_error_key", 503, `{"ERROR":{"type":"upstream_unavailable"}}`},
		{"paused_guard", 503, `{"error":{"type":"server_error","code":"conversation_account_paused"}}`},
		{"nested_type", 503, `{"response":{"error":{"type":"upstream_unavailable"}}}`},
		{"non_string_type", 503, `{"error":{"type":["upstream_unavailable"]}}`},
		{"null", 503, `null`},
		{"array", 503, `[{"error":{"type":"upstream_unavailable"}}]`},
		{"null_error", 503, `{"error":null}`},
		{"array_error", 503, `{"error":[{"type":"upstream_unavailable"}]}`},
		{"malformed", 503, `{"error":{"type":"upstream_unavailable"}`},
		{"trailing_document", 503, upstreamUnavailableBody + `{}`},
		{"oversize", 503, `{"error":{"type":"upstream_unavailable","message":"` + strings.Repeat("x", 64<<10) + `"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, rec := newOpenAIUpstreamErrorTestContext(t)
			require.False(t, WriteOpenAIUpstreamUnavailableError(c, tc.status, []byte(tc.body), false))
			require.Empty(t, rec.Body.String())
			require.False(t, c.Writer.Written())
		})
	}
	require.False(t, WriteOpenAIUpstreamUnavailableError(nil, 503, []byte(upstreamUnavailableBody), false))
}

func TestOpenAIUpstreamUnavailable_RedactsAndBoundsPublicFields(t *testing.T) {
	c, rec := newOpenAIUpstreamErrorTestContext(t)
	body := []byte(`{"error":{"type":"upstream_unavailable","code":"sk-PRIVATE","param":"input[0]['PRIVATE']","status_code":200,"message":"No eligible account; https://origin.example.invalid/private?api_key=PRIVATE Bearer SECRET sk-private-value api_key=PRIVATE cookie=PRIVATE"},"request":"PRIVATE","usage":{"input_tokens":999},"headers":{"authorization":"PRIVATE"}}`)
	require.True(t, WriteOpenAIUpstreamUnavailableError(c, 503, body, false))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	response := rec.Body.String()
	require.Equal(t, "upstream_unavailable", gjson.Get(response, "error.type").String())
	require.Equal(t, int64(503), gjson.Get(response, "error.status_code").Int())
	require.Contains(t, gjson.Get(response, "error.message").String(), "No eligible account")
	for _, private := range []string{"origin.example.invalid", "https://", "PRIVATE", "SECRET", "sk-private-value", "usage", "headers", "request"} {
		require.NotContains(t, response, private)
	}
	require.False(t, gjson.Get(response, "error.code").Exists())
	require.False(t, gjson.Get(response, "error.param").Exists())
}

func TestOpenAIUpstreamUnavailable_UsesExactErrorEnvelope(t *testing.T) {
	for _, body := range []string{
		`{"error":{"type":"upstream_unavailable","message":"expected reason"},"ERROR":{"type":"server_error","message":"unrelated sibling"}}`,
		`{"ERROR":{"type":"server_error","message":"unrelated sibling"},"error":{"type":"upstream_unavailable","message":"expected reason"}}`,
	} {
		c, rec := newOpenAIUpstreamErrorTestContext(t)
		require.True(t, WriteOpenAIUpstreamUnavailableError(c, 503, []byte(body), false))
		require.Equal(t, "upstream_unavailable", gjson.Get(rec.Body.String(), "error.type").String())
		require.Equal(t, "expected reason", gjson.Get(rec.Body.String(), "error.message").String())
		require.NotContains(t, rec.Body.String(), "unrelated sibling")
	}
}

func TestOpenAIUpstreamUnavailable_StopsHeartbeatAndKeepsLogical503(t *testing.T) {
	c, rec := newCompactBridgeTestContext(t, true)
	stop := StartOpenAICompactSSEKeepalive(c, keepaliveTestInterval)
	defer stop()
	waitForKeepaliveBeats()
	require.True(t, WriteOpenAIUpstreamUnavailableError(c, 503, []byte(upstreamUnavailableBody), false))
	length := rec.Body.Len()
	waitForKeepaliveBeats()
	require.Equal(t, length, rec.Body.Len(), "no heartbeat may follow the terminal")
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	require.Equal(t, 1, strings.Count(body, "event: response.created\n"))
	require.Equal(t, 1, strings.Count(body, "event: response.failed\n"))
	created := strings.Split(strings.Split(body, "event: response.created\ndata: ")[1], "\n")[0]
	failed := strings.Split(strings.Split(body, "event: response.failed\ndata: ")[1], "\n")[0]
	require.Equal(t, gjson.Get(created, "response.id").String(), gjson.Get(failed, "response.id").String())
	require.Equal(t, int64(503), gjson.Get(failed, "response.error.status_code").Int())
	require.False(t, gjson.Get(failed, "response.usage").Exists())
	streamErr, ok := GetOpsStreamError(c)
	require.True(t, ok)
	require.Equal(t, http.StatusServiceUnavailable, streamErr.IntendedStatus)
	require.Equal(t, "upstream_unavailable", streamErr.ErrType)
}

func TestOpenAIUpstreamUnavailable_DoesNotRestartExistingResponse(t *testing.T) {
	c, rec := newOpenAIUpstreamErrorTestContext(t)
	c.Header("Content-Type", "text/event-stream")
	_, _ = c.Writer.WriteString("event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_synthetic\"}}\n\n")
	c.Writer.Flush()
	MarkOpenAIClientErrorSemanticOutput(c)
	before := rec.Body.String()
	require.False(t, WriteOpenAIUpstreamUnavailableError(c, 503, []byte(upstreamUnavailableBody), true))
	require.Equal(t, before, rec.Body.String(), "the existing stream handler owns its lifecycle")
}

func TestOpenAIUpstreamUnavailable_CommittedJSONRetainsLogical503(t *testing.T) {
	c, rec := newOpenAIUpstreamErrorTestContext(t)
	c.Header("Content-Type", "application/json")
	_, _ = c.Writer.WriteString(" \n")
	c.Writer.Flush()
	require.True(t, WriteOpenAIUpstreamUnavailableError(c, 503, []byte(upstreamUnavailableBody), false))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, int64(503), gjson.Get(rec.Body.String(), "error.status_code").Int())
	streamErr, ok := GetOpsStreamError(c)
	require.True(t, ok)
	require.True(t, streamErr.NonStream)
	require.Equal(t, http.StatusServiceUnavailable, streamErr.IntendedStatus)
}
