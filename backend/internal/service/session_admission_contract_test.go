package service

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func sessionAdmissionBody(metered bool) string {
	if !metered {
		return sessionAdmissionTestBody
	}
	return strings.TrimSuffix(sessionAdmissionTestBody, "}") + `,"usage":{"input_tokens":12,"output_tokens":7,"cache_read_input_tokens":5,"cache_creation":{"ephemeral_5m_input_tokens":2,"ephemeral_1h_input_tokens":3}}}`
}

func TestSessionAdmissionRequiresExact429Contract(t *testing.T) {
	body := sessionAdmissionBody(false)
	require.NotEmpty(t, sessionAdmissionRejectionMessage(429, []byte(body)))
	for _, status := range []int{200, 400, 401, 402, 403, 500, 503} {
		require.Empty(t, sessionAdmissionRejectionMessage(status, []byte(body)))
	}
	for _, mutate := range []func(map[string]any){
		func(v map[string]any) { v["status"] = 503 },
		func(v map[string]any) { v["status"] = nil },
		func(v map[string]any) { v["error"].(map[string]any)["status"] = 400 },
		func(v map[string]any) { v["status_code"] = 503 },
		func(v map[string]any) { v["type"] = "success" },
		func(v map[string]any) { v["type"] = nil },
		func(v map[string]any) { v["status_code"] = nil },
		func(v map[string]any) { v["status_code"] = "429" },
		func(v map[string]any) { v["error"].(map[string]any)["status_code"] = 500 },
		func(v map[string]any) { v["error"].(map[string]any)["code"] = "insufficient_quota" },
		func(v map[string]any) { v["error"].(map[string]any)["code"] = nil },
		func(v map[string]any) { v["error"].(map[string]any)["message"] = sessionAdmissionRejection + " " },
		func(v map[string]any) { v["error"].(map[string]any)["type"] = "rate_limit" },
		func(v map[string]any) { v["error"].(map[string]any)["message"] = "insufficient budget" },
		func(v map[string]any) {
			e := v["error"].(map[string]any)
			e["message"] = e["message"].(string) + " https://private.invalid"
		},
	} {
		var v map[string]any
		require.NoError(t, json.Unmarshal([]byte(body), &v))
		mutate(v)
		raw, err := json.Marshal(v)
		require.NoError(t, err)
		require.Empty(t, sessionAdmissionRejectionMessage(429, raw), string(raw))
	}
	for _, body := range []string{
		strings.Replace(body, `"error":`, `"status":429,"status_code":429,"error":`, 1),
		strings.Replace(body, `"help":`, `"status":429,"status_code":429,"code":"","help":`, 1),
	} {
		require.NotEmpty(t, sessionAdmissionRejectionMessage(429, []byte(body)))
	}
	for _, bad := range []string{
		body + " trailing", body[:len(body)-1], "[" + body + "]",
		strings.Replace(body, `"type":"upstream_error"`, `"type":"upstream_error","type":"upstream_error"`, 1),
		strings.Replace(body, `"type":"upstream_error"`, `"type":"upstream_error","TYPE":"upstream_error"`, 1),
		strings.Replace(body, `"help":`, `"message":"different","help":`, 1),
		strings.Replace(body, `"help":`, `"status_code":429,"status_code":429,"help":`, 1),
		strings.Replace(body, `"help":`, `"padding":"`+strings.Repeat("x", sessionAdmissionBodyLimit)+`","help":`, 1),
	} {
		require.Empty(t, sessionAdmissionRejectionMessage(429, []byte(bad)))
	}
}

func TestSessionAdmissionUnmatchedBodyKeepsReadErrorAndClose(t *testing.T) {
	for _, readErr := range []error{nil, io.ErrUnexpectedEOF} {
		for _, body := range []string{sessionAdmissionBody(false), `{"error":{"message":"wallet empty"}}`, strings.Repeat("x", sessionAdmissionBodyLimit+33)} {
			if readErr == nil && body == sessionAdmissionBody(false) {
				continue
			}
			reader := io.Reader(strings.NewReader(body))
			if readErr != nil {
				reader = io.MultiReader(reader, &streamReadCloser{sent: true, err: readErr})
			}
			source := &modelPermissionReadCloser{Reader: reader}
			resp := &http.Response{StatusCode: 429, Body: source}
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			s := &GatewayService{}
			result, err, handled := s.handleSessionAdmissionResponse(c, newAnthropicAPIKeyAccountForTest(), resp, "claude-opus-5-5", "claude-opus-5-5", false, time.Now())
			require.False(t, handled)
			require.Nil(t, result)
			require.NoError(t, err)
			require.False(t, source.closed)
			actual, err := io.ReadAll(resp.Body)
			require.ErrorIs(t, err, readErr)
			require.Equal(t, body, string(actual))
			require.NoError(t, resp.Body.Close())
			require.True(t, source.closed)
		}
	}
}

func TestSessionAdmissionCommittedStreamPreservesStatusAndObservedBuckets(t *testing.T) {
	for _, path := range []string{"/v1/messages", "/v1/chat/completions", "/v1/responses"} {
		t.Run(path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest("POST", path, nil)
			c.Header("Content-Type", "text/event-stream")
			_, _ = c.Writer.WriteString(": ping\n\n")
			c.Writer.Flush()
			s := newForwardPartialUsageServiceForTest(nil)
			body := sessionAdmissionBody(true)
			resp := &http.Response{StatusCode: 429, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}
			result, err, handled := s.handleSessionAdmissionResponse(c, newAnthropicAPIKeyAccountForTest(), resp, "claude-opus-5-5", "claude-opus-5-5", true, time.Now())
			require.True(t, handled)
			require.Error(t, err)
			require.NotNil(t, result)
			require.Equal(t, 12, result.Usage.InputTokens)
			require.Equal(t, 7, result.Usage.OutputTokens)
			require.Equal(t, 5, result.Usage.CacheReadInputTokens)
			require.Equal(t, 2, result.Usage.CacheCreation5mTokens)
			require.Equal(t, 3, result.Usage.CacheCreation1hTokens)
			require.Equal(t, 200, rec.Code)
			require.Contains(t, rec.Body.String(), `"status_code":400`)
			require.Equal(t, 1, strings.Count(rec.Body.String(), `"code":"upstream_session_not_accepted"`))
			require.NotContains(t, rec.Body.String(), "private.invalid")
			require.NotContains(t, rec.Body.String(), "secret")
		})
	}
}
