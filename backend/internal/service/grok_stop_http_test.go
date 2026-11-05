//go:build unit

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGrokStringStopRealHTTP(t *testing.T) {
	runGrokStopHTTP(t, PlatformGrok, false, `"synthetic-stop"`, `["synthetic-stop"]`)
}

func TestGrokStopWirePreservesStreamAndOtherPlatforms(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, tc := range []struct{ platform, stop, want string }{
			{PlatformGrok, `"  \nend"`, `["  \nend"]`},
			{PlatformGrok, `["one","two"]`, `["one","two"]`},
			{PlatformGrok, `null`, `null`},
			{PlatformGrok, `42`, `42`},
			{PlatformOpenAI, `"synthetic-stop"`, `"synthetic-stop"`},
		} {
			t.Run(fmt.Sprintf("%s/%s/stream=%t", tc.platform, tc.stop, stream), func(t *testing.T) { runGrokStopHTTP(t, tc.platform, stream, tc.stop, tc.want) })
		}
	}
}

func runGrokStopHTTP(t *testing.T, platform string, stream bool, stop, want string) {
	t.Helper()
	var calls atomic.Int32
	var actual map[string]json.RawMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&actual))
		var gotStop, wantStop any
		if json.Unmarshal(actual["stop"], &gotStop) != nil || json.Unmarshal([]byte(want), &wantStop) != nil || !assert.ObjectsAreEqual(wantStop, gotStop) {
			w.WriteHeader(422)
			io.WriteString(w, "synthetic stop expects sequence")
			return
		}
		if stream {
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"OK\"},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":7,\"total_tokens\":19}}\n\ndata: [DONE]\n\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"synthetic","choices":[{"message":{"content":"OK"},"finish_reason":"stop"}],"usage":{"prompt_tokens":12,"completion_tokens":7,"total_tokens":19}}`)
	}))
	defer server.Close()
	dest, err := url.Parse(server.URL)
	require.NoError(t, err)
	svc := openAIClientToolsTestService(nil)
	svc.httpUpstream = &grokRefusalHTTPUpstream{client: server.Client(), url: dest}
	account := &Account{ID: 25001, Platform: platform, Type: AccountTypeAPIKey, Credentials: map[string]any{"api_key": "offline-only", "base_url": "https://api.x.ai/v1"}}
	model := "grok-4.7"
	if platform == PlatformOpenAI {
		model = "gpt-4o"
	}
	body := []byte(fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"synthetic"}],"stop":%s,"stream":%t}`, model, stop, stream))
	original := string(body)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(body)))
	result, err := svc.forwardAsRawChatCompletions(context.Background(), c, account, body, "")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 12, result.Usage.InputTokens)
	require.Equal(t, 7, result.Usage.OutputTokens)
	require.Equal(t, 200, rec.Code)
	require.EqualValues(t, 1, calls.Load())
	require.Equal(t, original, string(body))
}
