package service

import (
	"context"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPassthroughErrorCommitsOneFailureAndPreservesObservedUsage(t *testing.T) {
	for _, end := range []string{"\n\n", ""} {
		t.Run("ending_"+end, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			svc := &GatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}}
			account := &Account{ID: 41, Platform: PlatformAnthropic, Type: AccountTypeAPIKey}
			body := "event: message_start\ndata: " + `{"type":"message_start","message":{"usage":{"input_tokens":7,"cache_creation_input_tokens":99}}}` + "\n\nevent: error\ndata: " + syntheticPolicyRefusal + end
			resp := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(body))}
			result, err := svc.handleStreamingResponseAnthropicAPIKeyPassthrough(context.Background(), resp, c, account, time.Now(), "claude-opus")
			require.Error(t, err)
			require.NotErrorAs(t, err, new(*UpstreamFailoverError))
			require.NotNil(t, result)
			require.Equal(t, 7, result.usage.InputTokens)
			require.Equal(t, 99, result.usage.CacheCreationInputTokens)
			require.False(t, result.usage.receiptFinal, "a refusal must not invent a complete usage receipt")
			require.Equal(t, http.StatusBadRequest, anthropicStreamErrorStatus([]byte(syntheticPolicyRefusal)))
			require.True(t, IsResponseCommitted(c), "outer handler must not append a generic 502")
			require.Equal(t, 1, strings.Count(rec.Body.String(), "event: error"))
			require.Contains(t, rec.Body.String(), `"code":"refusal_guard"`)
			require.True(t, strings.HasSuffix(rec.Body.String(), "\n\n"))
			require.NotContains(t, rec.Body.String(), "message_stop")
		})
	}
}

func TestPassthroughTruncatedErrorDoesNotCommit(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	svc := &GatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}}
	account := &Account{ID: 41, Platform: PlatformAnthropic, Type: AccountTypeAPIKey}
	resp := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("event: error\ndata: {\"type\":\"error\",\"error\":{\n\n"))}
	_, err := svc.handleStreamingResponseAnthropicAPIKeyPassthrough(context.Background(), resp, c, account, time.Now(), "claude-opus")
	require.Error(t, err)
	require.False(t, IsResponseCommitted(c))
}
