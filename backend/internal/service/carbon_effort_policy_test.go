//go:build unit

package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestCarbonChatEffortCeilingPreservesRequestAndUsage(t *testing.T) {
	for _, ceiling := range []string{"", "high"} {
		t.Run("ceiling_"+ceiling, func(t *testing.T) {
			body := []byte(`{"model":"gemini-4-carbon","reasoning_effort":"max","messages":[{"role":"user","content":"isolated fixture"}],"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}],"stream":false}`)
			upstream := &httpUpstreamRecorder{resp: &http.Response{
				StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}},
				Body: io.NopCloser(strings.NewReader(`{"choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2}}`)),
			}}
			service := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig(), httpUpstream: upstream}
			account := rawChatCompletionsTestAccount()
			account.Credentials["model_mapping"] = map[string]any{"gemini-4-carbon": "ultra/gemini-4-carbon-low"}
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(body)))
			outbound, changed, err := ApplyOpenAIReasoningEffortPolicy(body, ceiling, nil, ReasoningEffortOverLimitDowngrade)
			require.NoError(t, err)
			require.Equal(t, ceiling != "", changed)
			result, err := service.forwardAsRawChatCompletions(context.Background(), c, account, outbound, "")
			require.NoError(t, err)
			expected := "max"
			if ceiling != "" {
				expected = ceiling
			}
			require.Equal(t, expected, gjson.GetBytes(upstream.lastBody, "reasoning_effort").String())
			require.Equal(t, "ultra/gemini-4-carbon-low", gjson.GetBytes(upstream.lastBody, "model").String())
			require.Equal(t, gjson.GetBytes(body, "messages").Raw, gjson.GetBytes(upstream.lastBody, "messages").Raw)
			require.Equal(t, gjson.GetBytes(body, "tools").Raw, gjson.GetBytes(upstream.lastBody, "tools").Raw)
			if ceiling != "" {
				require.NotNil(t, result.ReasoningEffort)
				require.Equal(t, ceiling, *result.ReasoningEffort)
			}
			require.Equal(t, 3, result.Usage.InputTokens)
			require.Equal(t, 2, result.Usage.OutputTokens)
		})
	}
}
