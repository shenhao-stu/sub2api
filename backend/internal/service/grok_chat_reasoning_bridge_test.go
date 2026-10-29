//go:build unit

package service

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestGrokChatReasoningUsesSSEUpstreamAndPreservesClientContract(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, stream := range []bool{false, true} {
		for _, effort := range []string{"none", "low", "medium", "high", "xhigh"} {
			t.Run(effort+map[bool]string{false: "/json", true: "/stream"}[stream], func(t *testing.T) {
				body, err := json.Marshal(map[string]any{
					"model": "grok-4.6", "stream": stream, "reasoning_effort": effort,
					"messages": []map[string]string{{"role": "user", "content": "Reply OK"}},
				})
				require.NoError(t, err)
				original := bytes.Clone(body)
				recorder := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(recorder)
				c.Request = httptest.NewRequest(http.MethodPost, grokChatRawEndpoint, bytes.NewReader(body))
				c.Set("api_key", &APIKey{ID: 7901})
				account := grokChatBridgeTestAccount(79)
				repo := &grokQuotaAccountRepo{mockAccountRepoForPlatform: &mockAccountRepoForPlatform{
					accountsByID: map[int64]*Account{account.ID: account},
				}}
				upstream := &httpUpstreamRecorder{resp: grokChatBridgeCompletedResponse("reasoning-bridge", 4096)}
				svc := &OpenAIGatewayService{
					billingService: newLocalFixtureBilling(), httpUpstream: upstream,
					grokTokenProvider: NewGrokTokenProvider(repo, nil), accountRepo: repo,
				}

				result, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body, "", "")

				require.Len(t, upstream.requests, 1, "conversion must not replay a dispatched request")
				require.Equal(t, grokChatResponsesEndpoint, upstream.lastReq.URL.Path)
				require.True(t, gjson.GetBytes(upstream.lastBody, "stream").Bool())
				require.Equal(t, HTTPUpstreamProfileGrok, HTTPUpstreamProfileFromContext(upstream.lastReq.Context()))
				require.Equal(t, effort, gjson.GetBytes(upstream.lastBody, "reasoning.effort").String())
				require.False(t, gjson.GetBytes(upstream.lastBody, "reasoning.summary").Exists(), "Chat did not request a reasoning summary")
				require.NotEmpty(t, gjson.GetBytes(upstream.lastBody, "prompt_cache_key").String())
				require.Equal(t, original, body)
				require.NoError(t, err)
				require.NotNil(t, result)
				require.Equal(t, stream, result.Stream)
				require.Equal(t, effort, *result.ReasoningEffort)
				require.Equal(t, 9908, result.Usage.InputTokens)
				require.Equal(t, 12, result.Usage.OutputTokens)
				require.Equal(t, 4096, result.Usage.CacheReadInputTokens)
				if stream {
					require.Contains(t, recorder.Body.String(), "data: [DONE]")
					require.Contains(t, recorder.Body.String(), `"cached_tokens":4096`)
				} else {
					require.True(t, gjson.ValidBytes(recorder.Body.Bytes()))
					require.Equal(t, "cached ok", gjson.GetBytes(recorder.Body.Bytes(), "choices.0.message.content").String())
					require.Equal(t, int64(4096), gjson.GetBytes(recorder.Body.Bytes(), "usage.prompt_tokens_details.cached_tokens").Int())
				}
			})
		}
	}
}
