//go:build unit

package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestGrokReceiptUsesExplicitExclusiveReasoningTotals(t *testing.T) {
	for _, tc := range []struct {
		name, platform, meter string
		stream, valid         bool
	}{
		{"exclusive_stream", PlatformGrok, `"prompt_tokens":211,"completion_tokens":1,"total_tokens":267,"prompt_tokens_details":{"cached_tokens":128},"completion_tokens_details":{"reasoning_tokens":55}`, true, true},
		{"exclusive_buffer", PlatformGrok, `"prompt_tokens":211,"completion_tokens":1,"total_tokens":267,"prompt_tokens_details":{"cached_tokens":128},"completion_tokens_details":{"reasoning_tokens":55}`, false, true},
		{"inclusive_unchanged", PlatformGrok, `"prompt_tokens":211,"completion_tokens":56,"total_tokens":267,"prompt_tokens_details":{"cached_tokens":128},"completion_tokens_details":{"reasoning_tokens":55}`, true, true},
		{"missing_total", PlatformGrok, `"prompt_tokens":211,"completion_tokens":1,"completion_tokens_details":{"reasoning_tokens":55}`, true, false},
		{"contradictory_total", PlatformGrok, `"prompt_tokens":211,"completion_tokens":1,"total_tokens":268,"completion_tokens_details":{"reasoning_tokens":55}`, true, false},
		{"duplicate_reasoning", PlatformGrok, `"prompt_tokens":211,"completion_tokens":1,"total_tokens":267,"completion_tokens_details":{"reasoning_tokens":55,"reasoning_tokens":54}`, true, false},
		{"other_provider_unchanged", PlatformOpenAI, `"prompt_tokens":211,"completion_tokens":1,"total_tokens":267,"completion_tokens_details":{"reasoning_tokens":55}`, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				usage := `"usage":{` + tc.meter + `}`
				if !tc.stream {
					w.Header().Set("Content-Type", "application/json")
					fmt.Fprint(w, `{"choices":[{"message":{"content":"OK"},"finish_reason":"stop"}],`+usage+`}`)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprint(w, `data: {"choices":[{"delta":{"content":"OK"},"finish_reason":"stop"}]}`+"\n\ndata: {\"choices\":[],"+usage+"}\n\ndata: [DONE]\n\n")
			}))
			defer upstream.Close()
			repo := &chatReceiptCaptureRepo{}
			transport := &independentHeaderHTTP{client: upstream.Client()}
			svc := &OpenAIGatewayService{cfg: structuredInputRetryTestConfig(), httpUpstream: transport, usageLogRepo: repo}
			account := structuredInputRetryTestAccount()
			account.Platform = tc.platform
			account.Credentials["base_url"] = upstream.URL
			account.Extra["openai_responses_supported"] = false
			body := []byte(fmt.Sprintf(`{"model":"grok-4.6","messages":[{"role":"user","content":"synthetic"}],"stream":%t}`, tc.stream))
			ctx := context.WithValue(context.Background(), ctxkey.ClientRequestID, uuid.NewString())
			ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			ctx, err := svc.BeginChatUsageReceipt(ctx, 11, uuid.NewString(), "grok-4.6", body)
			require.NoError(t, err)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("POST", ChatReceiptEndpoint, strings.NewReader(string(body))).WithContext(ctx)
			_, err = svc.ForwardAsChatCompletions(ctx, c, account, body, "", "")
			require.NoError(t, err)
			require.Equal(t, int32(1), transport.calls.Load(), "no model replay")
			if !tc.valid {
				require.Nil(t, repo.receipt)
				return
			}
			require.NotNil(t, repo.receipt)
			require.Equal(t, 83, repo.receipt.InputTokens)
			require.Equal(t, 128, repo.receipt.CacheReadTokens)
			require.Equal(t, 56, repo.receipt.OutputTokens)
			require.Equal(t, 55, repo.receipt.ReasoningTokens)
			require.Equal(t, "completed", repo.receipt.TerminalState)
			require.True(t, ValidateChatUsageReceipt(repo.receipt))
		})
	}
}

func TestGrokExclusiveReceiptRejectsAmbiguousCounters(t *testing.T) {
	for _, raw := range []string{
		`{"prompt_tokens":211,"completion_tokens":1,"total_tokens":267,"completion_tokens_details":{"reasoning_tokens":55},"prompt_tokens_details":{"cached_tokens":212}}`,
		`{"prompt_tokens":211,"completion_tokens":1,"total_tokens":267,"completion_tokens_details":{"reasoning_tokens":55,"audio_tokens":1}}`,
		`{"prompt_tokens":211,"completion_tokens":1,"total_tokens":267,"completion_tokens_details":{"reasoning_tokens":55},"output_tokens_details":{"reasoning_tokens":54}}`,
		`{"prompt_tokens":211,"completion_tokens":1,"total_tokens":267,"TOTAL_TOKENS":0,"completion_tokens_details":{"reasoning_tokens":55}}`,
		`{"prompt_tokens":9223372036854775807,"completion_tokens":1,"total_tokens":0,"completion_tokens_details":{"reasoning_tokens":1}}`,
		`{"prompt_tokens":211,"completion_tokens":1.1,"total_tokens":267,"completion_tokens_details":{"reasoning_tokens":55}}`,
		`{"prompt_tokens":211,"completion_tokens":1,"total_tokens":212,"completion_tokens_details":{"reasoning_tokens":0}}`,
	} {
		require.Nil(t, parseGrokExclusiveChatReceiptMeter([]byte(raw)), raw)
	}
	meter := parseGrokExclusiveChatReceiptMeter([]byte(`{"prompt_tokens":211,"completion_tokens":6,"total_tokens":219,"completion_tokens_details":{"reasoning_tokens":2}}`))
	require.NotNil(t, meter)
	require.Equal(t, 8, meter.OutputTokens)
	require.Equal(t, 2, meter.ReasoningTokens)
}
