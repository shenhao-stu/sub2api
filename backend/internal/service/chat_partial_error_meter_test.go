//go:build unit

package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChatV2PartialGenerationMeterRemainsPending(t *testing.T) {
	for _, reader := range []string{"chat_stream", "chat_buffered"} {
		for _, code := range []string{"upstream_empty_completion", "upstream_malformed_function_call"} {
			t.Run(reader+"/"+code, func(t *testing.T) {
				body := `data: {"type":"response.failed","response":{"status":"failed","error":{"type":"server_error","code":"` + code + `","status_code":502},"usage":{"input_tokens":100}}}` + "\n\n"
				c, rec, resp, svc, account := meteredOpenAITestContext(PlatformOpenAI, body)
				capture := &chatReceiptCapture{}
				c.Request.URL.Path = ChatReceiptEndpoint
				c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), chatReceiptContextKey{}, capture))
				usage, err := runMeteredOpenAIReader(svc, c, resp, account, reader)
				require.Error(t, err)
				var retry *UpstreamFailoverError
				require.NotErrorAs(t, err, &retry)
				require.NotNil(t, usage)
				require.Equal(t, 100, usage.InputTokens, "partial consumption must stay in internal accounting")
				require.Nil(t, capture.meter)
				require.NotContains(t, rec.Body.String(), `"usage":`, "partial counters cannot become a final downstream receipt")
				require.Contains(t, rec.Body.String(), `"code":"`+code+`"`)
			})
		}
	}
}
