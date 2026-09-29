package service

import (
	"context"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// Normalize the Responses envelope at the provider boundary so HTTP and SSE
// failures use the same Grok quota, capacity and retry policies.
func grokStreamErrorBody(payload []byte) []byte {
	for _, path := range []string{"response.error", "error"} {
		if failure := gjson.GetBytes(payload, path); failure.Exists() {
			return []byte(`{"error":` + failure.Raw + `}`)
		}
	}
	return payload
}

func (s *OpenAIGatewayService) handleGrokStreamTerminalError(c *gin.Context, account *Account, status int, payload []byte, model string) {
	ctx := context.Background()
	if c != nil && c.Request != nil {
		ctx = c.Request.Context()
	}
	body := grokStreamErrorBody(payload)
	// HTTP 200 headers describe the stream opening, not its later failure.
	s.handleGrokAccountUpstreamError(withGrokTeamRateLimitModel(ctx, model), account, status, nil, body)
}
