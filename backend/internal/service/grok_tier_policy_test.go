//go:build unit

package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestGrokResponsesAppliesTierPolicyAndRecordsBilling(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, tc := range []struct{ name, action, want string }{
			{"pass", BetaPolicyActionPass, "priority"},
			{"filter", BetaPolicyActionFilter, ""},
			{"block", BetaPolicyActionBlock, ""},
		} {
			t.Run(fmt.Sprintf("%s/stream=%t", tc.name, stream), func(t *testing.T) {
				svc := newOpenAIGatewayServiceWithSettings(t, &OpenAIFastPolicySettings{Rules: []OpenAIFastPolicyRule{{ServiceTier: OpenAIFastTierPriority, Action: tc.action, Scope: BetaPolicyScopeAll}}})
				body := []byte(fmt.Sprintf(`{"model":"grok-4.6","input":"hi","service_tier":"fast","stream":%t}`, stream))
				w := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(w)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
				account := &Account{ID: 242, Platform: PlatformGrok, Type: AccountTypeAPIKey, Credentials: map[string]any{"api_key": "test-only", "base_url": "https://xai.test/v1"}}
				response := `{"id":"resp_tier","object":"response","status":"completed","model":"grok-4.6","service_tier":"default","output":[],"usage":{"input_tokens":8,"output_tokens":3}}`
				contentType := "application/json"
				if stream {
					contentType = "text/event-stream"
					response = "data: {\"type\":\"response.completed\",\"response\":" + response + "}\n\n"
				}
				upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(response))}}
				svc.httpUpstream = upstream
				result, err := svc.forwardGrokResponses(context.Background(), c, account, body, "grok-4.6", stream, time.Now())
				if tc.action == BetaPolicyActionBlock {
					var blocked *OpenAIFastBlockedError
					require.ErrorAs(t, err, &blocked)
					require.Nil(t, result)
					require.Nil(t, upstream.lastReq)
					require.Equal(t, 403, w.Code)
					return
				}
				require.NoError(t, err)
				require.Equal(t, tc.want, gjson.GetBytes(upstream.lastBody, "service_tier").String())
				if tc.want == "" {
					require.Nil(t, result.ServiceTier)
				} else {
					require.NotNil(t, result.ServiceTier)
					require.Equal(t, tc.want, *result.ServiceTier)
				}
				require.Equal(t, "default", result.UpstreamResponseServiceTier)
				require.Equal(t, 8, result.Usage.InputTokens)
			})
		}
	}
}
