package handler

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestConversationGuardPreservesStatusAndRootCause(t *testing.T) {
	h := &OpenAIGatewayHandler{}
	for code, status := range map[string]int{"proxy_unavailable": 503, "conversation_account_paused": 503, "conversation_quota_exhausted": 429, "conversation_account_restricted": 403} {
		for _, path := range []string{"/v1/responses", "/v1/chat/completions", "/v1/messages"} {
			for _, committed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s%s/%t", code, path, committed), func(t *testing.T) {
					c, rec := newGinContextForEndpoint(t, path)
					if committed {
						c.Header("Content-Type", "text/event-stream")
						_, _ = c.Writer.WriteString(": ping\n\n")
						c.Writer.Flush()
					}
					e := &service.UpstreamFailoverError{StatusCode: status, ResponseHeaders: http.Header{"Retry-After": {"60"}}, ResponseBody: []byte(fmt.Sprintf(`{"error":{"type":"server_error","code":%q,"message":"原会话保护生效；本次未切换账号"}}`, code))}
					if path == "/v1/messages" {
						h.handleAnthropicFailoverExhausted(c, e, committed)
					} else {
						h.handleFailoverExhausted(c, e, committed)
					}
					body, prefix := rec.Body.String(), "error."
					if committed {
						require.Equal(t, 200, rec.Code)
						if path == "/v1/responses" {
							require.Equal(t, 1, strings.Count(body, "event: response.created\n"))
							require.Equal(t, 1, strings.Count(body, "event: response.failed\n"))
							body = strings.Split(body, "event: response.failed\ndata: ")[1]
							prefix = "response.error."
						} else {
							body = strings.Split(body, "data: ")[1]
						}
					} else {
						require.Equal(t, status, rec.Code)
					}
					require.Equal(t, code, gjson.Get(body, prefix+"code").String())
					require.Equal(t, int64(status), gjson.Get(body, prefix+"status_code").Int())
					require.Equal(t, "原会话保护生效；本次未切换账号", gjson.Get(body, prefix+"message").String())
					require.NotContains(t, body, "usage")
				})
			}
		}
	}
}
