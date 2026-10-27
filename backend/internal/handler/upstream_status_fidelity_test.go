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

func TestUpstreamStatusMappingKeeps503AndExistingBoundaries(t *testing.T) {
	for _, tc := range []struct{ upstream, want int }{{401, 502}, {403, 502}, {429, 429}, {500, 502}, {502, 502}, {503, 503}, {504, 502}, {529, 503}, {418, 502}} {
		t.Run(fmt.Sprint(tc.upstream), func(t *testing.T) {
			openAI, _, _ := (&OpenAIGatewayHandler{}).mapUpstreamError(tc.upstream)
			gateway, _, _ := (&GatewayHandler{}).mapUpstreamError(tc.upstream)
			gemini, _ := mapGeminiUpstreamError(tc.upstream)
			require.Equal(t, tc.want, openAI)
			require.Equal(t, tc.want, gateway)
			require.Equal(t, tc.want, gemini)
		})
	}
}

func TestUnknown503FailoverPreservesHTTPAndLogicalStreamStatus(t *testing.T) {
	for _, upstream := range []string{"openai", "anthropic", "gemini"} {
		paths := []string{"/v1/responses", "/v1/chat/completions", "/v1/messages"}
		if upstream == "gemini" {
			paths = []string{"/v1beta/models/gemini-test:generateContent"}
		}
		for _, path := range paths {
			for _, committed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s%s/committed=%t", upstream, path, committed), func(t *testing.T) {
					c, rec := newGinContextForEndpoint(t, path)
					if committed {
						c.Header("Content-Type", "text/event-stream")
						_, _ = c.Writer.WriteString(": ping\n\n")
						c.Writer.Flush()
					}
					e := &service.UpstreamFailoverError{StatusCode: 503, ResponseHeaders: http.Header{"Retry-After": {"60"}}, ResponseBody: []byte(`{"error":{"type":"server_error","message":"private account name@example.invalid https://private.fixture.invalid sk-privatefixture","status_code":418}}`)}
					switch upstream {
					case "openai":
						h := &OpenAIGatewayHandler{}
						if path == "/v1/messages" {
							h.handleAnthropicFailoverExhausted(c, e, committed)
						} else {
							h.handleFailoverExhausted(c, e, committed)
						}
					case "anthropic":
						(&GatewayHandler{}).handleFailoverExhausted(c, e, service.PlatformAnthropic, committed)
					default:
						(&GatewayHandler{}).handleGeminiFailoverExhausted(c, e)
					}
					body, prefix := rec.Body.String(), "error."
					if committed {
						require.Equal(t, 200, rec.Code)
						if upstream != "gemini" {
							streamErr, ok := service.GetOpsStreamError(c)
							require.True(t, ok)
							require.Equal(t, 503, streamErr.IntendedStatus)
							if strings.Contains(path, "/responses") {
								require.Equal(t, 1, strings.Count(body, "event: response.failed\n"))
								body = strings.Split(body, "event: response.failed\ndata: ")[1]
								prefix = "response.error."
							} else {
								body = strings.Split(body, "data: ")[1]
							}
							require.Equal(t, int64(503), gjson.Get(body, prefix+"status_code").Int())
						} else {
							body = strings.TrimPrefix(body, ": ping\n\n")
						}
					} else {
						require.Equal(t, 503, rec.Code)
					}
					require.Equal(t, "Upstream service temporarily unavailable", gjson.Get(body, prefix+"message").String())
					if upstream == "gemini" {
						require.Equal(t, int64(503), gjson.Get(body, "error.code").Int())
						require.Equal(t, "UNAVAILABLE", gjson.Get(body, "error.status").String())
					}
					require.NotContains(t, body, "private")
					require.NotContains(t, body, "name@example")
					require.NotEqual(t, int64(418), gjson.Get(body, prefix+"status_code").Int(), "check status, not digits in a random response ID")
					require.NotContains(t, body, `"usage"`)
				})
			}
		}
	}
}

func TestResponsesFailedLogicalStatusUsesValidatedHandlerMetadata(t *testing.T) {
	for _, status := range []int{0, 200, 429, 503, 600} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			c, rec := newGinContextForEndpoint(t, "/v1/responses")
			service.SetOpsUpstreamError(c, 418, "untrusted status hint", "")
			if status != 0 {
				service.MarkOpsStreamError(c, "upstream_error", "safe", status)
			}
			require.True(t, writeResponsesFailedSSE(c, "upstream_error", "", "safe"))
			body := strings.Split(rec.Body.String(), "event: response.failed\ndata: ")[1]
			if status >= 400 && status <= 599 {
				require.Equal(t, int64(status), gjson.Get(body, "response.error.status_code").Int())
			} else {
				require.False(t, gjson.Get(body, "response.error.status_code").Exists())
			}
			require.NotEqual(t, int64(418), gjson.Get(body, "response.error.status_code").Int())
			require.NotContains(t, body, `"usage"`)
		})
	}
}
