package handler

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestFailoverExhausted_UpstreamQuotaClassification(t *testing.T) {
	for _, path := range []string{"/v1/messages", "/v1/responses", "/v1/chat/completions"} {
		for _, stream := range []bool{false, true} {
			for _, quota := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/stream=%t/quota=%t", path, stream, quota), func(t *testing.T) {
					setupOpsErrorLogTestQueue(t, 2)
					gin.SetMode(gin.TestMode)
					ops := service.NewOpsService(&ingressRejectOpsRepo{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
					router := gin.New()
					router.Use(OpsErrorLoggerMiddleware(ops))
					router.POST(path, func(c *gin.Context) {
						if stream {
							c.Header("Content-Type", "text/event-stream")
							_, _ = c.Writer.WriteString(": ping\n\n")
							c.Writer.Flush()
						}
						body := []byte(`{"error":{"code":"rate_limit_exceeded","message":"Rate limit exceeded"}}`)
						if quota {
							body = []byte(`{"error":{"code":"insufficient_quota","message":"Insufficient account balance"}}`)
						}
						failure := &service.UpstreamFailoverError{StatusCode: http.StatusTooManyRequests, ResponseBody: body}
						if path == "/v1/messages" {
							(&GatewayHandler{}).handleFailoverExhausted(c, failure, service.PlatformAnthropic, stream)
						} else {
							(&OpenAIGatewayHandler{}).handleFailoverExhausted(c, failure, stream)
						}
					})
					rec := httptest.NewRecorder()
					router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
					if stream {
						require.Equal(t, http.StatusOK, rec.Code)
					} else {
						require.Equal(t, http.StatusTooManyRequests, rec.Code)
					}
					if quota {
						require.Contains(t, rec.Body.String(), `"code":"insufficient_quota"`)
						require.Contains(t, rec.Body.String(), "Upstream account quota is exhausted, please contact administrator")
						require.NotContains(t, rec.Body.String(), "retry later")
					} else {
						require.Contains(t, rec.Body.String(), "Upstream rate limit exceeded, please retry later")
						require.NotContains(t, rec.Body.String(), "insufficient_quota")
					}
					require.Equal(t, int64(1), OpsErrorLogQueueLength())
					entry := (<-opsErrorLogQueue).entry
					require.Equal(t, http.StatusTooManyRequests, entry.StatusCode)
					require.Equal(t, "provider", entry.ErrorOwner)
					require.False(t, entry.IsBusinessLimited)
					require.NotNil(t, entry.UpstreamStatusCode)
					require.Equal(t, http.StatusTooManyRequests, *entry.UpstreamStatusCode)
				})
			}
		}
	}
}

func TestFailoverExhausted_WalletClassificationAcrossProtocols(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		wallet bool
	}{
		{"wallet", 403, `{"error":{"type":"billing_error","message":"insufficient balance"}}`, true},
		{"permission", 403, `{"error":{"type":"permission_error","message":"insufficient balance"}}`, false},
		{"content guard", 403, `{"error":{"type":"content_policy_violation","message":"insufficient balance"}}`, false},
		{"quoted phrase", 403, `{"error":{"type":"billing_error","message":"Prompt contained insufficient balance"}}`, false},
		{"request guard", 429, `{"error":{"code":"quota_guard_request_too_large","message":"request budget"}}`, false},
		{"session guard", 429, `{"error":{"code":"quota_5h_seat_budget","message":"session budget"}}`, false},
	} {
		for _, protocol := range []string{"messages", "openai", "anthropic chat", "anthropic responses"} {
			for _, stream := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/stream=%t", tc.name, protocol, stream), func(t *testing.T) {
					rec := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(rec)
					c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
					if protocol == "openai" || protocol == "anthropic responses" {
						c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
					}
					if stream {
						c.Header("Content-Type", "text/event-stream")
						n, err := c.Writer.WriteString(": ping\n\n")
						require.NoError(t, err)
						recordGatewayStreamHeartbeat(c, n)
						c.Writer.Flush()
					}
					failure := &service.UpstreamFailoverError{StatusCode: tc.status, ResponseBody: []byte(tc.body)}
					switch protocol {
					case "messages":
						(&GatewayHandler{}).handleFailoverExhausted(c, failure, service.PlatformAnthropic, stream)
					case "openai":
						(&OpenAIGatewayHandler{}).handleFailoverExhausted(c, failure, stream)
					case "anthropic chat":
						(&GatewayHandler{}).handleCCFailoverExhausted(c, failure, stream)
					case "anthropic responses":
						(&GatewayHandler{}).handleResponsesFailoverExhausted(c, failure, stream)
					}
					if stream {
						require.Equal(t, http.StatusOK, rec.Code)
					}
					if protocol == "anthropic chat" && stream {
						require.Equal(t, ": ping\n\n", rec.Body.String(), "preserve the existing stream without replaying or replacing it")
						return
					}
					if tc.wallet {
						require.Equal(t, tc.status, c.GetInt(service.OpsUpstreamStatusCodeKey), "Ops must preserve the provider's original status")
						if !stream {
							require.Equal(t, http.StatusTooManyRequests, rec.Code)
						} else {
							streamErr, ok := service.GetOpsStreamError(c)
							require.True(t, ok)
							require.Equal(t, http.StatusTooManyRequests, streamErr.IntendedStatus)
						}
						require.Contains(t, rec.Body.String(), `"code":"insufficient_quota"`)
						require.Contains(t, rec.Body.String(), service.UpstreamQuotaExhaustedMessage)
						require.NotContains(t, rec.Body.String(), "insufficient balance")
						if stream && (protocol == "openai" || protocol == "anthropic responses") {
							require.Equal(t, 1, strings.Count(rec.Body.String(), "event: response.failed"))
						}
					} else {
						require.NotContains(t, rec.Body.String(), "insufficient_quota")
						if !stream {
							wantStatus := tc.status
							if tc.status == 403 && (protocol == "messages" || protocol == "openai") {
								wantStatus = http.StatusBadGateway
							}
							require.Equal(t, wantStatus, rec.Code)
						}
					}
				})
			}
		}
	}
}

func TestResponsesWalletFailurePreservesForwardedTerminal(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	official := "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"code\":\"server_error\",\"message\":\"official failure\"}}}\n\n"
	_, err := c.Writer.WriteString(official)
	require.NoError(t, err)
	service.MarkOpsStreamError(c, "server_error", "official failure", http.StatusBadGateway)
	(&GatewayHandler{}).handleResponsesFailoverExhausted(c, &service.UpstreamFailoverError{
		StatusCode: 403, ResponseBody: []byte(`{"error":{"type":"billing_error","message":"insufficient balance"}}`),
	}, true)
	require.Equal(t, official, rec.Body.String())
	streamErr, ok := service.GetOpsStreamError(c)
	require.True(t, ok)
	require.Equal(t, "official failure", streamErr.Message)
}
