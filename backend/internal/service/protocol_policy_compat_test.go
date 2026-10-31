package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestProtocolPolicyCompatDoesNotFailover(t *testing.T) {
	for _, endpoint := range []string{"chat/completions", "responses"} {
		for _, upstreamStream := range []bool{false, true} {
			for _, clientStream := range []bool{false, true} {
				for _, metered := range []bool{false, true} {
					for _, precommitted := range []bool{false, true} {
						if precommitted && (!clientStream || upstreamStream) {
							continue
						}
						t.Run(fmt.Sprintf("%s/upstreamSSE=%t/clientSSE=%t/metered=%t/precommitted=%t", endpoint, upstreamStream, clientStream, metered, precommitted), func(t *testing.T) {
							rec := httptest.NewRecorder()
							c, _ := gin.CreateTestContext(rec)
							body := fmt.Sprintf(`{"model":"claude-sonnet-5-5","stream":%t,"messages":[{"role":"user","content":"synthetic"}]}`, clientStream)
							if endpoint == "responses" {
								body = fmt.Sprintf(`{"model":"claude-sonnet-5-5","stream":%t,"input":"synthetic"}`, clientStream)
							}
							c.Request = httptest.NewRequest(http.MethodPost, "/v1/"+endpoint, strings.NewReader(body))
							if precommitted {
								c.Header("Content-Type", "text/event-stream")
								_, _ = c.Writer.WriteString(": ping\n\n")
								c.Writer.Flush()
							}
							failure := `{"type":"error","error":{"type":"permission_error","code":"policy_blocked","message":"request rejected https://private-provider.invalid/?token=secret"}`
							if metered {
								failure += `,"usage":{"input_tokens":12,"output_tokens":7,"cache_read_input_tokens":5}}`
							} else {
								failure += "}"
							}
							status, contentType, upstreamBody := 403, "application/json", failure
							if upstreamStream {
								status, contentType, upstreamBody = 200, "text/event-stream", ""
								if metered {
									upstreamBody = "event: message_start\ndata: " + `{"type":"message_start","message":{"id":"synthetic","type":"message","role":"assistant","content":[],"model":"claude-sonnet-5-5","usage":{"input_tokens":12,"cache_read_input_tokens":5}}}` + "\n\n"
								}
								upstreamBody += "event: error\ndata: " + failure + "\n\n"
							}
							upstream := &queuedHTTPUpstreamStub{responses: []*http.Response{{StatusCode: status, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(upstreamBody))}}}
							svc := newForwardPartialUsageServiceForTest(nil)
							svc.httpUpstream = upstream
							account := newAnthropicAPIKeyAccountForTest()
							account.Credentials["custom_error_codes_enabled"] = true
							account.Credentials["custom_error_codes"] = []any{float64(429)}
							var result *ForwardResult
							var err error
							if endpoint == "responses" {
								result, err = svc.ForwardAsResponses(context.Background(), c, account, []byte(body), nil)
							} else {
								result, err = svc.ForwardAsChatCompletions(context.Background(), c, account, []byte(body), nil)
							}
							require.Error(t, err)
							var failover *UpstreamFailoverError
							require.False(t, errors.As(err, &failover), "policy denial must not dispatch another account")
							require.Equal(t, 1, upstream.callCount)
							if metered {
								require.NotNil(t, result)
								require.Equal(t, 12, result.Usage.InputTokens)
								require.Equal(t, 7, result.Usage.OutputTokens)
								require.Equal(t, 5, result.Usage.CacheReadInputTokens)
							} else {
								require.Nil(t, result)
							}
							require.Equal(t, 1, strings.Count(rec.Body.String(), `"code":"policy_blocked"`), "exactly one terminal denial")
							require.Contains(t, rec.Body.String(), `"status_code":403`)
							require.NotContains(t, rec.Body.String(), "response.completed")
							require.NotContains(t, rec.Body.String(), "[DONE]")
							streamCommitted := precommitted || (clientStream && upstreamStream && metered)
							if streamCommitted {
								require.Equal(t, http.StatusOK, rec.Code)
								require.Contains(t, rec.Header().Get("Content-Type"), "text/event-stream")
								frames := 0
								for _, line := range strings.Split(rec.Body.String(), "\n") {
									if raw, ok := strings.CutPrefix(line, "data: "); ok {
										require.True(t, json.Valid([]byte(raw)), "each SSE data frame must parse")
										frames++
									}
								}
								require.Positive(t, frames)
								if endpoint == "responses" {
									require.Equal(t, 1, strings.Count(rec.Body.String(), `"type":"response.failed"`))
								}
							} else {
								require.Equal(t, http.StatusForbidden, rec.Code)
								require.True(t, json.Valid(rec.Body.Bytes()), "uncommitted denial must be one JSON object")
							}
							require.NotContains(t, rec.Body.String(), "private-provider.invalid")
							require.NotContains(t, rec.Body.String(), "token=secret")
							events := c.MustGet(OpsUpstreamErrorsKey).([]*OpsUpstreamErrorEvent)
							require.Len(t, events, 1)
							require.Equal(t, 403, events[0].UpstreamStatusCode)
							require.True(t, IsResponseCommitted(c))
						})
					}
				}
			}
		}
	}
}

func TestUnknownCompat403RetainsFailover(t *testing.T) {
	for _, endpoint := range []string{"chat/completions", "responses"} {
		for _, sse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/sse=%t", endpoint, sse), func(t *testing.T) {
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				body := `{"model":"claude-sonnet-5-5","stream":true,"messages":[{"role":"user","content":"synthetic"}]}`
				if endpoint == "responses" {
					body = `{"model":"claude-sonnet-5-5","stream":true,"input":"synthetic"}`
				}
				c.Request = httptest.NewRequest("POST", "/v1/"+endpoint, strings.NewReader(body))
				response := `{"type":"error","error":{"type":"permission_error","message":"unclassified credentials error"}}`
				status := 403
				if sse {
					status = 200
					response = "event: error\ndata: " + response + "\n\n"
				}
				up := &queuedHTTPUpstreamStub{responses: []*http.Response{{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(response))}}}
				svc := newForwardPartialUsageServiceForTest(nil)
				svc.httpUpstream = up
				account := newAnthropicAPIKeyAccountForTest()
				account.Credentials["custom_error_codes_enabled"] = true
				account.Credentials["custom_error_codes"] = []any{float64(429)}
				var err error
				if endpoint == "responses" {
					_, err = svc.ForwardAsResponses(context.Background(), c, account, []byte(body), nil)
				} else {
					_, err = svc.ForwardAsChatCompletions(context.Background(), c, account, []byte(body), nil)
				}
				var failover *UpstreamFailoverError
				require.ErrorAs(t, err, &failover)
				require.Equal(t, 403, failover.StatusCode)
			})
		}
	}
}
