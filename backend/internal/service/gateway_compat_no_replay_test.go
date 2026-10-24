//go:build unit

package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestCompatStreamProgressPreservesMeteringAndTerminalStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, endpoint := range []string{"chat/completions", "responses"} {
		for _, tc := range []struct {
			name, prefix string
			input, cache int
			progress     bool
		}{
			{name: "no_generation"},
			{name: "zero_usage_generation", prefix: `{"type":"message_start","message":{"id":"msg_test","model":"claude-sonnet-4-6","usage":{"input_tokens":0,"output_tokens":0}}}`, progress: true},
			{name: "cache_only_generation", prefix: `{"type":"message_start","message":{"id":"msg_test","model":"claude-sonnet-4-6","usage":{"input_tokens":0,"output_tokens":0,"cache_read_input_tokens":9}}}`, cache: 9, progress: true},
			{name: "input_generation", prefix: `{"type":"message_start","message":{"id":"msg_test","model":"claude-sonnet-4-6","usage":{"input_tokens":11,"output_tokens":0}}}`, input: 11, progress: true},
		} {
			for _, statusCase := range []struct {
				kind   string
				status int
			}{{"invalid_request_error", 400}, {"rate_limit_error", 429}, {"api_error", 500}, {"overloaded_error", 529}} {
				t.Run(endpoint+"/"+tc.name+"/"+statusCase.kind, func(t *testing.T) {
					rec := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(rec)
					body := `{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"synthetic"}],"stream":true}`
					if endpoint == "responses" {
						body = `{"model":"claude-sonnet-4-6","input":"synthetic","stream":true}`
					}
					c.Request = httptest.NewRequest(http.MethodPost, "/v1/"+endpoint, strings.NewReader(body))
					stream := ""
					if tc.prefix != "" {
						stream = "event: message_start\ndata: " + tc.prefix + "\n\n"
					}
					stream += fmt.Sprintf("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":%q,\"message\":\"synthetic failure\"}}\n\n", statusCase.kind)
					upstream := &anthropicHTTPUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(stream))}}
					svc := newForwardPartialUsageServiceForTest(upstream)
					repo := &gatewayForwardErrorPolicyRepoStub{}
					svc.rateLimitService = NewRateLimitService(repo, nil, svc.cfg, nil, nil)
					account := newAnthropicAPIKeyAccountForTest()
					var result *ForwardResult
					var err error
					if endpoint == "responses" {
						result, err = svc.ForwardAsResponses(context.Background(), c, account, []byte(body), nil)
					} else {
						result, err = svc.ForwardAsChatCompletions(context.Background(), c, account, []byte(body), nil)
					}
					require.Error(t, err)
					var failover *UpstreamFailoverError
					if tc.progress {
						require.False(t, errors.As(err, &failover), "accepted generation is never replayed")
						require.True(t, IsResponseCommitted(c))
						require.Zero(t, repo.overloadCalls, "terminal errors must not penalize a shared account")
						ops, ok := GetOpsStreamError(c)
						require.True(t, ok)
						require.Equal(t, statusCase.status, ops.IntendedStatus)
						if tc.input+tc.cache > 0 {
							require.NotNil(t, result)
							require.Equal(t, tc.input, result.Usage.InputTokens)
							require.Equal(t, tc.cache, result.Usage.CacheReadInputTokens)
							require.Zero(t, result.Usage.OutputTokens)
						} else {
							require.Nil(t, result, "do not invent usage")
						}
					} else {
						require.ErrorAs(t, err, &failover)
						require.Equal(t, statusCase.status, failover.StatusCode)
						require.Nil(t, result)
						require.Empty(t, rec.Body.String())
					}
					events := c.MustGet(OpsUpstreamErrorsKey).([]*OpsUpstreamErrorEvent)
					require.Len(t, events, 1)
					require.Equal(t, statusCase.status, events[0].UpstreamStatusCode)
				})
			}
		}
	}
}

func TestCompatStreamClientErrorsHideProviderAddresses(t *testing.T) {
	for endpoint, handle := range compatStreamHandlers() {
		for _, message := range []string{
			"https://provider.example/v1?api_key=sk-SYNTHETIC",
			"derouter.ai upstream failed at 192.0.2.8",
			"failed 2001:db8::7:443 api.provider.example",
			"Authorization: Bearer SYNTHETIC_TOKEN cookie=synthetic",
		} {
			t.Run(endpoint+"/"+message, func(t *testing.T) {
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				stream := anthropicStreamWithInBandError(message, "")
				result, err := handle(&GatewayService{}, &http.Response{Header: http.Header{}, Body: io.NopCloser(strings.NewReader(stream))}, c)
				require.Error(t, err)
				require.NotNil(t, result)
				require.Contains(t, rec.Body.String(), "Overloaded")
				for _, needle := range []string{"https://", "provider.example", "derouter.ai", "192.0.2.8", "2001:db8", "SYNTHETIC_TOKEN", "sk-SYNTHETIC"} {
					require.NotContains(t, rec.Body.String(), needle)
				}
				var private *sseStreamErrorEventError
				require.ErrorAs(t, err, &private)
				require.Contains(t, private.RawData, message, "private diagnostics retain the upstream cause")
			})
		}
	}
}

func TestCompatStreamMalformedErrorTypeDoesNotBlameClient(t *testing.T) {
	for _, payload := range []string{`{"error":{"message":"no type"}}`, `{"error":{"type":"https://provider.example","message":"bad type"}}`, `{"error":{"type":"sk-SECRET","message":"bad type"}}`} {
		stream := &anthropicCompatStream{body: io.NopCloser(strings.NewReader(""))}
		stream.observe(&apicompat.AnthropicStreamEvent{Type: "error"}, payload)
		require.Equal(t, "upstream_error", stream.upstreamErrType)
		require.NotContains(t, stream.upstreamErrMessage, "provider.example")
		require.NotContains(t, stream.upstreamErrMessage, "SECRET")
	}
}
