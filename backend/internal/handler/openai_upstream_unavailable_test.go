package handler

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/model"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIUpstreamUnavailable_FailoverKeepsStatusAndRootCause(t *testing.T) {
	h := &OpenAIGatewayHandler{}
	for _, path := range []string{"/v1/responses", "/v1/responses/compact", "/v1/chat/completions"} {
		for _, committed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/committed=%t", path, committed), func(t *testing.T) {
				c, rec := newGinContextForEndpoint(t, path)
				if committed {
					c.Header("Content-Type", "text/event-stream")
					_, _ = c.Writer.WriteString(": ping\n\n")
					c.Writer.Flush()
				}
				h.handleFailoverExhausted(c, &service.UpstreamFailoverError{
					StatusCode:      http.StatusServiceUnavailable,
					ResponseHeaders: http.Header{"Retry-After": {"60"}},
					ResponseBody:    []byte(`{"error":{"type":"upstream_unavailable","message":"No eligible upstream account is available"}}`),
				}, committed)
				body, prefix := rec.Body.String(), "error."
				if committed {
					require.Equal(t, http.StatusOK, rec.Code)
					streamErr, ok := service.GetOpsStreamError(c)
					require.True(t, ok)
					require.Equal(t, http.StatusServiceUnavailable, streamErr.IntendedStatus)
					if strings.HasPrefix(path, "/v1/responses") {
						require.Equal(t, 1, strings.Count(body, "event: response.created\n"))
						require.Equal(t, 1, strings.Count(body, "event: response.failed\n"))
						body = strings.Split(body, "event: response.failed\ndata: ")[1]
						prefix = "response.error."
					} else {
						body = strings.Split(body, "data: ")[1]
					}
				} else {
					require.Equal(t, http.StatusServiceUnavailable, rec.Code)
					require.Equal(t, "60", rec.Header().Get("Retry-After"))
				}
				require.Equal(t, "upstream_unavailable", gjson.Get(body, prefix+"type").String())
				require.Equal(t, "No eligible upstream account is available", gjson.Get(body, prefix+"message").String())
				require.Equal(t, int64(http.StatusServiceUnavailable), gjson.Get(body, prefix+"status_code").Int())
				require.NotContains(t, body, `"usage"`)
			})
		}
	}
}

func TestOpenAIUpstreamUnavailable_Unknown503RetainsStatusWithSafeMessage(t *testing.T) {
	c, rec := newGinContextForEndpoint(t, "/v1/responses")
	h := &OpenAIGatewayHandler{}
	h.handleFailoverExhausted(c, &service.UpstreamFailoverError{
		StatusCode:   http.StatusServiceUnavailable,
		ResponseBody: []byte(`{"error":{"type":"server_error","message":"unclassified service error"}}`),
	}, false)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "upstream_error", gjson.Get(rec.Body.String(), "error.type").String())
	require.Equal(t, "Upstream service temporarily unavailable", gjson.Get(rec.Body.String(), "error.message").String())
}

type upstreamUnavailableRuleRepo struct {
	service.ErrorPassthroughRepository
	rule *model.ErrorPassthroughRule
}

func (r *upstreamUnavailableRuleRepo) List(context.Context) ([]*model.ErrorPassthroughRule, error) {
	return []*model.ErrorPassthroughRule{r.rule}, nil
}

func TestOpenAIUpstreamUnavailable_PreservesConfiguredRulePriority(t *testing.T) {
	status, message := http.StatusConflict, "Administrator-defined response"
	rules := service.NewErrorPassthroughService(&upstreamUnavailableRuleRepo{rule: &model.ErrorPassthroughRule{
		ID: 1, Enabled: true, ErrorCodes: []int{503}, Keywords: []string{"upstream_unavailable"},
		MatchMode: "all", Platforms: []string{service.PlatformOpenAI},
		ResponseCode: &status, CustomMessage: &message,
	}}, nil)
	h := &OpenAIGatewayHandler{errorPassthroughService: rules}
	c, rec := newGinContextForEndpoint(t, "/v1/responses")
	h.handleFailoverExhausted(c, &service.UpstreamFailoverError{
		StatusCode:   http.StatusServiceUnavailable,
		ResponseBody: []byte(`{"error":{"type":"upstream_unavailable","message":"No eligible upstream account is available"}}`),
	}, false)
	require.Equal(t, status, rec.Code)
	require.Equal(t, message, gjson.Get(rec.Body.String(), "error.message").String())
}

func TestOpenAIUpstreamUnavailable_PreservesQuotaPriority(t *testing.T) {
	c, rec := newGinContextForEndpoint(t, "/v1/responses")
	h := &OpenAIGatewayHandler{}
	h.handleFailoverExhausted(c, &service.UpstreamFailoverError{
		StatusCode:   http.StatusTooManyRequests,
		ResponseBody: []byte(`{"error":{"type":"upstream_unavailable","code":"insufficient_quota","message":"Insufficient account balance"}}`),
	}, false)
	require.Equal(t, http.StatusTooManyRequests, rec.Code)
	require.Equal(t, "insufficient_quota", gjson.Get(rec.Body.String(), "error.code").String())
	require.Equal(t, service.UpstreamQuotaExhaustedMessage, gjson.Get(rec.Body.String(), "error.message").String())
}
