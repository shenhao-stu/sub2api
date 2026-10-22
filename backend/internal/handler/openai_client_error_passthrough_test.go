package handler

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/model"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type clientErrorRuleRepo struct {
	service.ErrorPassthroughRepository
}

func (*clientErrorRuleRepo) List(context.Context) ([]*model.ErrorPassthroughRule, error) {
	return []*model.ErrorPassthroughRule{{
		ID: 2, Enabled: true, ErrorCodes: []int{400, 422}, Keywords: []string{`"message"`},
		MatchMode: "all", Platforms: []string{service.PlatformOpenAI}, PassthroughCode: true, PassthroughBody: true,
	}}, nil
}

func TestOpenAIClientErrorPassthrough_FailoverExhaustedKeepsFields(t *testing.T) {
	rules := service.NewErrorPassthroughService(&clientErrorRuleRepo{}, nil)
	h := &OpenAIGatewayHandler{errorPassthroughService: rules}
	for _, committed := range []bool{false, true} {
		c, rec := newGinContextForEndpoint(t, "/v1/responses")
		if committed {
			c.Header("Content-Type", "text/event-stream")
			_, _ = c.Writer.WriteString(": ping\n\n")
			c.Writer.Flush()
		}
		h.handleFailoverExhausted(c, &service.UpstreamFailoverError{
			StatusCode:   http.StatusBadRequest,
			ResponseBody: []byte(`{"error":{"type":"invalid_request_error","code":"invalid_request","param":"input[535].name","message":"name must be a non-empty string"}}`),
		}, committed)
		body, prefix := rec.Body.String(), "error."
		if committed {
			require.Equal(t, 200, rec.Code)
			require.Equal(t, 1, strings.Count(body, "event: response.created\n"))
			require.Equal(t, 1, strings.Count(body, "event: response.failed\n"))
			body = strings.Split(body, "event: response.failed\ndata: ")[1]
			prefix = "response.error."
		} else {
			require.Equal(t, 400, rec.Code)
		}
		require.Equal(t, "invalid_request_error", gjson.Get(body, prefix+"type").String())
		require.Equal(t, "invalid_request", gjson.Get(body, prefix+"code").String())
		require.Equal(t, "input[535].name", gjson.Get(body, prefix+"param").String())
		require.Equal(t, int64(400), gjson.Get(body, prefix+"status_code").Int())
		require.NotContains(t, body, "usage")
	}
}
