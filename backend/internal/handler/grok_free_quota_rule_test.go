package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/model"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

type grokFreeRuleRepo struct {
	service.ErrorPassthroughRepository
	rule *model.ErrorPassthroughRule
}

func (r *grokFreeRuleRepo) List(context.Context) ([]*model.ErrorPassthroughRule, error) {
	return []*model.ErrorPassthroughRule{r.rule}, nil
}

func TestGrokFreeQuotaOperatorRule(t *testing.T) {
	data, err := os.ReadFile("../../../deploy/ops/grok_free_quota_error_rule.json")
	require.NoError(t, err)
	var rule model.ErrorPassthroughRule
	require.NoError(t, json.Unmarshal(data, &rule))
	require.NoError(t, rule.Validate())
	rules := service.NewErrorPassthroughService(&grokFreeRuleRepo{rule: &rule}, nil)
	body := []byte(`{"code":"subscription:free-usage-exhausted","error":"You've used all the included free usage for model grok-4.7 for now. Usage resets over a rolling 24-hour window — tokens (actual/limit): 516035/500000. Upgrade: https://provider.invalid/private"}`)
	for _, platform := range []string{service.PlatformOpenAI, service.PlatformGrok} {
		require.NotNil(t, rules.MatchRule(platform, 429, body))
		require.Nil(t, rules.MatchRule(platform, 400, body))
		require.Nil(t, rules.MatchRule(platform, 429, []byte(`{"error":"Rate limit exceeded"}`)))
	}
	require.Nil(t, rules.MatchRule(service.PlatformAnthropic, 429, body))
	for _, committed := range []bool{false, true} {
		c, rec := newGinContextForEndpoint(t, "/v1/responses")
		if committed {
			c.Header("Content-Type", "text/event-stream")
			_, err := c.Writer.WriteString(": ping\n\n")
			require.NoError(t, err)
			c.Writer.Flush()
		}
		(&OpenAIGatewayHandler{errorPassthroughService: rules}).handleFailoverExhausted(c,
			&service.UpstreamFailoverError{StatusCode: http.StatusTooManyRequests, ResponseBody: body}, committed)
		expectedHTTP := http.StatusTooManyRequests
		if committed {
			expectedHTTP = http.StatusOK
		}
		require.Equal(t, expectedHTTP, rec.Code)
		require.Contains(t, rec.Body.String(), "免费套餐用量已耗尽")
		require.NotContains(t, rec.Body.String(), "provider.invalid")
		require.NotContains(t, rec.Body.String(), "516035")
		require.NotContains(t, rec.Body.String(), `"usage"`)
	}
}
