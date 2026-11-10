//go:build unit

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/claude"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestFable52FallbackPrices(t *testing.T) {
	for _, model := range []string{"claude-fable-5-2", "anthropic/claude-fable-5.2", "claude-fable-5-2-20261109"} {
		t.Run(model, func(t *testing.T) {
			p, err := newTestBillingService().GetModelPricing(model)
			require.NoError(t, err)
			require.InDelta(t, 8e-6, p.InputPricePerToken, 1e-12)
			require.InDelta(t, 40e-6, p.OutputPricePerToken, 1e-12)
			require.InDelta(t, .2e-6, p.CacheReadPricePerToken, 1e-12)
			require.InDelta(t, 10e-6, p.CacheCreation5mPrice, 1e-12)
			require.InDelta(t, 16e-6, p.CacheCreation1hPrice, 1e-12)
			require.True(t, p.SupportsCacheBreakdown)
			require.Empty(t, p.ReasoningEffortMultipliers)
		})
	}
}

func TestFable52FastUsesMappedTargetAndPreservesUnsupportedPlatforms(t *testing.T) {
	account := newAnthropicAPIKeyAccountForTest()
	for _, tc := range []struct {
		requested, target string
		fast              bool
	}{
		{"public-alias", "claude-fable-5-2", true},
		{"public-alias", "anthropic/claude-fable-5.2", true},
		{"claude-fable-5-2", "claude-fable-5-1", false},
		{"claude-fable-5-20", "", false},
	} {
		model := anthropicSpeedModel(&ParsedRequest{Model: tc.requested}, &ForwardResult{UpstreamModel: tc.target})
		require.Equal(t, tc.fast, anthropicSpeedServiceTier(account, "fast", model) != nil, model)
	}
	for _, account := range []*Account{nil, {Platform: PlatformOpenAI}, {Platform: PlatformAnthropic, Type: AccountTypeBedrock}} {
		require.Nil(t, anthropicSpeedServiceTier(account, "fast", "claude-fable-5-2"))
	}
}

func TestFable52CatalogFastPricingAndExplicitOverride(t *testing.T) {
	pricing := newStubPricingServiceFromJSON(t, `{"claude-fable-5-2":{"litellm_provider":"anthropic","mode":"chat",
		"input_cost_per_token":8e-6,"output_cost_per_token":40e-6,"cache_read_input_token_cost":0.2e-6,
		"cache_creation_input_token_cost":10e-6,"cache_creation_input_token_cost_above_1hr":16e-6,
		"input_cost_per_token_priority":24e-6,"output_cost_per_token_priority":120e-6}}`)
	svc := NewBillingService(&config.Config{}, pricing)
	tokens := UsageTokens{InputTokens: 100, OutputTokens: 50, CacheReadTokens: 200, CacheCreationTokens: 20, CacheCreation5mTokens: 7, CacheCreation1hTokens: 13}
	want := 100*8e-6 + 50*40e-6 + 200*.2e-6 + 7*10e-6 + 13*16e-6
	for _, tier := range []string{"", "fast"} {
		cost, err := svc.CalculateCostWithServiceTier("claude-fable-5-2", tokens, .7, tier)
		require.NoError(t, err)
		multiplier := 1.0
		if tier == "fast" {
			multiplier = 2
		}
		require.InDelta(t, want*multiplier, cost.TotalCost, 1e-12)
		require.InDelta(t, want*multiplier*.7, cost.ActualCost, 1e-12)
	}
	custom := &ChannelModelPricing{FastMultiplier: testPtrFloat64(1.5)}
	cost, err := svc.calculateCostInternal("claude-fable-5-2", tokens, .7, "fast", custom)
	require.NoError(t, err)
	require.InDelta(t, want*1.5, cost.TotalCost, 1e-12)
	require.InDelta(t, want*1.5*.7, cost.ActualCost, 1e-12)
}

func TestFable52FastForwardAndBilling(t *testing.T) {
	for _, mode := range []string{"passthrough", "apikey", "oauth"} {
		for _, stream := range []bool{false, true} {
			for _, tc := range []struct {
				requested, observed string
				multiplier          float64
			}{
				{"fast", "fast", 2}, {"fast", "standard", 1}, {"fast", "", 2},
				{"standard", "standard", 1}, {"standard", "fast", 1}, {"", "fast", 1},
			} {
				t.Run(fmt.Sprintf("%s/stream=%v/%s/%s", mode, stream, tc.requested, tc.observed), func(t *testing.T) {
					usage := map[string]any{"input_tokens": 100, "output_tokens": 50, "cache_read_input_tokens": 200,
						"cache_creation_input_tokens": 20, "cache_creation": map[string]int{"ephemeral_5m_input_tokens": 7, "ephemeral_1h_input_tokens": 13}}
					if tc.observed != "" {
						usage["speed"] = tc.observed
					}
					message := map[string]any{"id": "msg_fable52", "type": "message", "role": "assistant", "model": "claude-fable-5-2",
						"content": []any{map[string]string{"type": "text", "text": "OK"}}, "stop_reason": "end_turn", "usage": usage}
					wire, err := json.Marshal(message)
					require.NoError(t, err)
					response, ct := string(wire), "application/json"
					if stream {
						message["content"], message["stop_reason"], usage["output_tokens"] = []any{}, nil, 0
						wire, err = json.Marshal(message)
						require.NoError(t, err)
						response = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":" + string(wire) + "}\n\n" +
							"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
							"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"OK\"}}\n\n" +
							"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
							"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":50}}\n\n" +
							"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
						ct = "text/event-stream"
					}
					up := &anthropicHTTPUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {ct}}, Body: io.NopCloser(strings.NewReader(response))}}
					svc := newForwardPartialUsageServiceForTest(up)
					account := newAnthropicAPIKeyAccountForTest()
					if mode == "apikey" {
						account.Extra["anthropic_passthrough"] = false
					}
					if mode == "oauth" {
						account = newAnthropicOAuthAccountForPartialUsageTest()
					}
					speedField := ""
					if tc.requested != "" {
						speedField = fmt.Sprintf(`"speed":%q,`, tc.requested)
					}
					body := fmt.Sprintf(`{"model":"claude-fable-5-2",%s"max_tokens":256,"stream":%v,"messages":[{"role":"user","content":[{"type":"text","text":"synthetic","cache_control":{"type":"ephemeral","ttl":"5m"}}]}]}`, speedField, stream)
					parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(body)), PlatformAnthropic)
					require.NoError(t, err)
					c, _ := gin.CreateTestContext(httptest.NewRecorder())
					c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
					result, err := svc.Forward(context.Background(), c, account, parsed)
					require.NoError(t, err)
					require.Equal(t, "claude-fable-5-2", gjson.GetBytes(up.lastBody, "model").String())
					require.Equal(t, tc.requested, gjson.GetBytes(up.lastBody, "speed").String())
					require.Equal(t, tc.requested != "", gjson.GetBytes(up.lastBody, "speed").Exists())
					require.Equal(t, "5m", gjson.GetBytes(up.lastBody, "messages.0.content.0.cache_control.ttl").String())
					require.Equal(t, 7, result.Usage.CacheCreation5mTokens)
					require.Equal(t, 13, result.Usage.CacheCreation1hTokens)
					require.Equal(t, tc.observed, result.UpstreamResponseServiceTier)
					if tc.requested == "fast" {
						require.Equal(t, "fast", optionalStringValue(result.ServiceTier))
					}
					logs := &openAIRecordUsageBestEffortLogRepoStub{}
					billing, key := newGatewayRecordUsageServiceWithResolverForTest(logs)
					key.Group.RateMultiplier = .7
					err = billing.RecordUsage(context.Background(), &RecordUsageInput{Result: result, APIKey: key, User: &User{ID: 1}, Account: account})
					require.NoError(t, err)
					require.NotNil(t, logs.lastLog)
					want := (100*8e-6 + 50*40e-6 + 200*.2e-6 + 7*10e-6 + 13*16e-6) * tc.multiplier
					require.InDelta(t, want, logs.lastLog.TotalCost, 1e-12)
					require.InDelta(t, want*.7, logs.lastLog.ActualCost, 1e-12)
				})
			}
		}
	}
}

func TestAnthropicFastBodyObeysBetaBlockWithoutHeader(t *testing.T) {
	settings, err := json.Marshal(BetaPolicySettings{Rules: []BetaPolicyRule{{
		BetaToken: claude.BetaFastMode, Action: BetaPolicyActionBlock, Scope: BetaPolicyScopeAll,
	}}})
	require.NoError(t, err)
	for _, model := range []string{"claude-fable-5-2", "claude-opus-5-5"} {
		t.Run(model, func(t *testing.T) {
			account := newAnthropicAPIKeyAccountForTest()
			account.Credentials["model_mapping"] = map[string]any{"public-alias": model}
			upstream := &anthropicHTTPUpstreamRecorder{err: fmt.Errorf("unexpected dispatch")}
			svc := newForwardPartialUsageServiceForTest(upstream)
			svc.settingService = NewSettingService(&betaPolicySettingRepoStub{values: map[string]string{
				SettingKeyBetaPolicySettings: string(settings),
			}}, &config.Config{})
			body := []byte(`{"model":"public-alias","speed":"fast","max_tokens":128,"messages":[{"role":"user","content":"synthetic"}]}`)
			parsed, err := ParseGatewayRequest(NewRequestBodyRef(body), PlatformAnthropic)
			require.NoError(t, err)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			result, err := svc.Forward(context.Background(), c, account, parsed)
			var blocked *BetaBlockedError
			require.ErrorAs(t, err, &blocked)
			require.Nil(t, result)
			require.Nil(t, upstream.lastReq)
		})
	}
}
