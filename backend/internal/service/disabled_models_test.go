package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func disabledModelsFixture() *config.Config {
	return &config.Config{Gateway: config.GatewayConfig{DisabledModels: []string{"gpt-5.5", "gpt-5.5-*"}}}
}

func TestDisabledModelWireSelectors(t *testing.T) {
	for _, body := range []string{
		`{"model":"gpt-6.1-sol","MODEL":"gpt-5.5"}`,
		`{"model":"gpt-6.1-sol","tools":[{"type":"image_generation","model":"gpt-5.5"}]}`,
		`{"model":"gpt-6.1-sol","tools":[],"TOOLS":[{"TYPE":"image_generation","MODEL":"gpt-5.5"}]}`,
		`{"model":"gpt-6.1-sol","tools":[{"type":"image_generation","model":"gpt-image-2","model":"gpt-5.5"}]}`,
	} {
		require.ErrorIs(t, RequireEnabledWireModels(disabledModelsFixture(), []byte(body)), ErrModelDisabled)
		require.ErrorIs(t, rejectDisabledWireModels(nil, disabledModelsFixture(), []byte(body)), ErrModelDisabled)
	}
	for _, body := range []string{
		`{"model":"gpt-6.1-sol","input":"gpt-5.5","tools":[{"type":"function","name":"x","parameters":{"model":"gpt-5.5"}}]}`,
		`{"model":"gpt-6.1-sol","tools":[{"type":"image_generation","model":"gpt-image-2.5-flare"}]}`,
	} {
		require.NoError(t, RequireEnabledWireModels(disabledModelsFixture(), []byte(body)))
	}
}

func TestDisabledModelCatalogUsesEndpointWinner(t *testing.T) {
	targets := func(routes []CompositeModelRoute, model, endpoint string) []string {
		var ids []string
		for _, route := range disabledPolicyCatalogRoutes(routes, model, endpoint) {
			ids = append(ids, route.UpstreamModel)
		}
		return ids
	}
	routes := []CompositeModelRoute{
		{ID: 1, Enabled: true, PublicModel: "alias", MatchType: CompositeRouteMatchExact, Endpoint: CompositeRouteEndpointMessages, UpstreamModel: "gpt-5.5", Priority: 1},
		{ID: 2, Enabled: true, PublicModel: "alias", MatchType: CompositeRouteMatchExact, Endpoint: CompositeRouteEndpointResponses, UpstreamModel: "gpt-6.1-sol", Priority: 1},
		{ID: 3, Enabled: false, PublicModel: "alias", MatchType: CompositeRouteMatchExact, Endpoint: CompositeRouteEndpointMessages, UpstreamModel: "gpt-6.1-sol", Priority: 0},
	}
	require.Equal(t, []string{"gpt-5.5"}, targets(routes, "alias", CompositeRouteEndpointMessages))
	require.Equal(t, []string{"gpt-6.1-sol"}, targets(routes, "alias", CompositeRouteEndpointResponses))
	require.ElementsMatch(t, []string{"gpt-5.5", "gpt-6.1-sol"}, targets(routes, "alias", ""))
	require.Equal(t, []string{"other"}, targets(routes, "other", ""))
}

func TestDisabledModelNormalizationBoundaries(t *testing.T) {
	cfg := disabledModelsFixture()
	for _, model := range []string{"gpt-5.5", " GPT-5.5 ", "gpt-5.5-pro", "gpt-5.5-high", "gpt-5.5[1m]", "gpt-5.5-openai-compact", "openai/gpt-5.5", "models/gpt-5.5", "GPT_5.5"} {
		t.Run(model, func(t *testing.T) {
			require.NotEmpty(t, DisabledModel(cfg, model))
			require.ErrorIs(t, RequireEnabledModels(cfg, model), ErrModelDisabled)
		})
	}
	for _, model := range []string{"gpt-5.50", "gpt-5.6-sol", "gpt-6.1-sol", "gpt-image-2.5-flare", "gpt-image-2.5-sunburst", "claude-opus-4-7", "grok-4.7-build-fast", "prefix-gpt-5.5", ""} {
		t.Run(model, func(t *testing.T) { require.Empty(t, DisabledModel(cfg, model)) })
	}
	require.Empty(t, DisabledModel(nil, "gpt-5.5"))
	require.Empty(t, DisabledModel(&config.Config{}, "gpt-5.5"))
	require.Empty(t, DisabledModel(&config.Config{Gateway: config.GatewayConfig{DisabledModels: []string{"model?"}}}, "modelx"))
}

func TestDisabledModelOAuthMappedCatalogMembership(t *testing.T) {
	account := Account{Platform: PlatformOpenAI, Type: AccountTypeOAuth, Credentials: map[string]any{"model_mapping": map[string]any{"middle": "gpt-5.5"}}}
	require.False(t, account.IsModelSupported("middle"), "official retirement already excludes this target")
	require.True(t, modelDisabledForAccounts(disabledModelsFixture(), []Account{account}, "middle"), "unsupported retired targets must not disappear from policy evidence")
}

func TestDisabledModelCannotReachOpenAIAdapters(t *testing.T) {
	for _, endpoint := range []string{"/v1/responses", "/v1/responses/compact", "/v1/chat/completions", "/v1/messages", "/v1/embeddings"} {
		for _, mode := range []string{"direct", "mapped_target", "mapped_public", "passthrough", "dispatch", "compact_target", "CN"} {
			t.Run(endpoint+"/"+mode, func(t *testing.T) {
				s := &OpenAIGatewayService{cfg: disabledModelsFixture()}
				account := &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
				model, dispatch := "gpt-5.5", ""
				switch mode {
				case "mapped_target":
					model = "alias"
					account.Credentials = map[string]any{"model_mapping": map[string]any{"alias": "gpt-5.5"}}
				case "mapped_public":
					account.Credentials = map[string]any{"model_mapping": map[string]any{"gpt-5.5": "gpt-6.1-sol"}}
				case "passthrough":
					account.Extra = map[string]any{"openai_passthrough": true}
				case "dispatch":
					if endpoint == "/v1/responses" || endpoint == "/v1/responses/compact" {
						return
					}
					model = "claude-opus-4-7"
					dispatch = "gpt-5.5"
				case "compact_target":
					if endpoint != "/v1/responses/compact" {
						return
					}
					model = "gpt-6.1-sol"
					account.Credentials = map[string]any{"compact_model_mapping": map[string]any{"gpt-6.1-sol": "gpt-5.5"}}
				case "CN":
					account.Platform = PlatformKimi
				}
				raw, _ := json.Marshal(map[string]any{"model": model, "input": "local", "messages": []any{}})
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(string(raw)))
				var result *OpenAIForwardResult
				var err error
				switch endpoint {
				case "/v1/responses", "/v1/responses/compact":
					result, err = s.Forward(context.Background(), c, account, raw)
				case "/v1/chat/completions":
					result, err = s.ForwardAsChatCompletions(context.Background(), c, account, raw, "", dispatch)
				case "/v1/messages":
					result, err = s.ForwardAsAnthropic(context.Background(), c, account, raw, "", dispatch)
				case "/v1/embeddings":
					result, err = s.ForwardEmbeddings(context.Background(), c, account, raw, dispatch)
				}
				require.Nil(t, result)
				require.ErrorIs(t, err, ErrModelDisabled)
				require.Equal(t, 404, rec.Code)
				var failover *UpstreamFailoverError
				require.False(t, errors.As(err, &failover))
			})
		}
	}
}

func TestDisabledModelResolvedGateWithoutBillingIdentity(t *testing.T) {
	s := &OpenAIGatewayService{cfg: disabledModelsFixture()}
	for _, models := range [][4]string{{"alias", "alias", "gpt-5.5", "gpt-5.5"}, {"gpt-5.5", "gpt-6.1-sol", "gpt-6.1-sol", "gpt-6.1-sol"}, {"alias", "gpt-6.1-sol", "gpt-6.1-sol", "gpt-5.5"}} {
		require.ErrorIs(t, s.RequireOpenAIResolvedRequestPricing(context.Background(), nil, nil, models[0], models[1], models[2], models[3]), ErrModelDisabled)
	}
}

func TestDisabledModelCatalogueProjection(t *testing.T) {
	cfg := disabledModelsFixture()
	account := Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"model_mapping": map[string]any{"alias": "gpt-5.5", "gpt-5.5": "gpt-6.1-sol", "gpt-6.1-sol": "gpt-6.1-sol", "gpt-image-2.5-flare": "gpt-image-2.5-flare"}}}
	raw := []byte(`{"object":"list","extra":true,"data":[{"id":"alias"},{"id":"gpt-5.5"},{"id":"gpt-6.1-sol","custom":123},{"id":"gpt-image-2.5-flare"}]}`)
	got, err := FilterDisabledModelsBody(cfg, raw, []Account{account}, false)
	require.NoError(t, err)
	require.JSONEq(t, `{"object":"list","extra":true,"data":[{"id":"gpt-6.1-sol","custom":123},{"id":"gpt-image-2.5-flare"}]}`, string(got))
	require.Equal(t, raw, mustDisabledCatalog(t, nil, raw, []Account{account}))
	only := []byte(`{"data":[{"id":"gpt-5.5"}]}`)
	got, err = FilterDisabledModelsBody(cfg, only, nil, false)
	require.NoError(t, err)
	require.JSONEq(t, `{"data":[]}`, string(got))
	testSvc := &AccountTestService{cfg: cfg}
	candidates, err := testSvc.FilterAvailableModels(&account, []map[string]string{{"id": "alias"}, {"id": "gpt-6.1-sol"}})
	require.NoError(t, err)
	encoded, _ := json.Marshal(candidates)
	require.JSONEq(t, `[{"id":"gpt-6.1-sol"}]`, string(encoded))
}

func mustDisabledCatalog(t *testing.T, cfg *config.Config, body []byte, accounts []Account) []byte {
	t.Helper()
	b, e := FilterDisabledModelsBody(cfg, body, accounts, false)
	require.NoError(t, e)
	return b
}

func TestDisabledModelGatewayTargets(t *testing.T) {
	for _, platform := range []string{PlatformAnthropic, PlatformGemini, PlatformAntigravity} {
		t.Run(platform, func(t *testing.T) {
			account := &Account{Platform: platform, Type: AccountTypeAPIKey, Credentials: map[string]any{"model_mapping": map[string]any{"alias": "gpt-5.5"}}}
			s := &GatewayService{cfg: disabledModelsFixture()}
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
			require.ErrorIs(t, s.RequireForwardPricing(context.Background(), c, account, "alias"), ErrModelDisabled)
			require.Equal(t, 404, rec.Code)
		})
	}
}

func TestDisabledModelCompactGlobalFallbackCannotReachTransport(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		t.Run(map[bool]string{true: "passthrough", false: "normal"}[passthrough], func(t *testing.T) {
			cfg := disabledModelsFixture()
			cfg.Gateway.OpenAICompactModel = "gpt-5.5"
			svc := &OpenAIGatewayService{cfg: cfg}
			account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"api_key": "local-only"}, Extra: map[string]any{"openai_passthrough": passthrough}}
			c := newOpenAICompactFallbackTestContext(t, "/v1/responses/compact")
			result, err := svc.Forward(context.Background(), c, account, []byte(`{"model":"gpt-6.1-sol","instructions":"local","input":[],"stream":false}`))
			require.Nil(t, result)
			require.ErrorIs(t, err, ErrModelDisabled)
		})
	}
}

func TestDisabledModelNativeGeminiAndAntigravityTargets(t *testing.T) {
	for _, endpoint := range []string{"gemini_native", "gemini_messages", "antigravity_messages", "antigravity_native"} {
		t.Run(endpoint, func(t *testing.T) {
			cfg := disabledModelsFixture()
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("POST", "/v1beta/models/alias:generateContent", nil)
			account := &Account{Platform: PlatformGemini, Type: AccountTypeAPIKey, Credentials: map[string]any{"model_mapping": map[string]any{"alias": "gpt-5.5"}}}
			var result *ForwardResult
			var err error
			switch endpoint {
			case "gemini_native":
				result, err = (&GeminiMessagesCompatService{cfg: cfg}).ForwardNative(context.Background(), c, account, "alias", "generateContent", false, []byte(`{"contents":[]}`))
			case "gemini_messages":
				result, err = (&GeminiMessagesCompatService{cfg: cfg}).Forward(context.Background(), c, account, []byte(`{"model":"alias","messages":[]}`))
			case "antigravity_messages":
				account.Platform = PlatformAntigravity
				result, err = (&AntigravityGatewayService{settingService: &SettingService{cfg: cfg}}).Forward(context.Background(), c, account, []byte(`{"model":"alias","messages":[]}`), false)
			case "antigravity_native":
				account.Platform = PlatformAntigravity
				result, err = (&AntigravityGatewayService{settingService: &SettingService{cfg: cfg}}).ForwardGemini(context.Background(), c, account, "alias", "generateContent", false, []byte(`{"contents":[]}`), false)
			}
			require.Nil(t, result)
			require.ErrorIs(t, err, ErrModelDisabled)
		})
	}
}

func TestDisabledModelSupportedCallsPreserveUsageAndPricing(t *testing.T) {
	prices, key, account := pricingAdmissionFixture()
	prices.cfg = disabledModelsFixture()
	require.NoError(t, prices.RequireOpenAIRequestPricing(context.Background(), key, account, "gpt-6.1-sol", "gpt-6.1-sol", ""))
	for _, model := range []string{"gpt-6.1-sol", "gpt-5.50"} {
		account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"api_key": "local-only"}}
		up := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"id":"resp_local","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":10,"output_tokens":5}}`))}}
		svc := &OpenAIGatewayService{cfg: disabledModelsFixture(), httpUpstream: up}
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
		result, err := svc.Forward(context.Background(), c, account, []byte(`{"model":"`+model+`","input":"local","stream":false}`))
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, 10, result.Usage.InputTokens)
		require.Equal(t, 5, result.Usage.OutputTokens)
		require.Contains(t, string(up.lastBody), model)
	}
}

func TestDisabledModelImageDriverCannotReachUpstream(t *testing.T) {
	for _, mode := range []string{"responses_api_key", "images_oauth"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("SUB2API_IMAGES_MAIN_MODEL", "gpt-5.5")
			up := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"usage":{"input_tokens":1,"output_tokens":2}}`))}}
			svc := &OpenAIGatewayService{cfg: disabledModelsFixture(), httpUpstream: up}
			account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"api_key": "local-only", "base_url": "https://example.com"}, Extra: map[string]any{"use_responses_api": true}}
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			var err error
			if mode == "responses_api_key" {
				c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
				SetOpenAIClientTransport(c, OpenAIClientTransportHTTP)
				_, err = svc.Forward(context.Background(), c, account, []byte(`{"model":"gpt-image-2","input":"draw local","stream":false,"tools":[{"type":"image_generation","model":"gpt-image-2"}]}`))
			} else {
				account.Type = AccountTypeOAuth
				account.Credentials = map[string]any{"access_token": "local-only", "chatgpt_account_id": "local-only"}
				body := []byte(`{"model":"gpt-image-1","prompt":"draw local","size":"1024x1024","quality":"high","n":1}`)
				c.Request = httptest.NewRequest("POST", "/v1/images/generations", strings.NewReader(string(body)))
				c.Request.Header.Set("Content-Type", "application/json")
				parsed, parseErr := svc.ParseOpenAIImagesRequest(c, body)
				require.NoError(t, parseErr)
				_, err = svc.ForwardImages(context.Background(), c, account, body, parsed, "")
			}
			require.ErrorIs(t, err, ErrModelDisabled, "final driver must obey policy, actual forwarded body=%s", up.lastBody)
			require.Nil(t, up.lastReq, "disabled driver must not reach transport")
		})
	}
}
