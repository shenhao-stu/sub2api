package handler

import (
	"encoding/json"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/requestmodel"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"net/http/httptest"
	"testing"
)

func TestDisabledModelWSAllCandidateForms(t *testing.T) {
	cfg := &config.Config{Gateway: config.GatewayConfig{DisabledModels: []string{"gpt-5.5", "gpt-5.5-*"}}}
	for _, body := range []string{`{"model":"gpt-5.5"}`, `{"model":"gpt-6.1-sol","Model":"gpt-5.5"}`, `{"model":"gpt-5.5","model":"gpt-6.1-sol"}`, `{"type":"session.update","session":{"model":"gpt-5.5"}}`} {
		for _, group := range []*service.Group{nil, {}, {ModelAllowlist: service.GroupModelAllowlist{Enabled: true, Models: []string{"*"}}}} {
			require.NotEmpty(t, blockedModelAllowlistCandidate(group, requestmodel.FromBodyCandidates("", "application/json", []byte(body)), cfg))
		}
	}
	require.Equal(t, "gpt-5.5", blockedModelAllowlistCandidate(nil, []string{"gpt-5.5"}, cfg), "inherited turn model must be checked")
}

func TestDisabledModelFallbackAndSingleModelLists(t *testing.T) {
	cfg := &config.Config{Gateway: config.GatewayConfig{DisabledModels: []string{"gpt-5.5", "gpt-5.5-*"}}}
	for _, single := range []bool{false, true} {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest("GET", "/v1/models", nil)
		if single {
			c.Params = gin.Params{{Key: "model", Value: "gpt-5.5"}}
		}
		writeModelsListResponse(c, []map[string]any{{"id": "gpt-5.5"}, {"id": "gpt-6.1-sol", "custom": true}}, cfg)
		if single {
			require.Equal(t, 404, rec.Code)
		} else {
			require.JSONEq(t, `{"object":"list","data":[{"id":"gpt-6.1-sol","custom":true}]}`, rec.Body.String())
		}
	}
	raw := []byte(`{"models":[{"name":"models/gpt-5.5"},{"name":"models/gemini-3.8-flash","metadata":123}]}`)
	got, changed, ok := filterUpstreamGeminiModelsBody(raw, service.GroupModelAllowlist{}, cfg)
	require.True(t, ok)
	require.True(t, changed)
	require.JSONEq(t, `{"models":[{"name":"models/gemini-3.8-flash","metadata":123}]}`, string(got))
	var envelope map[string]any
	require.NoError(t, json.Unmarshal(got, &envelope))
}
