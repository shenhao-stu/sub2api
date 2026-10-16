package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/Wei-Shaw/sub2api/internal/pkg/claude"
	"github.com/Wei-Shaw/sub2api/internal/pkg/requestmodel"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

var ErrModelDisabled = errors.New("model is disabled by gateway policy")

// DisabledModel checks the supplied immutable startup policy. Use both the
// public model and the actual resolved target; an alias cannot undo a denial.
func DisabledModel(cfg *config.Config, models ...string) string {
	if cfg == nil || len(cfg.Gateway.DisabledModels) == 0 {
		return ""
	}
	for _, model := range models {
		normalized := normalizeClaudeCodeLongContextModel(strings.ToLower(strings.TrimSpace(model)))
		candidates := append(groupModelAllowlistCandidates(normalized), canonicalizeOpenAIModelAliasSpelling(normalized), model)
		for _, pattern := range cfg.Gateway.DisabledModels {
			pattern = strings.TrimSpace(pattern)
			if pattern == "" {
				continue
			}
			for _, candidate := range candidates {
				if candidate != "" && groupAllowlistPatternMatches(pattern, candidate) {
					return model
				}
			}
		}
	}
	return ""
}

func RequireEnabledModels(cfg *config.Config, models ...string) error {
	if blocked := DisabledModel(cfg, models...); blocked != "" {
		return fmt.Errorf("%w: %s", ErrModelDisabled, blocked)
	}
	return nil
}

func FilterEnabledModels(cfg *config.Config, models []string) []string {
	if cfg == nil || len(cfg.Gateway.DisabledModels) == 0 {
		return models
	}
	filtered := make([]string, 0, len(models))
	for _, model := range models {
		if DisabledModel(cfg, model) == "" {
			filtered = append(filtered, model)
		}
	}
	return filtered
}

func rejectDisabledModels(c *gin.Context, cfg *config.Config, models ...string) error {
	err := RequireEnabledModels(cfg, models...)
	if err == nil || c == nil {
		return err
	}
	MarkOpsClientBusinessLimited(c, OpsClientBusinessLimitedReasonLocalModelConfiguration)
	if c.Request != nil && strings.Contains(c.Request.URL.Path, "/v1beta/") && !c.Writer.Written() {
		c.JSON(http.StatusNotFound, gin.H{"error": gin.H{"code": http.StatusNotFound, "status": "NOT_FOUND", "message": "Model is not available"}})
	} else {
		writeRequestAdmissionError(c, http.StatusNotFound, "invalid_request_error", "model_not_available", "Model is not available")
	}
	return err
}

func gatewayForwardPolicyModel(account *Account, model string) string {
	if account == nil {
		return model
	}
	switch {
	case account.Platform == PlatformAntigravity:
		return mapAntigravityModel(account, model)
	case account.IsBedrock():
		if resolved, ok := ResolveBedrockModelID(account, model); ok {
			return resolved
		}
	case account.Platform == PlatformAnthropic && account.Type != AccountTypeAPIKey && account.Type != AccountTypeServiceAccount:
		return claude.NormalizeModelID(model)
	}
	return account.GetMappedModel(model)
}

func modelDisabledForAccounts(cfg *config.Config, accounts []Account, model string) bool {
	if cfg == nil || len(cfg.Gateway.DisabledModels) == 0 {
		return false
	}
	if DisabledModel(cfg, model) != "" {
		return true
	}
	blocked := false
	for i := range accounts {
		account := &accounts[i]
		// Use mapping membership here: OAuth retirement can make IsModelSupported
		// false precisely because the mapped target is disabled and must be hidden.
		if !account.isModelMappingSupported(model) {
			continue
		}
		target := gatewayForwardPolicyModel(account, model)
		if account.IsOpenAI() {
			target = resolveOpenAIAccountUpstreamModelForRequest(account, model, false)
		}
		if DisabledModel(cfg, target) != "" {
			blocked = true
		} else {
			return false
		}
	}
	return blocked
}

func firstPolicyConfig(configs []*config.Config) *config.Config {
	if len(configs) > 0 {
		return configs[0]
	}
	return nil
}

// FilterDisabledModelsBody operates on a projected public catalogue and leaves
// cached upstream bytes alone. Callers compute ETags after this projection.
func FilterDisabledModelsBody(cfg *config.Config, body []byte, accounts []Account, codex bool) ([]byte, error) {
	if cfg == nil || len(cfg.Gateway.DisabledModels) == 0 {
		return body, nil
	}
	filtered, _, err := filterRetiredOpenAIPublicModelsBody(body, accounts, codex, cfg)
	return filtered, err
}

// FilterAvailableModels applies the startup policy to admin test candidates,
// including fallback lists, without altering the stored pricing catalogue.
func (s *AccountTestService) FilterAvailableModels(account *Account, models any) (any, error) {
	if s == nil || s.cfg == nil || len(s.cfg.Gateway.DisabledModels) == 0 {
		return models, nil
	}
	body, err := json.Marshal(map[string]any{"data": models})
	if err != nil {
		return nil, err
	}
	var accounts []Account
	if account != nil {
		accounts = []Account{*account}
	}
	body, err = FilterDisabledModelsBody(s.cfg, body, accounts, false)
	if err != nil {
		return nil, err
	}
	var result struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	return result.Data, nil
}

func (s *AntigravityGatewayService) disabledModelsConfig() *config.Config {
	if s != nil && s.settingService != nil {
		return s.settingService.cfg
	}
	return nil
}

// FilterDisabledModelIDs also checks configured channel and composite targets.
// It is used only for discovery; generation checks the actual selected target.
func (s *GatewayService) FilterDisabledModelIDs(ctx context.Context, groupID *int64, models []string, endpoints ...string) []string {
	if s == nil || s.cfg == nil || len(s.cfg.Gateway.DisabledModels) == 0 || models == nil {
		return models
	}
	var routes []CompositeModelRoute
	if groupID != nil && s.compositeResolver != nil && s.compositeResolver.repo != nil {
		var err error
		routes, err = s.compositeResolver.repo.ListByGroup(ctx, *groupID, false)
		if err != nil {
			return []string{}
		}
	}
	endpoint := ""
	if len(endpoints) > 0 {
		endpoint = endpoints[0]
	}
	var accounts []Account
	if s.accountRepo != nil {
		var err error
		if groupID != nil {
			accounts, err = s.accountRepo.ListModelAvailabilityCandidates(ctx, groupID, domain.ConcretePlatformIDs(), false)
			if err != nil {
				accounts, err = s.accountRepo.ListSchedulableByGroupID(ctx, *groupID)
			}
		} else {
			accounts, err = s.accountRepo.ListSchedulable(ctx)
		}
		if err != nil {
			return []string{}
		}
	}
	filtered := make([]string, 0, len(models))
	for _, model := range models {
		if DisabledModel(s.cfg, model) != "" {
			continue
		}
		for _, route := range disabledPolicyCatalogRoutes(routes, model, endpoint) {
			target := route.UpstreamModel
			if DisabledModel(s.cfg, target) != "" {
				continue
			}
			if groupID != nil && s.channelService != nil {
				target = s.channelService.ResolveChannelMapping(ctx, *groupID, target).MappedModel
			}
			eligible := accounts
			if route.TargetPlatform != "" {
				eligible = nil
				for _, account := range accounts {
					if account.Platform == route.TargetPlatform || mixedListingAccountAllowed(route.TargetPlatform, &account) {
						eligible = append(eligible, account)
					}
				}
			}
			if !modelDisabledForAccounts(s.cfg, eligible, target) {
				filtered = append(filtered, model)
				break
			}
		}
	}
	return filtered
}

// A general catalogue can advertise an alias if any configured endpoint serves
// it. A protocol-specific catalogue uses exactly that endpoint's winning route.
func disabledPolicyCatalogRoutes(routes []CompositeModelRoute, model, endpoint string) []CompositeModelRoute {
	enabled := make([]CompositeModelRoute, 0, len(routes))
	for _, route := range routes {
		if route.Enabled {
			enabled = append(enabled, route)
		}
	}
	routes = enabled
	endpoints := []string{endpoint}
	if endpoint == "" {
		endpoints = nil
		seen := make(map[string]bool)
		for _, route := range routes {
			ep := normalizeCompositeRouteEndpoint(route.Endpoint)
			if !seen[ep] {
				endpoints = append(endpoints, ep)
				seen[ep] = true
			}
		}
	}
	targets := make([]CompositeModelRoute, 0, len(endpoints))
	for _, ep := range endpoints {
		if route, ok := matchCompositeRoute(routes, model, normalizeCompositeRouteEndpoint(ep)); ok {
			route.UpstreamModel = strings.TrimSpace(route.UpstreamModel)
			if route.UpstreamModel == "" {
				route.UpstreamModel = model
			}
			targets = append(targets, route)
		}
	}
	if len(targets) == 0 {
		return []CompositeModelRoute{{UpstreamModel: model}}
	}
	return targets
}

// filterDisabledChannelModelsBody evaluates public -> channel -> account in
// routing order, after alias projection and before computing the final ETag.
func (s *OpenAIGatewayService) filterDisabledChannelModelsBody(ctx context.Context, group *Group, accounts []Account, body []byte, codex bool) ([]byte, error) {
	if s == nil || s.cfg == nil || len(s.cfg.Gateway.DisabledModels) == 0 {
		return body, nil
	}
	field, idField := "data", "id"
	if codex {
		field, idField = "models", "slug"
	}
	envelope, entries, err := modelCatalogEntries(body, field)
	if err != nil {
		return nil, err
	}
	filtered := make([]json.RawMessage, 0, len(entries))
	for _, raw := range entries {
		model := gjson.GetBytes(raw, idField).String()
		if DisabledModel(s.cfg, model) != "" {
			continue
		}
		target := model
		if group != nil && s.channelService != nil {
			target = s.channelService.ResolveChannelMapping(ctx, group.ID, model).MappedModel
		}
		if !modelDisabledForAccounts(s.cfg, accounts, target) {
			filtered = append(filtered, raw)
		}
	}
	if len(filtered) == len(entries) {
		return body, nil
	}
	envelope[field], err = json.Marshal(filtered)
	if err != nil {
		return nil, err
	}
	return json.Marshal(envelope)
}

// Wire models include the Responses driver and independently selected native
// image tool. Tool function arguments and user content are not model selectors.
func RequireEnabledWireModels(cfg *config.Config, body []byte) error {
	if cfg == nil || len(cfg.Gateway.DisabledModels) == 0 {
		return nil
	}
	models := requestmodel.FromBodyCandidates("", "application/json", body)
	gjson.ParseBytes(body).ForEach(func(field, value gjson.Result) bool {
		if !strings.EqualFold(field.String(), "tools") {
			return true
		}
		value.ForEach(func(_, tool gjson.Result) bool {
			imageTool := false
			var toolModels []string
			tool.ForEach(func(key, value gjson.Result) bool {
				if strings.EqualFold(key.String(), "type") && value.String() == "image_generation" {
					imageTool = true
				}
				if strings.EqualFold(key.String(), "model") && value.Type == gjson.String {
					toolModels = append(toolModels, value.String())
				}
				return true
			})
			if imageTool {
				models = append(models, toolModels...)
			}
			return true
		})
		return true
	})
	return RequireEnabledModels(cfg, models...)
}

func rejectDisabledWireModels(c *gin.Context, cfg *config.Config, body []byte) error {
	if err := RequireEnabledWireModels(cfg, body); err != nil {
		if c == nil {
			return err
		}
		// Reuse the protocol-specific local rejection; never turn this into failover.
		MarkOpsClientBusinessLimited(c, OpsClientBusinessLimitedReasonLocalModelConfiguration)
		writeRequestAdmissionError(c, http.StatusNotFound, "invalid_request_error", "model_not_available", "Model is not available")
		return err
	}
	return nil
}
