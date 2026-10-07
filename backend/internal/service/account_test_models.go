package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai_compat"
)

const commandCodeModelCatalogLimit = 2 << 20

// FetchCommandCodeAccountModels reads the public protocol catalog, not account
// entitlement. A failed discovery never fabricates a different provider's models.
func (s *AccountTestService) FetchCommandCodeAccountModels(ctx context.Context, account *Account) ([]openai.Model, error) {
	body, err := s.fetchCommandCodeModelCatalog(ctx, account)
	if err != nil {
		return nil, err
	}
	projected, err := projectAccountModelsBody(body, account, nil, false)
	if err != nil {
		return nil, fmt.Errorf("project Command Code account models: %w", err)
	}
	var result struct {
		Data []openai.Model `json:"data"`
	}
	if err := json.Unmarshal(projected, &result); err != nil {
		return nil, err
	}
	for i := range result.Data {
		model := &result.Data[i]
		model.Object, model.Type, model.OwnedBy = "model", "model", CommandCodeProvider
		if strings.TrimSpace(model.DisplayName) == "" {
			model.DisplayName = model.ID
		}
	}
	sort.Slice(result.Data, func(i, j int) bool { return result.Data[i].ID < result.Data[j].ID })
	return result.Data, nil
}

func validateCommandCodeCatalogAccount(account *Account) error {
	if !account.IsCommandCode() || account.Type != AccountTypeAPIKey ||
		(account.Platform != PlatformOpenAI && account.Platform != PlatformAnthropic) {
		return newUpstreamModelSyncConfigError("Command Code model discovery requires a compatible API key account", nil)
	}
	expected := CommandCodeBaseURL
	if account.IsCommandCodeGo() {
		expected = CommandCodeGoBaseURL
		if account.Platform != PlatformOpenAI {
			return newUpstreamModelSyncConfigError("Command Code Go requires the OpenAI protocol", nil)
		}
	}
	if strings.TrimRight(strings.TrimSpace(account.GetCredential("base_url")), "/") != expected {
		return newUpstreamModelSyncConfigError("Command Code requires its fixed official base URL", nil)
	}
	return nil
}

func (s *AccountTestService) fetchCommandCodeModelCatalog(ctx context.Context, account *Account) ([]byte, error) {
	if s == nil || s.httpUpstream == nil {
		return nil, newUpstreamModelSyncConfigError("Command Code model discovery is unavailable", nil)
	}
	if err := validateCommandCodeCatalogAccount(account); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	ctx = WithHTTPUpstreamPublicHostsOnly(WithHTTPUpstreamRedirectsDisabled(ctx))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, CommandCodeBaseURL+"/v1/models", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	// No API key, account header overrides or cookies belong on this public request.
	resp, err := s.commandCodeAccountTestUpstream(req, account)
	if err != nil {
		return nil, newUpstreamModelSyncUpstreamError("Command Code model catalog request failed", nil)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, newUpstreamModelSyncUpstreamError(fmt.Sprintf("Command Code model catalog returned HTTP %d", resp.StatusCode), nil)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, commandCodeModelCatalogLimit+1))
	if err != nil || len(body) > commandCodeModelCatalogLimit {
		return nil, newUpstreamModelSyncUpstreamError("Command Code model catalog could not be read within its size limit", nil)
	}
	body, err = filterCommandCodeModelCatalog(body, account)
	if err != nil {
		return nil, newUpstreamModelSyncUpstreamError(err.Error(), nil)
	}
	return body, nil
}

func commandCodeAccountEndpoint(account *Account) string {
	if account.Platform == PlatformAnthropic {
		return "/messages"
	}
	if !account.IsCommandCodeGo() && openai_compat.ShouldUseResponsesAPI(account.Extra) {
		return "/responses"
	}
	return "/chat/completions"
}

func filterCommandCodeModelCatalog(body []byte, account *Account) ([]byte, error) {
	var catalog struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &catalog); err != nil {
		return nil, errors.New("command code model catalog is invalid JSON")
	}
	models := make([]map[string]json.RawMessage, 0, len(catalog.Data))
	seen := make(map[string]bool, len(catalog.Data))
	endpoint := commandCodeAccountEndpoint(account)
	for _, raw := range catalog.Data {
		var model struct {
			ID                 string   `json:"id"`
			Name               string   `json:"name"`
			DisplayName        string   `json:"display_name"`
			ContextLength      int64    `json:"context_length"`
			ContextWindow      int64    `json:"context_window"`
			SupportedEndpoints []string `json:"supported_endpoints"`
		}
		if json.Unmarshal(raw, &model) != nil || model.ID == "" || strings.ContainsAny(model.ID, " \t\r\n") {
			return nil, errors.New("command code model catalog contains an invalid model")
		}
		compatible := false
		for _, supported := range model.SupportedEndpoints {
			supported = strings.TrimPrefix(strings.TrimPrefix(supported, "/provider"), "/v1")
			if account.IsCommandCodeGo() {
				compatible = compatible || supported == "/chat/completions" || supported == "/responses" || supported == "/messages"
			} else {
				compatible = compatible || supported == endpoint
			}
		}
		if !compatible || seen[model.ID] {
			continue
		}
		seen[model.ID] = true
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return nil, err
		}
		if model.DisplayName == "" && model.Name != "" {
			fields["display_name"], _ = json.Marshal(model.Name)
		}
		if model.ContextWindow == 0 && model.ContextLength > 0 {
			fields["context_window"], _ = json.Marshal(model.ContextLength)
		}
		models = append(models, fields)
	}
	if len(models) == 0 {
		return nil, errors.New("command code catalog has no models for the configured protocol")
	}
	return json.Marshal(map[string]any{"data": models})
}
