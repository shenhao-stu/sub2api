package service

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// RequireOpenAIRequestPricing uses the settlement resolver before generated work
// can start. A configured zero price is valid; a missing price is not free use.
func (s *OpenAIGatewayService) RequireOpenAIRequestPricing(ctx context.Context, apiKey *APIKey, account *Account, requested, forwarded, dispatched string) error {
	return s.requireOpenAIRequestPricing(ctx, apiKey, account, requested, forwarded, dispatched, false)
}

func (s *OpenAIGatewayService) requireOpenAIRequestPricing(ctx context.Context, apiKey *APIKey, account *Account, requested, forwarded, dispatched string, compact bool) error {
	if s == nil || s.billingService == nil || apiKey == nil {
		return fmt.Errorf("%w: billing context unavailable", ErrModelPricingUnavailable)
	}
	if strings.TrimSpace(forwarded) == "" {
		forwarded = requested
	}
	model := resolveOpenAIForwardModel(account, forwarded, dispatched)
	upstreamModel := model
	if account != nil && account.IsOpenAIPassthroughEnabled() || compact {
		// Passthrough deliberately skips ordinary account mappings. Compact
		// mapping affects the upstream model but preserves the billing model.
		model, upstreamModel = resolveOpenAIForwardMappedModels(account, forwarded, compact)
	}
	models := usageBillingModelCandidates(model, forwarded, requested, upstreamModel)
	models = s.filterCNProviderBillingModelCandidates(ctx, account, apiKey, models)
	result := &OpenAIForwardResult{Model: forwarded, UpstreamModel: model}
	// Only native image model families use media pricing here. Merely adding an
	// image tool to an unknown text model must not skip its token-price gate.
	if isOpenAIImageGenerationModel(model) {
		result.ImageCount, result.ImageSize = 1, "2K"
	}
	_, err := s.calculateOpenAIRecordUsageCost(ctx, result, apiKey, models, 1, 1, 1, 1,
		UsageTokens{InputTokens: 1, OutputTokens: 1}, "", openAILongContextBillingGate(account), time.Now())
	if err != nil {
		return fmt.Errorf("%w: %w", ErrModelPricingUnavailable, err)
	}
	return nil
}

func (s *OpenAIGatewayService) requireOpenAIForwardPricing(ctx context.Context, c *gin.Context, account *Account, body []byte, dispatched string) error {
	key := getAPIKeyFromContext(c)
	if key == nil {
		// Internal adapters and account diagnostics have no customer billing
		// identity. Public generation routes always install one in auth middleware.
		return nil
	}
	forwarded := strings.TrimSpace(gjson.GetBytes(body, "model").String())
	if forwarded == "" {
		return nil // Existing protocol validation rejects missing models.
	}
	requested := forwarded
	if public, ok := RequestedPublicModelFromContext(ctx); ok {
		requested = public
	}
	if err := s.requireOpenAIRequestPricing(ctx, key, account, requested, forwarded, dispatched, isOpenAIResponsesCompactPath(c)); err != nil {
		MarkOpsClientBusinessLimited(c, OpsClientBusinessLimitedReasonLocalModelConfiguration)
		writeRequestAdmissionError(c, http.StatusServiceUnavailable, "api_error", "model_pricing_unavailable", "Model pricing is unavailable; contact the administrator")
		return err
	}
	return nil
}
