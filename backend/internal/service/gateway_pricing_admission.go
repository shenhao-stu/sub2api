package service

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/claude"
	"github.com/gin-gonic/gin"
)

// RequireForwardPricing is the common admission boundary for Anthropic,
// Gemini and Antigravity traffic that settles through GatewayService.
func (s *GatewayService) RequireForwardPricing(ctx context.Context, c *gin.Context, account *Account, forwarded string) error {
	key := getAPIKeyFromContext(c)
	if key == nil || strings.TrimSpace(forwarded) == "" {
		return nil // Internal diagnostics have no customer billing identity.
	}
	if err := s.requireRequestPricing(ctx, key, account, forwarded); err != nil {
		MarkOpsClientBusinessLimited(c, OpsClientBusinessLimitedReasonLocalModelConfiguration)
		writeRequestAdmissionError(c, http.StatusServiceUnavailable, "api_error", "model_pricing_unavailable", "Model pricing is unavailable; contact the administrator")
		return err
	}
	return nil
}

func (s *GatewayService) requireRequestPricing(ctx context.Context, key *APIKey, account *Account, forwarded string) error {
	if s == nil || s.billingService == nil || key == nil {
		return fmt.Errorf("%w: billing context unavailable", ErrModelPricingUnavailable)
	}
	model := forwarded
	if account != nil {
		switch {
		case account.Platform == PlatformAntigravity:
			model = mapAntigravityModel(account, forwarded)
		case account.Platform == PlatformAnthropic && account.Type != AccountTypeAPIKey && account.Type != AccountTypeServiceAccount && !account.IsBedrock():
			// OAuth ignores arbitrary account mappings and uses native aliases.
			model = claude.NormalizeModelID(forwarded)
		default:
			model = account.GetMappedModel(forwarded)
		}
	}
	requested := forwarded
	if public, ok := RequestedPublicModelFromContext(ctx); ok {
		requested = public
	}
	model = s.billableModelWithFallback(ctx, key, model, forwarded, requested)
	result := &ForwardResult{Model: forwarded, UpstreamModel: model}
	if isImageGenerationModel(model) || isOpenAIImageGenerationModel(model) {
		result.ImageCount, result.ImageSize = 1, "2K"
	}
	_, err := s.calculateRecordUsageCost(ctx, result, key, model, 1, 1, time.Now())
	if err != nil {
		return fmt.Errorf("%w: %w", ErrModelPricingUnavailable, err)
	}
	return nil
}
