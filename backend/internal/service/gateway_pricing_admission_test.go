package service

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestGatewayPricingAdmissionAcrossProviders(t *testing.T) {
	for _, platform := range []string{PlatformAnthropic, PlatformGemini, PlatformAntigravity} {
		t.Run(platform, func(t *testing.T) {
			openai, key, account := pricingAdmissionFixture()
			s := &GatewayService{billingService: openai.billingService, resolver: openai.resolver}
			account.Platform = platform
			if err := s.requireRequestPricing(context.Background(), key, account, "zz-no-price-test"); !errors.Is(err, ErrModelPricingUnavailable) {
				t.Fatalf("missing price accepted: %v", err)
			}
			zero := 0.0
			key.Group.ModelPricing = []ChannelModelPricing{{Models: []string{"local-free"}, BillingMode: BillingModeToken, InputPrice: &zero, OutputPrice: &zero}}
			if err := s.requireRequestPricing(context.Background(), key, account, "local-free"); err != nil {
				t.Fatalf("configured free price rejected: %v", err)
			}
			account.Credentials = map[string]any{"model_mapping": map[string]any{"local-alias": "claude-sonnet-4-5"}}
			if err := s.requireRequestPricing(context.Background(), key, account, "local-alias"); err != nil {
				t.Fatalf("mapped known price rejected: %v", err)
			}
			if platform == PlatformAnthropic {
				account.Type = AccountTypeOAuth
				if err := s.requireRequestPricing(context.Background(), key, account, "local-alias"); !errors.Is(err, ErrModelPricingUnavailable) {
					t.Fatalf("OAuth admitted an unused account mapping: %v", err)
				}
			}
		})
	}
}

func TestUnknownPriceCannotReachGatewayAdapters(t *testing.T) {
	for _, endpoint := range []string{"/v1/messages", "/v1/chat/completions", "/v1/responses"} {
		t.Run(endpoint, func(t *testing.T) {
			openai, key, account := pricingAdmissionFixture()
			account.Platform = PlatformAnthropic
			s := &GatewayService{billingService: openai.billingService, resolver: openai.resolver}
			body := []byte(`{"model":"zz-no-price-test","input":"local","messages":[]}`)
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(string(body)))
			c.Set("api_key", key)
			var err error
			switch endpoint {
			case "/v1/messages":
				_, err = s.Forward(context.Background(), c, account, &ParsedRequest{Model: "zz-no-price-test"})
			case "/v1/chat/completions":
				_, err = s.ForwardAsChatCompletions(context.Background(), c, account, body, nil)
			case "/v1/responses":
				_, err = s.ForwardAsResponses(context.Background(), c, account, body, nil)
			}
			if !errors.Is(err, ErrModelPricingUnavailable) || rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("unpriced model reached an adapter: error=%v status=%d", err, rec.Code)
			}
		})
	}
}

func TestGatewayMissingPriceDoesNotConsumeBillingIdentity(t *testing.T) {
	for _, platform := range []string{PlatformAnthropic, PlatformGemini, PlatformAntigravity} {
		t.Run(platform, func(t *testing.T) {
			logs := &openAIRecordUsageLogRepoStub{inserted: true}
			users := &openAIRecordUsageUserRepoStub{}
			billing := &openAIRecordUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: true}}
			openai, _, _ := pricingAdmissionFixture()
			s := &GatewayService{billingService: openai.billingService, resolver: openai.resolver, usageLogRepo: logs, userRepo: users, usageBillingRepo: billing}
			err := s.RecordUsage(context.Background(), &RecordUsageInput{
				Result: &ForwardResult{RequestID: "unpriced-local", Model: "zz-no-price-test", Usage: ClaudeUsage{InputTokens: 100, OutputTokens: 10}},
				APIKey: &APIKey{ID: 7}, User: &User{ID: 8}, Account: &Account{ID: 9, Platform: platform},
			})
			if !errors.Is(err, ErrModelPricingUnavailable) || billing.calls != 0 || users.deductCalls != 0 || logs.calls != 0 {
				t.Fatalf("pricing error consumed a billing identity or wrote a free row: error=%v billing=%d logs=%d", err, billing.calls, logs.calls)
			}
		})
	}
}
