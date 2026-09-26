package service

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
)

func pricingAdmissionFixture() (*OpenAIGatewayService, *APIKey, *Account) {
	billing := NewBillingService(&config.Config{}, nil)
	s := &OpenAIGatewayService{billingService: billing, resolver: NewModelPricingResolver(nil, billing)}
	gid := int64(8)
	return s, &APIKey{ID: 7, GroupID: &gid, Group: &Group{ID: gid, RateMultiplier: 1}}, &Account{ID: 9, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
}

func TestOpenAIRequestPricingAdmission(t *testing.T) {
	for _, tc := range []struct {
		name, model, mapped    string
		custom, free, rejected bool
	}{
		{"known", "gpt-5.1", "", false, false, false},
		{"mapped_alias", "local-alias", "gpt-5.1", false, false, false},
		{"configured_custom", "local-custom", "", true, false, false},
		{"configured_free", "local-free", "", true, true, false},
		{"missing", "zz-no-price-test", "", false, false, true},
		{"native_media", "grok-imagine-image", "", false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, key, account := pricingAdmissionFixture()
			if tc.custom {
				price := 0.000001
				if tc.free {
					price = 0
				}
				key.Group.ModelPricing = []ChannelModelPricing{{Models: []string{tc.model}, BillingMode: BillingModeToken, InputPrice: &price, OutputPrice: &price}}
			}
			if tc.mapped != "" {
				account.Credentials = map[string]any{"model_mapping": map[string]any{tc.model: tc.mapped}}
			}
			err := s.RequireOpenAIRequestPricing(context.Background(), key, account, tc.model, tc.model, "")
			if (err != nil) != tc.rejected {
				t.Fatalf("error=%v want rejected=%v", err, tc.rejected)
			}
		})
	}
}

func TestUnknownPriceCannotReachForwardAdapters(t *testing.T) {
	for _, endpoint := range []string{"/v1/responses", "/v1/chat/completions", "/v1/messages", "/v1/embeddings"} {
		t.Run(endpoint, func(t *testing.T) {
			s, key, account := pricingAdmissionFixture()
			// No HTTP dependency is installed: reaching transport is a test failure.
			body := []byte(`{"model":"zz-no-price-test","input":"local","messages":[],"tools":[{"type":"image_generation"}]}`)
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(string(body)))
			c.Set("api_key", key)
			var result *OpenAIForwardResult
			var err error
			switch endpoint {
			case "/v1/responses":
				result, err = s.Forward(context.Background(), c, account, body)
			case "/v1/chat/completions":
				result, err = s.ForwardAsChatCompletions(context.Background(), c, account, body, "", "")
			case "/v1/messages":
				result, err = s.ForwardAsAnthropic(context.Background(), c, account, body, "", "")
			case "/v1/embeddings":
				result, err = s.ForwardEmbeddings(context.Background(), c, account, body, "")
			}
			if result != nil || !errors.Is(err, ErrModelPricingUnavailable) || rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "model_pricing_unavailable") {
				t.Fatalf("unpriced request was not rejected: result=%v error=%v status=%d", result, err, rec.Code)
			}
		})
	}
}

func TestOpenAIRequestPricingAdmissionUsesApplicableMappings(t *testing.T) {
	s, key, account := pricingAdmissionFixture()
	account.Credentials = map[string]any{"model_mapping": map[string]any{"unpriced-alias": "gpt-5.1"}}
	account.Extra = map[string]any{"openai_passthrough": true}
	if err := s.RequireOpenAIRequestPricing(context.Background(), key, account, "unpriced-alias", "unpriced-alias", ""); !errors.Is(err, ErrModelPricingUnavailable) {
		t.Fatalf("passthrough admitted a price from an unused mapping: %v", err)
	}
	account.Extra = nil
	account.Platform = PlatformKimi
	account.Credentials = nil
	if err := s.RequireOpenAIRequestPricing(context.Background(), key, account, "claude-sonnet-4-5", "claude-sonnet-4-5", ""); !errors.Is(err, ErrModelPricingUnavailable) {
		t.Fatalf("CN request used an unrelated Claude price: %v", err)
	}
	price := 0.000001
	key.Group.ModelPricing = []ChannelModelPricing{{Models: []string{"claude-sonnet-4-5"}, BillingMode: BillingModeToken, InputPrice: &price, OutputPrice: &price}}
	if err := s.RequireOpenAIRequestPricing(context.Background(), key, account, "claude-sonnet-4-5", "claude-sonnet-4-5", ""); err != nil {
		t.Fatalf("explicit CN alias pricing rejected: %v", err)
	}
}

func TestPricingAdmissionAfterCommittedSSE(t *testing.T) {
	for _, endpoint := range []string{"/v1/responses", "/v1/chat/completions", "/v1/messages"} {
		t.Run(endpoint, func(t *testing.T) {
			s, key, account := pricingAdmissionFixture()
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, endpoint, nil)
			c.Set("api_key", key)
			c.Writer.Header().Set("Content-Type", "text/event-stream")
			_, _ = c.Writer.Write([]byte(": keepalive\n\n"))
			err := s.requireOpenAIForwardPricing(context.Background(), c, account, []byte(`{"model":"zz-no-price-test"}`), "")
			if !errors.Is(err, ErrModelPricingUnavailable) || !strings.Contains(rec.Body.String(), "model_pricing_unavailable") || !strings.Contains(rec.Body.String(), "data: ") {
				t.Fatalf("committed stream lost pricing rejection: %v", err)
			}
			if rec.Code != http.StatusOK || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/event-stream") {
				t.Fatal("committed SSE transport was overwritten")
			}
		})
	}
}
