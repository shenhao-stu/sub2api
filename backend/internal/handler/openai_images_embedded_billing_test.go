//go:build unit

package handler

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// Exercise real forwarding, Ops and billing with two eligible accounts. Only
// the provider transport and persistence are synthetic; no model calls occur.
func TestImagesEmbeddedErrorHandlerBillsDeliveredImagesOnce(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name              string
		stream, delivered bool
	}{
		{"json_error", false, false},
		{"sse_error", true, false},
		{"sse_after_image", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setupOpsErrorLogTestQueue(t, 2)
			accounts := make([]service.Account, 2)
			for i := range accounts {
				accounts[i] = service.Account{ID: int64(19940 + i), Platform: service.PlatformOpenAI,
					Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Priority: i + 1,
					Credentials: map[string]any{"api_key": "synthetic"}}
			}
			accountRepo := &grokPartialUsageAccountRepo{grokStreamFailoverAccountRepo: grokStreamFailoverAccountRepo{openAIWSFailoverHandlerAccountRepoStub: openAIWSFailoverHandlerAccountRepoStub{accounts: accounts}}}
			usageRepo := &openAIWSUsageHandlerUsageLogRepoStub{created: make(chan *service.UsageLog, 3)}
			billingRepo := &monitorCyberBillingRepo{commands: make(chan *service.UsageBillingCommand, 3)}
			userRepo := &monitorCyberUserRepo{}
			cfg := &config.Config{}
			cfg.Default.RateMultiplier = 1
			cfg.Gateway.MaxAccountSwitches = 1
			billingCache := service.NewBillingCacheService(nil, userRepo, nil, nil, nil, nil, cfg, nil)
			t.Cleanup(billingCache.Stop)
			upstream := &grokMediaSlotUpstream{call: func(req *http.Request, _ int64) (*http.Response, error) {
				require.Equal(t, "/v1/images/generations", req.URL.Path)
				body := `{"error":{"type":"server_error","code":"upstream_unavailable","message":"Temporarily unavailable","status_code":503},"status_code":503}`
				contentType := "application/json"
				if tc.stream {
					contentType = "text/event-stream"
					body = "event: error\ndata: " + body + "\n\n"
					if tc.delivered {
						body = "data: {\"type\":\"image_generation.completed\",\"b64_json\":\"aW1hZ2U=\",\"size\":\"1024x1024\"}\n\n" + body
					}
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{contentType}}, Body: io.NopCloser(strings.NewReader(body))}, nil
			}}
			gateway := service.NewOpenAIGatewayService(accountRepo, usageRepo, billingRepo, userRepo, nil, nil, nil, cfg, nil, nil,
				service.NewBillingService(cfg, nil), nil, billingCache, upstream, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil)
			h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(nil), billingCache,
				service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
			ops := service.NewOpsService(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
			router := gin.New()
			router.Use(OpsErrorLoggerMiddleware(ops))
			router.POST("/v1/images/generations", func(c *gin.Context) {
				groupID, imagePrice := int64(14240), 0.04
				c.Set(string(middleware.ContextKeyAPIKey), &service.APIKey{ID: 11840, UserID: 11740, GroupID: &groupID,
					User: &service.User{ID: 11740, Status: service.StatusActive, Balance: 100},
					Group: &service.Group{ID: groupID, Hydrated: true, Platform: service.PlatformOpenAI, Status: service.StatusActive,
						RateMultiplier: 1, AllowImageGeneration: true, ImagePrice1K: &imagePrice, ImagePrice2K: &imagePrice, ImagePrice4K: &imagePrice}})
				c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 11740})
				h.Images(c)
			})
			rec := httptest.NewRecorder()
			body := fmt.Sprintf(`{"model":"gpt-image-2.5-flare","prompt":"synthetic","stream":%t}`, tc.stream)
			req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(rec, req)
			require.Equal(t, 1, upstream.calls, "a terminal response cannot dispatch another image generation")
			require.Contains(t, rec.Body.String(), `"status_code":503`)
			if tc.delivered {
				require.Len(t, billingRepo.commands, 1)
				require.InDelta(t, 0.04, (<-billingRepo.commands).BalanceCost, 1e-12)
				require.Len(t, usageRepo.created, 1)
				usage := <-usageRepo.created
				require.Equal(t, 1, usage.ImageCount)
				require.InDelta(t, 0.04, usage.ActualCost, 1e-12)
			} else {
				require.Empty(t, billingRepo.commands)
				require.Empty(t, usageRepo.created)
			}
			require.Equal(t, int64(1), OpsErrorLogQueueLength())
			entry := (<-opsErrorLogQueue).entry
			require.NotNil(t, entry.UpstreamStatusCode)
			require.Equal(t, 503, *entry.UpstreamStatusCode)
			require.Equal(t, 503, entry.StatusCode)
		})
	}
}
