package routes

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

type receiptRouteRepo struct {
	service.UsageLogRepository
	key int64
}

func (r *receiptRouteRepo) GetUsageReceipt(_ context.Context, key int64, id string) (*service.UsageReceipt, error) {
	r.key = key
	if id != "2c4e888b-836b-4992-9ff1-dc1699488902" {
		return nil, service.ErrUsageLogNotFound
	}
	return &service.UsageReceipt{Model: "model", OutputTokens: 64000, RecordedAt: time.Now()}, nil
}

func TestReceiptActualRouteAuthenticatesBeforeReadingOwnedMeter(t *testing.T) {
	for _, authorized := range []bool{false, true} {
		cfg := &config.Config{RunMode: config.RunModeStandard}
		group := &service.Group{ID: 42, Status: service.StatusActive, Hydrated: true, Platform: service.PlatformGemini, SubscriptionType: service.SubscriptionTypeStandard}
		key := &service.APIKey{ID: 100, Key: "synthetic-receipt-route", Status: service.StatusAPIKeyQuotaExhausted, UserID: 7, User: &service.User{ID: 7, Role: service.RoleUser, Status: service.StatusActive}, GroupID: &group.ID, Group: group, Quota: 1, QuotaUsed: 1}
		ks := service.NewAPIKeyService(&keyBillingRouteAPIKeyRepo{apiKey: key}, nil, nil, nil, &keyBillingRouteRateRepo{}, nil, cfg)
		repo := &receiptRouteRepo{}
		usage := service.NewUsageService(repo, nil, nil, nil)
		h := handler.NewGatewayHandler(nil, nil, nil, nil, nil, nil, nil, usage, ks, nil, nil, nil, nil, cfg, nil)
		r := gin.New()
		RegisterGatewayRoutes(r, &handler.Handlers{Gateway: h, OpenAIGateway: &handler.OpenAIGatewayHandler{}}, middleware.NewAPIKeyAuthMiddleware(ks, nil, cfg), ks, nil, nil, nil, nil, cfg)
		req := httptest.NewRequest("GET", "/v1/sub2api/usage/2c4e888b-836b-4992-9ff1-dc1699488902?api_key_id=999", nil)
		if authorized {
			req.Header.Set("x-api-key", key.Key)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if authorized && (w.Code != 200 || repo.key != 100) {
			t.Fatalf("owned receipt route failed: %d owner=%d", w.Code, repo.key)
		}
		if !authorized && (w.Code != 401 || repo.key != 0) {
			t.Fatalf("unauthenticated storage access: %d owner=%d", w.Code, repo.key)
		}
	}
}
