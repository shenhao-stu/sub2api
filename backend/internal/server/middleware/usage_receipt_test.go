//go:build unit

package middleware

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func TestReceiptQuotaExemptionIsExactReadOnlyPath(t *testing.T) {
	const p = "/v1/sub2api/usage/2c4e888b-836b-4992-9ff1-dc1699488902"
	for _, tc := range []struct {
		method, path string
		want         bool
	}{
		{"GET", p, true}, {"POST", p, false}, {"DELETE", p, false}, {"HEAD", p, false},
		{"POST", "/v1/messages", false}, {"GET", p + "/messages", false},
		{"GET", "/v1/sub2api/usage/", false}, {"GET", "/v1/sub2api/usage/not-a-uuid", false},
		{"GET", "/v1/sub2api/usage/00000000-0000-0000-0000-000000000000", false},
	} {
		if got := isUsageReceiptRequest(tc.method, tc.path); got != tc.want {
			t.Fatalf("%s %s=%v", tc.method, tc.path, got)
		}
	}
}

func TestReceiptAuthCannotBypassGenerationOrDisabledKey(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const path = "/v1/sub2api/usage/2c4e888b-836b-4992-9ff1-dc1699488902"
	for _, tc := range []struct {
		name, method, path, status string
		want                       int
	}{
		{"read_exhausted", "GET", path, service.StatusAPIKeyQuotaExhausted, 200},
		{"generate_exhausted", "POST", "/v1/messages", service.StatusAPIKeyQuotaExhausted, 429},
		{"read_disabled", "GET", path, service.StatusDisabled, 401},
	} {
		t.Run(tc.name, func(t *testing.T) {
			group := &service.Group{ID: 42, Status: service.StatusActive, Hydrated: true, SubscriptionType: service.SubscriptionTypeSubscription}
			user := &service.User{ID: 7, Role: service.RoleUser, Status: service.StatusActive, Balance: 0, Concurrency: 3}
			expired := time.Now().Add(-time.Hour)
			key := &service.APIKey{ID: 100, UserID: 7, Key: "synthetic-receipt-key", Status: tc.status, User: user, GroupID: &group.ID, Group: group, Quota: 1, QuotaUsed: 1, ExpiresAt: &expired}
			touches, subscriptions := 0, 0
			kr := &stubApiKeyRepo{getByKey: func(context.Context, string) (*service.APIKey, error) { clone := *key; return &clone, nil }, updateLastUsed: func(context.Context, int64, time.Time) error { touches++; return nil }}
			sr := &stubUserSubscriptionRepo{getActive: func(context.Context, int64, int64) (*service.UserSubscription, error) {
				subscriptions++
				return nil, service.ErrSubscriptionNotFound
			}}
			cfg := &config.Config{RunMode: config.RunModeStandard}
			ks := service.NewAPIKeyService(kr, nil, nil, nil, nil, nil, cfg)
			ss := service.NewSubscriptionService(nil, sr, nil, nil, cfg)
			t.Cleanup(ss.Stop)
			router := newAuthTestRouter(ks, ss, cfg)
			router.GET("/v1/sub2api/usage/:request_id", func(c *gin.Context) { c.Status(200) })
			w := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			req.Header.Set("x-api-key", key.Key)
			router.ServeHTTP(w, req)
			if tc.name == "generate_exhausted" {
				if w.Code < 400 || w.Code >= 500 {
					t.Fatalf("generation bypassed billing: %d", w.Code)
				}
			} else if w.Code != tc.want {
				t.Fatalf("status=%d want %d", w.Code, tc.want)
			}
			if tc.name == "read_exhausted" && (touches != 0 || subscriptions != 0) {
				t.Fatal("receipt read mutated usage/subscription state")
			}
		})
	}
}
