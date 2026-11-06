//go:build unit

package handler

import (
	"context"
	"github.com/Wei-Shaw/sub2api/internal/config"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPersonalGrokImagePoolFence(t *testing.T) {
	policy := "410/700/grok-imagine-image-2.0"
	model := "grok-imagine-image-2.0"
	group := service.Group{ID: 410, Platform: service.PlatformOpenAI}
	req := service.OpenAIImagesRequest{Model: model, N: 1, ResponseFormat: "b64_json"}
	body := []byte(`{"model":"grok-imagine-image-2.0","prompt":"fixture","response_format":"b64_json"}`)
	for _, tc := range []struct {
		label, policy, model, mapped, body string
		group                              service.Group
		req                                service.OpenAIImagesRequest
		want                               bool
	}{
		{"exact", policy, model, "", string(body), group, req, true}, {"disabled", "", model, "", string(body), group, req, false}, {"self", "410/410/" + model, model, "", string(body), group, req, false}, {"different group", "28/700/" + model, model, "", string(body), group, req, false}, {"other sku", policy, "grok-imagine-image", "", string(body), group, req, false}, {"provider remap", policy, model, "grok-imagine-image-quality", string(body), group, req, false}, {"video", policy, model, "", `{"video":true}`, group, req, false}, {"wildcard", "410/700/*", model, "", string(body), group, req, false}, {"streaming", policy, model, "", string(body), group, service.OpenAIImagesRequest{Model: model, Stream: true, N: 1}, false},
	} {
		t.Run(tc.label, func(t *testing.T) {
			id, ok := personalGrokImagePool(tc.policy, &tc.group, &tc.req, tc.model, tc.mapped, []byte(tc.body))
			if ok != tc.want || ok && id != 700 {
				t.Fatalf("pool=%d enabled=%v", id, ok)
			}
		})
	}
}

// Exercise the public Images handler. A typed empty upstream 200 is ambiguous,
// and must terminate even if a second selected provider was available.
func TestPersonalGrokImagesOneNativeDispatch(t *testing.T) {
	for _, tc := range []struct {
		name, policy string
		eligible     bool
		wantCalls    int
	}{
		{"admitted", "410/24/grok-imagine-image-2.0", true, 1},
		{"no eligibility", "410/24/grok-imagine-image-2.0", false, 0},
		{"wrong pool", "410/444/grok-imagine-image-2.0", true, 0},
		{"policy disabled", "", true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SUB2API_PERSONAL_GROK_IMAGE_POOL", tc.policy)
			handler, slots, bindings, up := newGrokMediaSlotHandler(t, true, false)
			handler.personalImageIntents = &imageClaimTestStore{}
			handler.grokMediaEligibilityProber = grokMediaSlotProber(func(context.Context, int64) (bool, string, error) { return tc.eligible, "fixture", nil })
			up.call = func(r *http.Request, id int64) (*http.Response, error) {
				if r.URL.Path != "/v1/images/generations" || r.URL.Host != "api.x.ai" {
					t.Fatalf("unexpected route %s", r.URL)
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"data":[]}`))}, nil
			}
			body := `{"model":"grok-imagine-image-2.0","prompt":"fixture","response_format":"b64_json"}`
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("POST", "/v1/images/generations", strings.NewReader(body))
			c.Request.Header.Set("Content-Type", "application/json")
			id := int64(410)
			c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{ID: 99, UserID: 100, GroupID: &id, Group: &service.Group{ID: id, Platform: service.PlatformOpenAI, AllowImageGeneration: true}, User: &service.User{ID: 100}})
			c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: 100, Concurrency: 5})
			handler.Images(c)
			if up.calls != tc.wantCalls {
				t.Errorf("expected %d native dispatches got %d", tc.wantCalls, up.calls)
			}
			if bindings.writes != 0 {
				t.Error("unexpected session write")
			}
			slots.assertReleased(t)
		})
	}
}

type personalImageUsageRecorder struct {
	service.UsageLogRepository
	logs []service.UsageLog
}

func (r *personalImageUsageRecorder) CreateBestEffort(_ context.Context, row *service.UsageLog) error {
	r.logs = append(r.logs, *row)
	return nil
}

func TestPersonalImagesRecordClientGroupOnly(t *testing.T) {
	for _, tc := range []struct {
		name             string
		eligible         bool
		expected         int64
		credentialsReady bool
	}{{"personal", true, 777, true}, {"origin after negative eligibility", false, 710, true}, {"origin after predispatch auth rejection", true, 710, false}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SUB2API_PERSONAL_GROK_IMAGE_POOL", "410/24/grok-imagine-image-2.0")
			cfg := &config.Config{RunMode: config.RunModeSimple}
			costs := service.NewBillingService(cfg, nil)
			usage := &personalImageUsageRecorder{}
			slots := &grokMediaSlotsCache{accounts: map[string]int64{}, users: map[string]int64{}}
			concurrency := service.NewConcurrencyService(slots)
			repo := openAIImagesFailoverAccountRepo{accounts: []service.Account{
				{ID: 777, Platform: service.PlatformGrok, Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true, GroupIDs: []int64{24}, Concurrency: 8, Credentials: map[string]any{"access_token": "fixture-token", "refresh_token": "fixture-refresh", "expires_at": time.Now().Add(7 * 24 * time.Hour).Format(time.RFC3339)}},
				{ID: 710, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, GroupIDs: []int64{410}, Concurrency: 8, Credentials: map[string]any{"api_key": "fixture-secret", "base_url": "https://origin.invalid/v1", "model_mapping": map[string]any{"grok-imagine-image-2.0": "grok-imagine-image-2.0"}}},
			}}
			if !tc.credentialsReady {
				repo.accounts[0].Credentials["expires_at"] = time.Now().Add(30 * time.Minute).Format(time.RFC3339)
			}
			response := `{"data":[{"b64_json":"aW1hZ2UtZml4dHVyZQ=="}]}`
			up := &grokMediaSlotUpstream{call: func(r *http.Request, id int64) (*http.Response, error) {
				if id != tc.expected {
					t.Errorf("wrong provider id %d", id)
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}, "X-Request-Id": {"fixture-receipt"}}, Body: io.NopCloser(strings.NewReader(response))}, nil
			}}
			grokTokens := service.NewGrokTokenProvider(repo, nil)
			gateway := service.NewOpenAIGatewayService(repo, usage, nil, nil, nil, nil, &grokMediaSlotBindings{}, cfg, nil, concurrency, costs, nil, nil, up, nil, nil, grokTokens, nil, nil, nil, nil, nil)
			billing := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
			t.Cleanup(billing.Stop)
			h := NewOpenAIGatewayHandler(gateway, concurrency, billing, service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
			h.personalImageIntents = &imageClaimTestStore{}
			h.grokMediaEligibilityProber = grokMediaSlotProber(func(context.Context, int64) (bool, string, error) { return tc.eligible, "fixture", nil })
			body := `{"model":"grok-imagine-image-2.0","prompt":"fixture","response_format":"b64_json"}`
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest("POST", "/v1/images/generations", strings.NewReader(body))
			c.Request.Header.Set("Content-Type", "application/json")
			groupID := int64(410)
			price := 0.006
			group := &service.Group{ID: groupID, Platform: service.PlatformOpenAI, AllowImageGeneration: true, RateMultiplier: 1, ImagePrice1K: &price}
			c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{ID: 99, UserID: 100, GroupID: &groupID, Group: group, User: &service.User{ID: 100}})
			c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: 100, Concurrency: 5})
			h.Images(c)
			if up.calls != 1 || w.Code != 200 || !strings.Contains(w.Body.String(), "aW1hZ2UtZml4dHVyZQ==") {
				t.Fatalf("calls=%d status=%d body=%s", up.calls, w.Code, w.Body.String())
			}
			if len(usage.logs) != 1 || usage.logs[0].GroupID == nil || *usage.logs[0].GroupID != 410 || usage.logs[0].APIKeyID != 99 || usage.logs[0].AccountID != tc.expected || usage.logs[0].ImageCount != 1 {
				t.Fatalf("unexpected usage %+v", usage.logs)
			}
			slots.assertReleased(t)
		})
	}
}

func TestGrokSlotLivesUntilDetachedPaidDispatchCompletes(t *testing.T) {
	t.Setenv("SUB2API_PERSONAL_GROK_IMAGE_POOL", "410/24/grok-imagine-image-2.0")
	h, slots, _, up := newGrokMediaSlotHandler(t, true, false)
	h.personalImageIntents = &imageClaimTestStore{}
	h.grokMediaEligibilityProber = grokMediaSlotProber(func(context.Context, int64) (bool, string, error) { return true, "fixture", nil })
	started := make(chan struct{})
	unblock := make(chan struct{})
	finish := sync.OnceFunc(func() { close(unblock) })
	defer finish()
	up.call = func(*http.Request, int64) (*http.Response, error) {
		close(started)
		<-unblock
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"data":[]}`))}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/v1/images/generations", strings.NewReader(`{"model":"grok-imagine-image-2.0","prompt":"fixture","response_format":"b64_json"}`)).WithContext(ctx)
	c.Request.Header.Set("Content-Type", "application/json")
	id := int64(410)
	key := &service.APIKey{ID: 99, UserID: 100, GroupID: &id, Group: &service.Group{ID: id, Platform: service.PlatformOpenAI, AllowImageGeneration: true}, User: &service.User{ID: 100}}
	c.Set(string(middleware2.ContextKeyAPIKey), key)
	c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: 100, Concurrency: 3})
	done := make(chan struct{})
	go func() { h.Images(c); close(done) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("missing upstream dispatch")
	}
	cancel()
	// Any on-cancel callbacks have time to run while the provider is blocked.
	require.Never(t, func() bool { slots.mu.Lock(); defer slots.mu.Unlock(); return len(slots.accounts) == 0 }, 60*time.Millisecond, 5*time.Millisecond)
	finish()
	<-done
	slots.assertReleased(t)
}
