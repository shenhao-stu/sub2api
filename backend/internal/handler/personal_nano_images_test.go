//go:build unit

package handler

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
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

func TestPersonalNanoImagesEndToEnd(t *testing.T) {
	const image = `{"data":[{"b64_json":"aW1hZ2U=","size":"1024x1024"}]}`
	const regularBody = `{"model":"nano-banana-2","prompt":"fixture"}`
	cases := []struct {
		name, path, body, mode          string
		wantNano, wantOrigin, wantUsage int
	}{
		{"duplicate during native", "/v1/images/generations", regularBody, "concurrent", 1, 0, 1},
		{"native", "/v1/images/generations", regularBody, "success", 1, 0, 1},
		{"signed busy", "/v1/images/generations", regularBody, "signed", 1, 1, 1},
		{"unsigned busy", "/v1/images/generations", regularBody, "unsigned", 1, 0, 0},
		{"upstream balance text", "/v1/images/generations", regularBody, "balance", 1, 0, 0},
		{"upstream forbidden", "/v1/images/generations", regularBody, "forbidden", 1, 0, 0},
		{"ambiguous 503", "/v1/images/generations", regularBody, "ambiguous", 1, 0, 0},
		{"duplicate", "/v1/images/generations", regularBody, "duplicate", 1, 0, 0},
		{"stream uses existing provider", "/v1/images/generations", `{"model":"nano-banana-2","prompt":"fixture","stream":true}`, "success", 0, 1, 1},
		{"original resolution tier", "/v1/images/generations", `{"model":"nano-banana-2","prompt":"fixture","size":"2048x2048"}`, "success", 0, 1, 1},
		{"unsupported format", "/v1/images/generations", `{"model":"nano-banana-2","prompt":"fixture","response_format":"url"}`, "success", 0, 1, 1},
		{"unsupported edit", "/v1/images/edits", `{"model":"nano-banana-2","prompt":"fixture","images":[{"image_url":"https://example.invalid/fixture"}]}`, "success", 0, 1, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pub, priv, err := ed25519.GenerateKey(rand.Reader)
			require.NoError(t, err)
			adapterCalls := 0
			started := make(chan struct{})
			unblock := make(chan struct{})
			finish := sync.OnceFunc(func() { close(unblock) })
			defer finish()
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				adapterCalls++
				require.Equal(t, "/v1/images/generations", r.URL.Path)
				require.Equal(t, "Bearer peer-key", r.Header.Get("Authorization"))
				require.Empty(t, r.Header.Get("Idempotency-Key"))
				require.Empty(t, r.Header.Get("X-Client-Request-ID"))
				id, challenge := r.Header.Get("X-Request-ID"), r.Header.Get("X-Nano-Attempt")
				require.Len(t, id, 64)
				require.Len(t, challenge, 32)
				w.Header().Set("Content-Type", "application/json")
				switch tc.mode {
				case "concurrent":
					close(started)
					<-unblock
					io.WriteString(w, image)
				case "success":
					io.WriteString(w, image)
				case "signed", "unsigned":
					if tc.mode == "signed" {
						sign := ed25519.Sign(priv, []byte(fmt.Sprintf("nano-admission-v1\n429\nPOST\n%s\n%s\n%s\nadapter_busy", r.URL.Path, id, challenge)))
						w.Header().Set("X-Nano-Admission-Proof", base64.StdEncoding.EncodeToString(sign))
					}
					w.WriteHeader(429)
					io.WriteString(w, `{"error":{"code":"adapter_busy","type":"image_request_error","message":"full"}}`)
				case "forbidden":
					w.WriteHeader(403)
					io.WriteString(w, `{"error":{"type":"permission_error","message":"Access denied"}}`)
				case "balance":
					w.WriteHeader(403)
					io.WriteString(w, `{"error":{"message":"insufficient_balance: insufficient balance for precharge"}}`)
				case "ambiguous":
					w.WriteHeader(503)
					io.WriteString(w, `{"error":{"code":"upstream_rejected"}}`)
				case "duplicate":
					w.WriteHeader(409)
					io.WriteString(w, `{"error":{"code":"duplicate_request"}}`)
				}
			}))
			t.Cleanup(peer.Close)
			t.Setenv("SUB2API_PERSONAL_NANO_IMAGE_POOL", "600/601")
			t.Setenv("SUB2API_PERSONAL_NANO_IMAGE_PEER", peer.URL)
			t.Setenv("SUB2API_PERSONAL_NANO_ADMISSION_PUBLIC_KEY", base64.StdEncoding.EncodeToString(pub))
			t.Setenv("SUB2API_PERSONAL_NANO_IMAGE_TIERS", "1K")
			cfg := &config.Config{RunMode: config.RunModeSimple}
			costs := service.NewBillingService(cfg, nil)
			usage := &personalImageUsageRecorder{}
			slots := &grokMediaSlotsCache{accounts: map[string]int64{}, users: map[string]int64{}}
			concurrency := service.NewConcurrencyService(slots)
			acct := func(id int64, base, key string) service.Account {
				return service.Account{ID: id, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, GroupIDs: []int64{600}, Concurrency: 8, Credentials: map[string]any{"api_key": key, "base_url": base, "model_mapping": map[string]any{"nano-banana-2": "nano-banana-2"}}}
			}
			origin := acct(635, "https://origin.invalid/v1", "origin-key")
			donor := acct(636, peer.URL+"/v1", "peer-key")
			donor.GroupIDs = []int64{601}
			donor.Extra = map[string]any{"personal_nano_image_adapter": true}
			repo := openAIImagesFailoverAccountRepo{accounts: []service.Account{origin, donor}}
			up := &grokMediaSlotUpstream{call: func(r *http.Request, id int64) (*http.Response, error) {
				require.Equal(t, int64(635), id)
				require.Equal(t, "Bearer origin-key", r.Header.Get("Authorization"))
				require.Empty(t, r.Header.Get("X-Nano-Attempt"))
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(image))}, nil
			}}
			gateway := service.NewOpenAIGatewayService(repo, usage, nil, nil, nil, nil, &grokMediaSlotBindings{}, cfg, nil, concurrency, costs, nil, nil, up, nil, nil, nil, nil, nil, nil, nil, nil)
			billing := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
			t.Cleanup(billing.Stop)
			handler := NewOpenAIGatewayHandler(gateway, concurrency, billing, service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
			handler.personalImageIntents = &imageClaimTestStore{}
			price := 0.014
			send := func(payload string) *httptest.ResponseRecorder {
				w := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(w)
				c.Request = httptest.NewRequest("POST", tc.path, strings.NewReader(payload)).WithContext(context.WithValue(context.Background(), ctxkey.ClientRequestID, "fixture-id"))
				c.Request.Header.Set("Content-Type", "application/json")
				c.Request.Header.Set("X-Nano-Attempt", "spoof")
				c.Request.Header.Set("Idempotency-Key", "spoof")
				gid := int64(600)
				group := &service.Group{ID: gid, Platform: service.PlatformOpenAI, AllowImageGeneration: true, RateMultiplier: 1, ImagePrice1K: &price}
				c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{ID: 608, UserID: 609, GroupID: &gid, Group: group, User: &service.User{ID: 609}})
				c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: 609, Concurrency: 3})
				handler.Images(c)
				return w
			}
			var w *httptest.ResponseRecorder
			if tc.mode == "concurrent" {
				done := make(chan *httptest.ResponseRecorder, 1)
				go func() { done <- send(tc.body) }()
				select {
				case <-started:
				case <-time.After(4 * time.Second):
					t.Fatal("no initial private dispatch")
				}
				// A changed request shape would ordinarily select Origin. The same
				// intent has already paid the donor, so this attempt must stop here.
				duplicate := send(`{"model":"nano-banana-2","prompt":"fixture","response_format":"url"}`)
				require.Equal(t, 409, duplicate.Code, duplicate.Body.String())
				finish()
				w = <-done
			} else {
				w = send(tc.body)
			}

			require.Equal(t, tc.wantNano, adapterCalls, w.Body.String())
			require.Equal(t, tc.wantOrigin, up.calls, w.Body.String())
			require.Len(t, usage.logs, tc.wantUsage, w.Body.String())
			if tc.wantUsage == 1 {
				u := usage.logs[0]
				require.Equal(t, int64(600), *u.GroupID)
				require.Equal(t, int64(608), u.APIKeyID)
				require.Equal(t, 1, u.ImageCount)
				require.InDelta(t, price, u.TotalCost, 1e-9)
				wantID := int64(636)
				if tc.wantOrigin > 0 {
					wantID = 635
				}
				require.Equal(t, wantID, u.AccountID)
			}
			slots.assertReleased(t)
		})
	}
}

// Mimics PostgreSQL's atomic ON CONFLICT, without replay or lease reclaim.
type imageClaimTestStore struct {
	service.IdempotencyRepository
	mu     sync.Mutex
	claims map[string]bool
}

func (s *imageClaimTestStore) CreateProcessing(_ context.Context, record *service.IdempotencyRecord) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claims == nil {
		s.claims = map[string]bool{}
	}
	if s.claims[record.IdempotencyKeyHash] {
		return false, nil
	}
	s.claims[record.IdempotencyKeyHash] = true
	return true, nil
}
