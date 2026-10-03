//go:build unit

package handler

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/requestlifecycle"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type shutdownBillingUpstream struct {
	service.HTTPUpstream
	client *http.Client
	target *url.URL
}

func (u *shutdownBillingUpstream) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.URL.Scheme, req.URL.Host, req.Host = u.target.Scheme, u.target.Host, u.target.Host
	return u.client.Do(req)
}

type shutdownBillingRecorder struct {
	*httptest.ResponseRecorder
	visible chan struct{}
	once    sync.Once
}

func (w *shutdownBillingRecorder) Write(p []byte) (int, error) {
	n, err := w.ResponseRecorder.Write(p)
	if strings.Contains(string(p), "shutdown-billing-fixture") {
		w.once.Do(func() { close(w.visible) })
	}
	return n, err
}

func TestForcedUpstreamShutdownRecordsObservedUsageExactlyOnce(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, observedUsage := range []bool{true, false} {
		t.Run(map[bool]string{true: "observed_partial_usage", false: "no_invented_usage"}[observedUsage], func(t *testing.T) {
			abort := make(chan struct{})
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "text/event-stream")
				if observedUsage {
					_, _ = io.WriteString(w, "data: {\"type\":\"response.in_progress\",\"response\":{\"usage\":{\"input_tokens\":100,\"output_tokens\":7,\"input_tokens_details\":{\"cached_tokens\":40}}}}\n\n")
				}
				_, _ = io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"shutdown-billing-fixture\"}\n\n")
				w.(http.Flusher).Flush()
				select {
				case <-r.Context().Done():
				case <-abort:
				}
			}))
			t.Cleanup(upstream.Close)
			t.Cleanup(func() { close(abort) })
			target, err := url.Parse(upstream.URL)
			require.NoError(t, err)
			accounts := []service.Account{{ID: 9930, Platform: service.PlatformGrok, Type: service.AccountTypeAPIKey,
				Status: service.StatusActive, Schedulable: true,
				Credentials: map[string]any{"api_key": "offline-only", "base_url": "https://api.x.ai/v1"}}}
			cfg := &config.Config{RunMode: config.RunModeSimple}
			cfg.Default.RateMultiplier = 1
			accountRepo := &grokPartialUsageAccountRepo{grokStreamFailoverAccountRepo: grokStreamFailoverAccountRepo{openAIWSFailoverHandlerAccountRepoStub: openAIWSFailoverHandlerAccountRepoStub{accounts: accounts}}}
			usageRepo := &openAIWSUsageHandlerUsageLogRepoStub{created: make(chan *service.UsageLog, 3)}
			billingCache := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
			t.Cleanup(billingCache.Stop)
			gateway := service.NewOpenAIGatewayService(accountRepo, usageRepo, nil, nil, nil, nil, nil, cfg, nil, nil,
				service.NewBillingService(cfg, nil), nil, billingCache, &shutdownBillingUpstream{client: upstream.Client(), target: target}, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil)
			h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(nil), billingCache,
				service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
			recorder := &shutdownBillingRecorder{ResponseRecorder: httptest.NewRecorder(), visible: make(chan struct{})}
			c, _ := gin.CreateTestContext(recorder)
			client, cancelClient := context.WithCancel(context.Background())
			defer cancelClient()
			force, cancelForce := context.WithCancel(context.Background())
			defer cancelForce()
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"grok-4.6","input":"test","stream":true}`)).WithContext(requestlifecycle.WithForceCancellation(client, force))
			groupID := int64(4230)
			c.Set(string(middleware.ContextKeyAPIKey), &service.APIKey{ID: 1830, GroupID: &groupID,
				User: &service.User{ID: 1730, Status: service.StatusActive}, Group: &service.Group{ID: groupID, Platform: service.PlatformGrok, Status: service.StatusActive}})
			c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 1730})
			done := make(chan struct{})
			go func() { h.Responses(c); close(done) }()
			select {
			case <-recorder.visible:
			case <-time.After(2 * time.Second):
				t.Fatal("upstream did not emit the observed response")
			}
			cancelClient()
			cancelForce()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("handler did not return through the usage settlement path")
			}
			require.EqualValues(t, 1, calls.Load())
			require.Len(t, usageRepo.created, 1, "partial result must reach exactly one usage callback")
			usage := <-usageRepo.created
			if observedUsage {
				require.Equal(t, 60, usage.InputTokens)
				require.Equal(t, 40, usage.CacheReadTokens)
				require.Equal(t, 7, usage.OutputTokens)
				require.Greater(t, usage.TotalCost, 0.0)
			} else {
				require.Zero(t, usage.InputTokens+usage.OutputTokens+usage.CacheReadTokens+usage.CacheCreationTokens)
				require.Zero(t, usage.TotalCost)
				require.Zero(t, usage.ActualCost)
			}
		})
	}
}
