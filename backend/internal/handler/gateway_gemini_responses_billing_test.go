//go:build unit

package handler

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	middleware "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestGeminiResponsesDispatchBillsExactlyOnce(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, cancelClient := range []bool{false, true} {
			for _, final := range []bool{false, true} {
				t.Run(fmt.Sprintf("stream=%t/cancel=%t/final=%t", stream, cancelClient, final), func(t *testing.T) {

					group := &service.Group{ID: 54, Platform: service.PlatformGemini, Hydrated: true, Status: service.StatusActive, RateMultiplier: 1}
					account := &service.Account{ID: 811, Platform: service.PlatformGemini, Type: service.AccountTypeServiceAccount, Status: service.StatusActive, Schedulable: true, Concurrency: 10, GroupIDs: []int64{54}, AccountGroups: []service.AccountGroup{{AccountID: 811, GroupID: 54}}, Credentials: map[string]any{"auth_mode": "apikey", "api_key": "isolated-key", "location": "global", "tier_id": "vertex", "model_mapping": map[string]any{"gemini-4-argon": "gemini-4-argon"}}}
					h, cleanup := newTestGatewayHandler(t, group, []*service.Account{account})
					t.Cleanup(cleanup)
					cfg := &config.Config{}
					cfg.Default.RateMultiplier = 1
					logs := &cachedBillingUsageRepo{}
					billing := &mediaCompletionBillingRepo{apply: func(ctx context.Context) error { require.NoError(t, ctx.Err()); return nil }}
					slots := &helperConcurrencyCacheStub{accountSeq: []bool{true}, userSeq: []bool{true}}
					concurrency := service.NewConcurrencyService(slots)
					h.concurrencyHelper = NewConcurrencyHelper(concurrency, SSEPingFormatClaude, 0)
					snapshot := service.NewSchedulerSnapshotService(&fakeSchedulerCache{accounts: []*service.Account{account}}, nil, nil, nil, nil)
					h.gatewayService = service.NewGatewayService(nil, &fakeGroupRepo{group: group}, logs, billing, nil, nil, nil, nil, cfg, snapshot, concurrency, service.NewBillingService(cfg, nil), nil, nil, nil, nil, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
					synctest.Test(t, func(t *testing.T) {
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						upstream := &grokMediaSlotUpstream{call: func(req *http.Request, _ int64) (*http.Response, error) {
							require.Equal(t, "aiplatform.googleapis.com", req.URL.Host)
							if cancelClient {
								cancel()
								require.NoError(t, req.Context().Err())
								synctest.Wait()
								slots.mu.Lock()
								require.Zero(t, slots.accountReleaseCalls, "upstream is still running after caller cancellation")
								require.Zero(t, slots.userReleaseCalls, "detached work still occupies its user slot")
								slots.mu.Unlock()
							}
							body := `{"candidates":[{"content":{"role":"model","parts":[{"text":"OK"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":19,"cachedContentTokenCount":5,"candidatesTokenCount":13,"thoughtsTokenCount":2,"candidatesTokensDetails":[{"modality":"IMAGE","tokenCount":10},{"modality":"TEXT","tokenCount":3}]}}`
							if !final {
								body = strings.Replace(body, `,"finishReason":"STOP"`, "", 1)
							}
							if stream {
								body = "data: " + body + "\n\n"
							}
							return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, nil
						}}
						h.geminiCompatService = service.NewGeminiMessagesCompatService(nil, nil, nil, nil, nil, nil, upstream, nil, cfg)
						body := fmt.Sprintf(`{"model":"gemini-4-argon","stream":%t,"input":"fixture"}`, stream)
						rec := httptest.NewRecorder()
						c, _ := gin.CreateTestContext(rec)
						c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body)).WithContext(context.WithValue(ctx, ctxkey.Group, group))
						key := &service.APIKey{ID: 20, UserID: 10, GroupID: &group.ID, Group: group, User: &service.User{ID: 10, Balance: 100}}
						c.Set(string(middleware.ContextKeyAPIKey), key)
						c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 10, Concurrency: 10})
						h.Responses(c)
						require.Equal(t, 1, upstream.calls, rec.Body.String())
						require.Len(t, billing.commands, 1, rec.Body.String())
						require.Equal(t, 14, billing.commands[0].InputTokens)
						require.Equal(t, 15, billing.commands[0].OutputTokens)
						require.Equal(t, 10, logs.first().ImageOutputTokens)
						require.InDelta(t, 10e-5, logs.first().ImageOutputCost, 1e-10)
						require.InDelta(t, 5e-5, logs.first().OutputCost, 1e-10)
						require.Positive(t, billing.commands[0].BalanceCost)
						slots.mu.Lock()
						require.Equal(t, 1, slots.accountReleaseCalls)
						require.Equal(t, 1, slots.userReleaseCalls)
						slots.mu.Unlock()
					})
				})
			}
		}
	}
}
