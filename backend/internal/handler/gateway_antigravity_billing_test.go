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

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	middleware "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type antigravityBillingTokenCache struct{ service.AntigravityTokenCache }

func (antigravityBillingTokenCache) GetAccessToken(context.Context, string) (string, error) {
	return "fixture-token", nil
}

func TestAntigravityEmptyResponseBillsObservedUsageWithoutRetry(t *testing.T) {
	for _, responses := range []bool{false, true} {
		for _, cancelClient := range []bool{false, true} {
			t.Run(fmt.Sprintf("responses=%v/cancel=%v", responses, cancelClient), func(t *testing.T) {
				group := &service.Group{ID: 24, Platform: service.PlatformAntigravity, Hydrated: true, Status: service.StatusActive, RateMultiplier: 1}
				account := &service.Account{ID: 1, Platform: service.PlatformAntigravity, Type: service.AccountTypeOAuth,
					Status: service.StatusActive, Schedulable: true, Concurrency: 10, GroupIDs: []int64{24},
					AccountGroups: []service.AccountGroup{{AccountID: 1, GroupID: 24}},
					Credentials:   map[string]any{"project_id": "fixture-project", "model_mapping": map[string]any{"gemini-3.1-pro-high": "gemini-3.1-pro-high"}}}
				h, cleanup := newTestGatewayHandler(t, group, []*service.Account{account})
				t.Cleanup(cleanup)
				cfg := &config.Config{}
				cfg.Default.RateMultiplier = 1
				billing := &mediaCompletionBillingRepo{apply: func(ctx context.Context) error {
					require.NoError(t, ctx.Err())
					_, bounded := ctx.Deadline()
					require.True(t, bounded)
					return nil
				}}
				snapshot := service.NewSchedulerSnapshotService(&fakeSchedulerCache{accounts: []*service.Account{account}}, nil, nil, nil, nil)
				h.gatewayService = service.NewGatewayService(nil, &fakeGroupRepo{group: group}, nil, billing,
					nil, nil, nil, nil, cfg, snapshot, nil, service.NewBillingService(cfg, nil),
					nil, nil, nil, nil, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				upstream := &grokMediaSlotUpstream{call: func(*http.Request, int64) (*http.Response, error) {
					if cancelClient {
						cancel()
					}
					return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(
						`data: {"response":{"usageMetadata":{"promptTokenCount":8,"candidatesTokenCount":3},"candidates":[{"finishReason":"STOP"}]}}` + "\n\n"))}, nil
				}}
				token := service.NewAntigravityTokenProvider(nil, antigravityBillingTokenCache{}, nil)
				h.antigravityGatewayService = service.NewAntigravityGatewayService(nil, nil, nil, token, nil, upstream, nil, nil)
				body, path := `{"model":"gemini-3.1-pro-high","stream":true,"messages":[{"role":"user","content":"fixture"}]}`, "/v1/chat/completions"
				call := h.ChatCompletions
				if responses {
					body, path, call = `{"model":"gemini-3.1-pro-high","stream":true,"input":"fixture"}`, "/v1/responses", h.Responses
				}
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)).WithContext(context.WithValue(ctx, ctxkey.Group, group))
				key := &service.APIKey{ID: 20, UserID: 10, GroupID: &group.ID, Group: group, User: &service.User{ID: 10, Balance: 100}}
				c.Set(string(middleware.ContextKeyAPIKey), key)
				c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 10, Concurrency: 10})
				call(c)
				require.Equal(t, 1, upstream.calls)
				require.Len(t, billing.commands, 1)
				require.Equal(t, 8, billing.commands[0].InputTokens)
				require.Equal(t, 3, billing.commands[0].OutputTokens)
				require.Positive(t, billing.commands[0].BalanceCost)
				require.NotEqual(t, http.StatusOK, c.Writer.Status())
			})
		}
	}
}
