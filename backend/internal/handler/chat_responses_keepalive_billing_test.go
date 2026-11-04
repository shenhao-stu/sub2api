//go:build unit

package handler

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// Use the actual handler, forwarding service, billing calculation and Ops
// middleware. Only persistence and the upstream transport are test doubles.
func TestChatResponsesKeepaliveLateFailureRecordsActualOpsAndBilling(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name                string
		status              int
		measured, httpError bool
	}{
		{name: "late_http_400", status: 400, httpError: true},
		{name: "late_http_503_metered", status: 503, httpError: true, measured: true},
		{name: "late_sse_400", status: 400},
		{name: "late_sse_503_metered", status: 503, measured: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setupOpsErrorLogTestQueue(t, 2)
			accounts := make([]service.Account, 2)
			for i := range accounts {
				accounts[i] = service.Account{ID: int64(19930 + i), Platform: service.PlatformOpenAI,
					Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Priority: i + 1,
					Credentials: map[string]any{"api_key": "synthetic", "model_mapping": map[string]any{"gemini-4-argon": "ultra/gemini-4-argon-high"}},
					Extra:       map[string]any{"openai_responses_supported": true}}
			}
			accountRepo := &grokPartialUsageAccountRepo{grokStreamFailoverAccountRepo: grokStreamFailoverAccountRepo{openAIWSFailoverHandlerAccountRepoStub: openAIWSFailoverHandlerAccountRepoStub{accounts: accounts}}}
			usageRepo := &openAIWSUsageHandlerUsageLogRepoStub{created: make(chan *service.UsageLog, 3)}
			billingRepo := &monitorCyberBillingRepo{commands: make(chan *service.UsageBillingCommand, 3)}
			userRepo := &monitorCyberUserRepo{}
			cfg := &config.Config{}
			cfg.Default.RateMultiplier = 1
			cfg.Gateway.MaxAccountSwitches = 0
			cfg.Gateway.StreamKeepaliveInterval = 1
			billingCache := service.NewBillingCacheService(nil, userRepo, nil, nil, nil, nil, cfg, nil)
			t.Cleanup(billingCache.Stop)
			upstream := &grokMediaSlotUpstream{call: func(req *http.Request, _ int64) (*http.Response, error) {
				require.Equal(t, "/v1/responses", req.URL.Path)
				usage := ""
				if tc.measured {
					usage = `,"usage":{"input_tokens":100,"output_tokens":0,"input_tokens_details":{"cached_tokens":90}}`
				}

				time.Sleep(2 * time.Second)
				message := "synthetic invalid request"
				code := "invalid_request"
				if tc.status == 503 {
					message = "synthetic unavailable"
					code = "overloaded_error"
				}
				errJSON := fmt.Sprintf(`{"type":"invalid_request_error","code":%q,"message":%q,"status_code":%d}`, code, message, tc.status)
				if tc.httpError {
					return &http.Response{StatusCode: tc.status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"error":` + errJSON + usage + `}`))}, nil
				}
				body := `data: {"type":"response.failed","response":{"status":"failed","error":` + errJSON + usage + "}}\n\n"
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
			}}
			gateway := service.NewOpenAIGatewayService(accountRepo, usageRepo, billingRepo, userRepo, nil, nil, nil, cfg, nil, nil,
				service.NewBillingService(cfg, nil), nil, billingCache, upstream, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil)
			h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(nil), billingCache,
				service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
			ops := service.NewOpsService(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
			router := gin.New()
			router.Use(OpsErrorLoggerMiddleware(ops))
			router.POST("/v1/chat/completions", func(c *gin.Context) {
				groupID := int64(14230)
				c.Set(string(middleware.ContextKeyAPIKey), &service.APIKey{ID: 11830, UserID: 11730, GroupID: &groupID,
					User:  &service.User{ID: 11730, Status: service.StatusActive, Balance: 100},
					Group: &service.Group{ID: groupID, Hydrated: true, Platform: service.PlatformOpenAI, Status: service.StatusActive, RateMultiplier: 1}})
				c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 11730})
				h.ChatCompletions(c)
			})
			rec := httptest.NewRecorder()
			body := `{"model":"gemini-4-argon","messages":[{"role":"user","content":"synthetic"}],"stream":true}`
			router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
			require.Equal(t, 1, upstream.calls, "two eligible accounts must not cause replay")
			require.Contains(t, rec.Body.String(), fmt.Sprintf(`"status_code":%d`, tc.status))
			require.Equal(t, 1, strings.Count(rec.Body.String(), `"error":`))
			require.NotContains(t, rec.Body.String(), "data: [DONE]")
			require.Equal(t, 200, rec.Code)
			require.Equal(t, int64(1), OpsErrorLogQueueLength())
			entry := (<-opsErrorLogQueue).entry
			require.Equal(t, tc.status, entry.StatusCode)
			// Classification remains owned by Ops; the stored status is the terminal failure.
			if tc.measured {
				require.Len(t, billingRepo.commands, 1)
				cmd := <-billingRepo.commands
				require.Equal(t, 10, cmd.InputTokens)
				require.Equal(t, 90, cmd.CacheReadTokens)
				require.Zero(t, cmd.OutputTokens)
				require.InDelta(t, 29e-6, cmd.BalanceCost, 1e-12)
				require.Len(t, usageRepo.created, 1)
				log := <-usageRepo.created
				require.Equal(t, 10, log.InputTokens)
				require.Equal(t, 90, log.CacheReadTokens)
				require.InDelta(t, cmd.BalanceCost, log.ActualCost, 1e-12)
			} else {
				for len(billingRepo.commands) > 0 {
					require.Zero(t, (<-billingRepo.commands).BalanceCost)
				}
				for len(usageRepo.created) > 0 {
					require.Zero(t, (<-usageRepo.created).ActualCost)
				}
			}
		})
	}
}
