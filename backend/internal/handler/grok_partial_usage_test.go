package handler

import (
	"context"
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

type grokPartialUsageUpstream struct {
	service.HTTPUpstream
	accountIDs       []int64
	preOutputFailure bool
	preOutputTokens  int
	cyberFailure     bool
}

type grokPartialUsageOpsRepo struct {
	service.OpsRepository
	recorded chan struct{}
}

func (r *grokPartialUsageOpsRepo) InsertErrorLog(context.Context, *service.OpsInsertErrorLogInput) (int64, error) {
	r.recorded <- struct{}{}
	return 1, nil
}

func (r *grokPartialUsageOpsRepo) BatchInsertErrorLogs(context.Context, []*service.OpsInsertErrorLogInput) (int64, error) {
	r.recorded <- struct{}{}
	return 1, nil
}

type grokPartialUsageAccountRepo struct{ grokStreamFailoverAccountRepo }

func (r *grokPartialUsageAccountRepo) SetTempUnschedulable(context.Context, int64, time.Time, string) error {
	return nil
}

func (u *grokPartialUsageUpstream) Do(_ *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	u.accountIDs = append(u.accountIDs, accountID)
	body := `data: {"type":"response.output_text.delta","delta":"visible output"}` + "\n\n" +
		`data: {"type":"response.failed","response":{"id":"resp_billed_partial","error":{"code":"server_error","message":"upstream stopped"},"usage":{"input_tokens":100,"output_tokens":7,"input_tokens_details":{"cached_tokens":40}}}}` + "\n\n"
	if u.preOutputFailure && accountID == 9930 {
		body = fmt.Sprintf(`data: {"type":"response.failed","response":{"error":{"code":"rate_limit_exceeded","message":"Rate limit exceeded"},"usage":{"input_tokens":%d,"output_tokens":0}}}`+"\n\n", u.preOutputTokens)
	}
	if u.cyberFailure {
		body = strings.ReplaceAll(body, `"code":"server_error","message":"upstream stopped"`, `"code":"cyber_policy","message":"flagged for cyber policy"`)
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
}

func TestGrokResponsesPartialUsageSubmittedOnceWithoutReplay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name                           string
		preOutputFailure, cyberFailure bool
		preOutputTokens                int
	}{
		{name: "partial_failure"},
		{name: "failover_then_partial_failure", preOutputFailure: true},
		{name: "metered_preoutput_failure", preOutputFailure: true, preOutputTokens: 900},
		{name: "cyber_partial_failure", cyberFailure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			accounts := make([]service.Account, 2)
			for i := range accounts {
				accounts[i] = service.Account{ID: int64(9930 + i), Platform: service.PlatformGrok,
					Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Priority: i + 1,
					Credentials: map[string]any{"api_key": "test-only-token", "base_url": "https://api.x.ai/v1"}}
			}
			cfg := &config.Config{RunMode: config.RunModeSimple}
			cfg.Default.RateMultiplier = 1
			cfg.Gateway.MaxAccountSwitches = 1
			accountRepo := &grokPartialUsageAccountRepo{grokStreamFailoverAccountRepo: grokStreamFailoverAccountRepo{openAIWSFailoverHandlerAccountRepoStub: openAIWSFailoverHandlerAccountRepoStub{accounts: accounts}}}
			usageRepo := &openAIWSUsageHandlerUsageLogRepoStub{created: make(chan *service.UsageLog, 3)}
			upstream := &grokPartialUsageUpstream{preOutputFailure: tc.preOutputFailure, preOutputTokens: tc.preOutputTokens, cyberFailure: tc.cyberFailure}
			billingCache := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
			t.Cleanup(billingCache.Stop)
			gateway := service.NewOpenAIGatewayService(accountRepo, usageRepo, nil, nil, nil, nil, nil, cfg, nil, nil,
				service.NewBillingService(cfg, nil), nil, billingCache, upstream, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil)
			h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(nil), billingCache,
				service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
			opsRepo := &grokPartialUsageOpsRepo{recorded: make(chan struct{}, 1)}
			if tc.cyberFailure {
				h.opsService = service.NewOpsService(opsRepo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
			}
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"grok-4.6","input":"test","stream":true}`))
			groupID := int64(4230)
			c.Set(string(middleware.ContextKeyAPIKey), &service.APIKey{ID: 1830, GroupID: &groupID,
				User:  &service.User{ID: 1730, Status: service.StatusActive},
				Group: &service.Group{ID: groupID, Platform: service.PlatformGrok, Status: service.StatusActive}})
			c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 1730})
			h.Responses(c)
			if tc.cyberFailure {
				// The cyber audit is enqueued after its optional fallback bill. This
				// observable barrier detects duplicate billing without timing guesses.
				select {
				case <-opsRepo.recorded:
				case <-time.After(3 * time.Second):
					t.Fatal("cyber audit did not complete")
				}
			}

			wantCalls := []int64{9930}
			if tc.preOutputFailure && tc.preOutputTokens == 0 {
				wantCalls = append(wantCalls, 9931)
			}
			require.Equal(t, wantCalls, upstream.accountIDs)
			require.Len(t, usageRepo.created, 1, "only the final committed attempt reaches customer usage recording")
			usage := <-usageRepo.created
			require.Equal(t, wantCalls[len(wantCalls)-1], usage.AccountID)
			if tc.preOutputTokens > 0 {
				require.Equal(t, tc.preOutputTokens, usage.InputTokens)
				require.Zero(t, usage.CacheReadTokens)
				require.Zero(t, usage.OutputTokens)
			} else {
				require.Equal(t, 60, usage.InputTokens, "cached tokens must not also count as regular input")
				require.Equal(t, 40, usage.CacheReadTokens)
				require.Equal(t, 7, usage.OutputTokens)
			}
			if tc.cyberFailure {
				require.Equal(t, service.RequestTypeCyberBlocked, usage.RequestType)
			}
			require.Greater(t, usage.TotalCost, 0.0)
			wantOutput := 1
			if tc.preOutputTokens > 0 {
				wantOutput = 0
			}
			require.Equal(t, wantOutput, strings.Count(recorder.Body.String(), "visible output"))
		})
	}
}
