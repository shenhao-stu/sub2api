//go:build unit

package handler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type emptyPoolContractRepo struct{ grokPartialUsageAccountRepo }

func (r *emptyPoolContractRepo) ListModelAvailabilityCandidates(context.Context, *int64, []string, bool) ([]service.Account, error) {
	return nil, nil
}

func TestOpenAIEmptyPoolContractBeforeDispatch(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		endpoint string
		stream   bool
	}{
		{"/v1/chat/completions", false}, {"/v1/chat/completions", true},
		{"/v1/responses", false}, {"/v1/responses", true},
	} {
		endpoint := tc.endpoint
		t.Run(fmt.Sprintf("%s/stream=%t", endpoint, tc.stream), func(t *testing.T) {
			accounts := &emptyPoolContractRepo{}
			usage := &openAIWSUsageHandlerUsageLogRepoStub{created: make(chan *service.UsageLog, 1)}
			billing := &monitorCyberBillingRepo{commands: make(chan *service.UsageBillingCommand, 1)}
			users := &monitorCyberUserRepo{}
			cfg := &config.Config{}
			cfg.Default.RateMultiplier = 1
			cache := service.NewBillingCacheService(nil, users, nil, nil, nil, nil, cfg, nil)
			t.Cleanup(cache.Stop)
			upstream := &grokMediaSlotUpstream{call: func(*http.Request, int64) (*http.Response, error) {
				t.Fatal("empty pool must not dispatch")
				return nil, nil
			}}
			gateway := service.NewOpenAIGatewayService(accounts, usage, billing, users, nil, nil, nil, cfg, nil, nil,
				service.NewBillingService(cfg, nil), nil, cache, upstream, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil)
			h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(nil), cache,
				service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
			router := gin.New()
			router.POST(endpoint, func(c *gin.Context) {
				groupID := int64(14230)
				c.Set(string(middleware.ContextKeyAPIKey), &service.APIKey{ID: 11830, UserID: 11730, GroupID: &groupID,
					User:  &service.User{ID: 11730, Status: service.StatusActive, Balance: 100},
					Group: &service.Group{ID: groupID, Hydrated: true, Platform: service.PlatformOpenAI, Status: service.StatusActive, RateMultiplier: 1}})
				c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 11730})
				if endpoint == "/v1/responses" {
					h.Responses(c)
				} else {
					h.ChatCompletions(c)
				}
			})
			body := `{"model":"gpt-6-astra","messages":[{"role":"user","content":"synthetic"}],"stream":false}`
			if endpoint == "/v1/responses" {
				body = `{"model":"gpt-6-astra","input":"synthetic","stream":false}`
			}
			if tc.stream {
				body = strings.Replace(body, `"stream":false`, `"stream":true`, 1)
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(body)))
			require.Equal(t, 503, rec.Code, rec.Body.String())
			require.Equal(t, "api_error", gjson.GetBytes(rec.Body.Bytes(), "error.type").String())
			require.Equal(t, "no_available_accounts", gjson.GetBytes(rec.Body.Bytes(), "error.code").String())
			require.Equal(t, "Service temporarily unavailable", gjson.GetBytes(rec.Body.Bytes(), "error.message").String())
			require.Empty(t, rec.Header().Get("Retry-After"))
			require.Zero(t, upstream.calls)
			require.Empty(t, usage.created)
			require.Empty(t, billing.commands)
		})
	}
}

func TestNoAccountSelectionCodeRequiresExactPreDispatchSentinel(t *testing.T) {
	unavailable := noAccountErrorClassification{Status: 503, ErrType: "api_error"}
	for _, tc := range []struct {
		name string
		err  error
		cls  noAccountErrorClassification
		want string
	}{
		{"empty", service.ErrNoAvailableAccounts, unavailable, "no_available_accounts"},
		{"wrapped", fmt.Errorf("private upstream detail: %w", service.ErrNoAvailableAccounts), unavailable, "no_available_accounts"},
		{"nil", nil, unavailable, ""},
		{"same_text_not_sentinel", errors.New(service.ErrNoAvailableAccounts.Error()), unavailable, ""},
		{"database_failure", errors.New("database unavailable"), unavailable, ""},
		{"compact", service.ErrNoAvailableCompactAccounts, unavailable, ""},
		{"rate_limit", service.ErrNoAvailableAccounts, noAccountErrorClassification{Status: 429, ErrType: "rate_limit_error"}, ""},
		{"model_unsupported", service.ErrNoAvailableAccounts, noAccountErrorClassification{Status: 404, ErrType: "model_not_found", ModelNotFound: true}, ""},
		{"upstream_503", service.ErrNoAvailableAccounts, noAccountErrorClassification{Status: 503, ErrType: "upstream_error"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, noAccountSelectionErrorCode(tc.err, tc.cls)) })
	}
}
