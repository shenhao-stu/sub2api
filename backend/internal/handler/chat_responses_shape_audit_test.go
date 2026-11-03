//go:build unit

package handler

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/securityaudit"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type chatShapeAuditConfig struct {
	securityaudit.ConfigStore
	active securityaudit.ActiveConfig
}

func (s chatShapeAuditConfig) Active() (securityaudit.ActiveConfig, bool) { return s.active, true }
func (s chatShapeAuditConfig) EffectiveMode() securityaudit.Mode          { return s.active.EffectiveMode() }
func (s chatShapeAuditConfig) BlockingActivationDegraded() bool           { return false }

// Run the real handler, coordinator, prompt extractor and HTTP guard scanner.
// The synthetic guard rejects a harmless marker, without calling a paid model.
func TestChatResponsesShapeCannotSkipBlockingAudit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct{ name, fields, wantText string }{
		{"missing_messages", `"input":"synthetic input marker"`, "synthetic input marker"},
		{"null_messages", `"messages":null,"input":"synthetic input marker"`, "synthetic input marker"},
		{"empty_messages", `"messages":[],"input":[{"role":"user","content":[{"type":"input_text","text":"synthetic input marker"}]}]`, "synthetic input marker"},
		{"ordinary_chat", `"messages":[{"role":"user","content":"synthetic chat marker"}],"input":"unused input marker"`, "synthetic chat marker"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var scans atomic.Int32
			guard := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				require.Contains(t, string(body), tc.wantText)
				require.NotContains(t, string(body), "unused input marker")
				scans.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"Safety: Unsafe\nCategories: Jailbreak"}}]}`)
			}))
			t.Cleanup(guard.Close)
			db, auditDB, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			// Audit persistence is a double; recording failure must not turn a
			// real blocking decision into an upstream dispatch or a charge.
			auditDB.ExpectBegin().WillReturnError(errors.New("synthetic audit storage unavailable"))
			audit := securityaudit.NewPromptService(chatShapeAuditConfig{active: securityaudit.ActiveConfig{
				RiskControlEnabled: true, Enabled: true, BlockingEnabled: true, BlockingLatestTurnOnly: true, AllGroups: true,
				Scanners: []string{"jailbreak"}, ConfigVersion: 1,
				Endpoints: []securityaudit.ActiveEndpoint{{ID: "synthetic", BaseURL: guard.URL, Model: "synthetic-guard", Enabled: true, TimeoutMS: 1000, InputLimit: 4000}},
			}}, securityaudit.NewPostgreSQLRepository(db), nil, securityaudit.NewOpenAICompatibleScanner(), nil)
			accounts := []service.Account{{ID: 19930, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Credentials: map[string]any{"api_key": "synthetic"}, Extra: map[string]any{"openai_responses_supported": true}}}
			accountRepo := &grokPartialUsageAccountRepo{grokStreamFailoverAccountRepo: grokStreamFailoverAccountRepo{openAIWSFailoverHandlerAccountRepoStub: openAIWSFailoverHandlerAccountRepoStub{accounts: accounts}}}
			repo := &chatHandlerReceiptRepo{openAIWSUsageHandlerUsageLogRepoStub: openAIWSUsageHandlerUsageLogRepoStub{created: make(chan *service.UsageLog, 3)}}
			billing := &monitorCyberBillingRepo{commands: make(chan *service.UsageBillingCommand, 3)}
			users := &monitorCyberUserRepo{}
			cfg := &config.Config{}
			cfg.Default.RateMultiplier = 1
			cache := service.NewBillingCacheService(nil, users, nil, nil, nil, nil, cfg, nil)
			t.Cleanup(cache.Stop)
			upstream := &grokMediaSlotUpstream{call: func(*http.Request, int64) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"error":{"message":"synthetic provider reached"}}`))}, nil
			}}
			gateway := service.NewOpenAIGatewayService(accountRepo, repo, billing, users, nil, nil, nil, cfg, nil, nil, service.NewBillingService(cfg, nil), nil, cache, upstream, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil)
			h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(nil), cache, service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
			h.securityAuditCoordinator = securityaudit.NewCoordinator(nil, audit)
			router := gin.New()
			router.Use(middleware.ClientRequestID())
			router.Use(func(c *gin.Context) {
				gid := int64(14230)
				c.Set(string(middleware.ContextKeyAPIKey), &service.APIKey{ID: 11830, UserID: 11730, GroupID: &gid, User: &service.User{ID: 11730, Status: service.StatusActive, Balance: 100}, Group: &service.Group{ID: gid, Hydrated: true, Platform: service.PlatformOpenAI, Status: service.StatusActive, RateMultiplier: 1}})
				c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 11730})
				c.Next()
			})
			router.POST(service.ChatReceiptEndpoint, h.ChatCompletions)
			request := httptest.NewRequest(http.MethodPost, service.ChatReceiptEndpoint, strings.NewReader(`{"model":"gemini-4-argon","stream":false,`+tc.fields+`}`))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			t.Logf("status=%d guard_calls=%d provider_calls=%d charges=%d", response.Code, scans.Load(), upstream.calls, len(billing.commands))
			require.Equal(t, 1, int(scans.Load()), "actual prompt scanner must see the forwarded input")
			require.Equal(t, http.StatusForbidden, response.Code, response.Body.String())
			require.Contains(t, response.Body.String(), securityaudit.ErrorCodeBlocked)
			require.Zero(t, upstream.calls)
			require.Zero(t, len(billing.commands))
			require.Zero(t, len(repo.created))
			require.Nil(t, repo.claim)
			require.NoError(t, auditDB.ExpectationsWereMet())
		})
	}
}
