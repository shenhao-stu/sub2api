package handler

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type grokChatTimeoutUpstream struct {
	service.HTTPUpstream
	accountIDs      []int64
	allFail         bool
	cancel          context.CancelFunc
	transport       service.HTTPUpstream
	transportErrors []error
}

func (u *grokChatTimeoutUpstream) Do(req *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	u.accountIDs = append(u.accountIDs, accountID)
	if u.transport != nil {
		resp, err := u.transport.Do(req, "", accountID, 1)
		if err != nil {
			u.transportErrors = append(u.transportErrors, err)
		}
		return resp, err
	}
	if accountID == 9980 || u.allFail {
		if u.cancel != nil {
			u.cancel()
		}
		return nil, &url.Error{Op: "Post", URL: req.URL.String(), Err: context.DeadlineExceeded}
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(strings.NewReader(`{"id":"chatcmpl-recovered","object":"chat.completion","model":"grok-4.6",` +
			`"choices":[{"index":0,"message":{"role":"assistant","content":"5"},"finish_reason":"stop"}],` +
			`"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`)),
	}, nil
}

func TestGrokChatHeaderTimeoutSkipsPoolReplay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name      string
		cancel    bool
		allFail   bool
		realHTTP  bool
		wantCalls []int64
	}{
		{"switches_directly_to_healthy_account", false, false, false, []int64{9980, 9981}},
		{"client_gone_stops_failover", true, false, false, []int64{9980}},
		{"switch_budget_remains_bounded", false, true, false, []int64{9980, 9981}},
		{"real_http_header_timeout_switches_once", false, false, true, []int64{9980, 9981}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			baseURL := "https://api.x.ai/v1"
			if tc.realHTTP {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.Copy(io.Discard, r.Body)
					if r.Header.Get("Authorization") == "Bearer test-key-9980" {
						select {
						case <-r.Context().Done():
						case <-time.After(3 * time.Second):
							w.WriteHeader(http.StatusGatewayTimeout)
						}
						return
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, `{"id":"chatcmpl-recovered","object":"chat.completion","model":"grok-4.6",`+
						`"choices":[{"index":0,"message":{"role":"assistant","content":"5"},"finish_reason":"stop"}],`+
						`"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`)
				}))
				t.Cleanup(server.Close)
				baseURL = server.URL
			}
			accounts := []service.Account{}
			for index, id := range []int64{9980, 9981} {
				accounts = append(accounts, service.Account{
					ID: id, Name: "grok-timeout-test", Platform: service.PlatformGrok,
					Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Priority: index + 1,
					Credentials: map[string]any{
						"api_key": fmt.Sprintf("test-key-%d", id), "base_url": baseURL,
						"pool_mode": true, "pool_mode_retry_count": float64(3),
					},
				})
			}
			cfg := &config.Config{RunMode: config.RunModeSimple}
			cfg.Default.RateMultiplier = 1
			cfg.Gateway.MaxAccountSwitches = 1
			cfg.Gateway.GrokResponseHeaderTimeout = 1
			cfg.Gateway.GrokNonstreamResponseHeaderTimeout = 1
			cfg.Security.URLAllowlist.AllowInsecureHTTP = true
			cfg.Security.URLAllowlist.AllowPrivateHosts = true
			accountRepo := &grokStreamFailoverAccountRepo{
				openAIWSFailoverHandlerAccountRepoStub: openAIWSFailoverHandlerAccountRepoStub{accounts: accounts},
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			upstream := &grokChatTimeoutUpstream{allFail: tc.allFail}
			if tc.realHTTP {
				upstream.transport = repository.NewHTTPUpstream(cfg)
			}
			if tc.cancel {
				upstream.cancel = cancel
			}
			billingCache := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
			t.Cleanup(billingCache.Stop)
			gateway := service.NewOpenAIGatewayService(
				accountRepo, nil, nil, nil, nil, nil, nil, cfg, nil, nil,
				service.NewBillingService(cfg, nil), nil, billingCache, upstream,
				&service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil,
			)
			h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(nil), billingCache,
				service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
				strings.NewReader(`{"model":"grok-4.6","messages":[{"role":"user","content":"2+3"}],"stream":false}`)).WithContext(ctx)
			c.Request.Header.Set("Content-Type", "application/json")
			groupID := int64(4229)
			c.Set(string(middleware.ContextKeyAPIKey), &service.APIKey{
				ID: 1829, GroupID: &groupID, User: &service.User{ID: 1729, Status: service.StatusActive},
				Group: &service.Group{ID: groupID, Platform: service.PlatformGrok, Status: service.StatusActive},
			})
			c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 1729})

			h.ChatCompletions(c)

			require.Equal(t, tc.wantCalls, upstream.accountIDs)
			if tc.realHTTP {
				require.Len(t, upstream.transportErrors, 1)
				var timeout net.Error
				require.ErrorAs(t, upstream.transportErrors[0], &timeout)
				require.True(t, timeout.Timeout(), "the real HTTP transport must time out before the mock origin returns")
				require.Contains(t, timeout.Error(), "timeout awaiting response headers")
				require.NoError(t, ctx.Err(), "the upstream header deadline is independent of client cancellation")
			}
			if tc.cancel {
				require.Empty(t, rec.Body.String())
			} else if tc.allFail {
				require.Equal(t, http.StatusBadGateway, rec.Code)
				require.True(t, gjson.Get(rec.Body.String(), "error").Exists())
			} else {
				require.Equal(t, http.StatusOK, rec.Code)
				require.Equal(t, "5", gjson.Get(rec.Body.String(), "choices.0.message.content").String())
				require.Equal(t, "stop", gjson.Get(rec.Body.String(), "choices.0.finish_reason").String())
				require.Equal(t, int64(3), gjson.Get(rec.Body.String(), "usage.total_tokens").Int())
			}
		})
	}
}
