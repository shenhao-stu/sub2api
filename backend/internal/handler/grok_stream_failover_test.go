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
	"github.com/tidwall/gjson"
)

type grokStreamFailoverAccountRepo struct {
	openAIWSFailoverHandlerAccountRepoStub
}

func (s *grokStreamFailoverAccountRepo) UpdateExtra(context.Context, int64, map[string]any) error {
	return nil
}

type grokStreamFailoverRecorder struct {
	*httptest.ResponseRecorder
	heartbeat chan struct{}
}

func (r *grokStreamFailoverRecorder) Write(p []byte) (int, error) {
	n, err := r.ResponseRecorder.Write(p)
	if n == len(p) && string(p) == ":\n\n" {
		select {
		case r.heartbeat <- struct{}{}:
		default:
		}
	}
	return n, err
}

type grokStreamFailoverUpstream struct {
	service.HTTPUpstream
	accountIDs []int64
	heartbeat  <-chan struct{}
	allFail    bool
}

func (u *grokStreamFailoverUpstream) Do(_ *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	u.accountIDs = append(u.accountIDs, accountID)
	body := `data: {"type":"response.completed","sequence_number":0,"response":{"id":"resp_healthy","object":"response","created_at":1,"status":"completed","model":"grok-4.6","output":[],"usage":{"input_tokens":1,"output_tokens":1}}}` + "\n\n"
	reader := io.NopCloser(strings.NewReader(body))
	if accountID == 9920 || u.allFail {
		metadata := fmt.Sprintf("event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_failed_%d_%d\"}}\n\n", accountID, len(u.accountIDs))
		failure := "event: error\ndata: {\"error\":{\"code\":\"rate_limit_exceeded\",\"message\":\"Rate limit exceeded\"}}\n\n"
		if len(u.accountIDs) == 1 {
			pipeReader, pipeWriter := io.Pipe()
			reader = pipeReader
			go func() {
				defer func() { _ = pipeWriter.Close() }()
				if _, err := io.WriteString(pipeWriter, metadata); err != nil {
					return
				}
				select {
				case <-u.heartbeat:
					_, _ = io.WriteString(pipeWriter, failure)
				case <-time.After(5 * time.Second):
					_ = pipeWriter.CloseWithError(fmt.Errorf("downstream heartbeat was not written"))
				}
			}()
		} else {
			reader = io.NopCloser(strings.NewReader(metadata + failure))
		}
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       reader,
	}, nil
}

func TestGrokResponses_BareSSE429AfterHeartbeat(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name      string
		pool      bool
		allFail   bool
		wantCalls []int64
	}{
		{"pool_retry_then_healthy_account", true, false, []int64{9920, 9920, 9921}},
		{"non_pool_switches_to_healthy_account", false, false, []int64{9920, 9921}},
		{"pool_retries_then_exhausts", true, true, []int64{9920, 9920, 9921}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			accounts := []service.Account{
				{
					ID: 9920, Name: "grok-rate-limited", Platform: service.PlatformGrok,
					Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Priority: 1,
					Credentials: map[string]any{
						"api_key": "xai-first", "base_url": "https://api.x.ai/v1",
						"pool_mode": tc.pool, "pool_mode_retry_count": float64(1),
						"pool_mode_retry_status_codes": []any{float64(http.StatusTooManyRequests)},
					},
				},
				{
					ID: 9921, Name: "grok-fallback", Platform: service.PlatformGrok,
					Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Priority: 2,
					Credentials: map[string]any{"api_key": "xai-second", "base_url": "https://api.x.ai/v1"},
				},
			}
			cfg := &config.Config{RunMode: config.RunModeSimple}
			cfg.Default.RateMultiplier = 1
			cfg.Gateway.MaxAccountSwitches = 1
			cfg.Gateway.StreamKeepaliveInterval = 1
			accountRepo := &grokStreamFailoverAccountRepo{
				openAIWSFailoverHandlerAccountRepoStub: openAIWSFailoverHandlerAccountRepoStub{accounts: accounts},
			}
			rec := &grokStreamFailoverRecorder{httptest.NewRecorder(), make(chan struct{}, 1)}
			upstream := &grokStreamFailoverUpstream{heartbeat: rec.heartbeat, allFail: tc.allFail}
			billingCache := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
			t.Cleanup(billingCache.Stop)
			gateway := service.NewOpenAIGatewayService(
				accountRepo, nil, nil, nil, nil, nil, nil, cfg, nil, nil,
				service.NewBillingService(cfg, nil), nil, billingCache, upstream,
				&service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil,
			)
			h := NewOpenAIGatewayHandler(
				gateway, service.NewConcurrencyService(nil), billingCache,
				service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg,
			)
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"grok-4.6","input":"hello","stream":true}`))
			c.Request.Header.Set("Content-Type", "application/json")
			groupID := int64(4210)
			c.Set(string(middleware.ContextKeyAPIKey), &service.APIKey{
				ID: 1810, GroupID: &groupID,
				User:  &service.User{ID: 1710, Status: service.StatusActive},
				Group: &service.Group{ID: groupID, Platform: service.PlatformGrok, Status: service.StatusActive},
			})
			c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 1710})

			h.Responses(c)

			require.Equal(t, tc.wantCalls, upstream.accountIDs)
			require.Equal(t, http.StatusOK, rec.Code)
			require.Equal(t, "text/event-stream", rec.Result().Header.Get("Content-Type"))
			body := rec.Body.String()
			require.True(t, strings.HasPrefix(body, ":\n\n"), "real heartbeat must precede retry")
			require.NotContains(t, body, "resp_failed_", "metadata from failed attempts must remain private")
			var events []gjson.Result
			for _, line := range strings.Split(body, "\n") {
				if line == "" || strings.HasPrefix(line, ":") || strings.HasPrefix(line, "event:") {
					continue
				}
				require.True(t, strings.HasPrefix(line, "data: "), "SSE must not contain a JSON error body: %s", line)
				payload := strings.TrimPrefix(line, "data: ")
				require.True(t, gjson.Valid(payload))
				events = append(events, gjson.Parse(payload))
			}
			require.Len(t, events, 1, "exactly one terminal event must reach the client")
			if tc.allFail {
				require.Equal(t, "response.failed", events[0].Get("type").String())
				require.Equal(t, "rate_limit_exceeded", events[0].Get("response.error.code").String())
				require.Equal(t, "failed", events[0].Get("response.status").String())
			} else {
				require.Equal(t, "response.completed", events[0].Get("type").String())
				require.Equal(t, "resp_healthy", events[0].Get("response.id").String())
			}
		})
	}
}
