//go:build unit

package handler

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestAlphaSearchBillsCompletedCallBeforeResponseError(t *testing.T) {
	for _, cancelClient := range []bool{false, true} {
		t.Run(map[bool]string{false: "connected", true: "cancelled"}[cancelClient], func(t *testing.T) {
			h, slots, bindings, upstream := newGrokMediaSlotHandler(t, false, false)
			account := service.Account{ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
				Status: service.StatusActive, Schedulable: true, Concurrency: 50, GroupIDs: []int64{24},
				Credentials: map[string]any{"access_token": "at-fixture", "auth_mode": service.OpenAIAuthModePersonalAccessToken, "chatgpt_account_id": "fixture-account"}}
			repo := grokMediaSlotRepo{openAIImagesFailoverAccountRepo: openAIImagesFailoverAccountRepo{accounts: []service.Account{account}}}
			cfg := &config.Config{}
			cfg.Default.RateMultiplier = 1
			billing := &mediaCompletionBillingRepo{}
			billing.apply = func(ctx context.Context) error {
				require.NoError(t, ctx.Err())
				_, bounded := ctx.Deadline()
				require.True(t, bounded)
				return nil
			}
			h.gatewayService = service.NewOpenAIGatewayService(repo, nil, billing, nil, nil, nil, bindings, cfg,
				nil, service.NewConcurrencyService(slots), service.NewBillingService(cfg, nil), nil, nil, upstream,
				service.NewDeferredService(repo, nil, time.Minute), nil, nil, nil, nil, nil, nil, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			upstream.call = func(*http.Request, int64) (*http.Response, error) {
				if cancelClient {
					cancel()
				}
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(
					"data: {\"type\":\"response.web_search_call.completed\",\"item_id\":\"search-1\"}\n\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\"}}\n\n"))}, nil
			}
			c, recorder := grokMediaSlotContext(ctx, true)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/alpha/search", strings.NewReader(`{"model":"gpt-5.5","commands":{"search_query":[{"q":"fixture"}]}}`)).WithContext(ctx)
			key, _ := middleware2.GetAPIKeyFromContext(c)
			key.Group.Platform, key.Group.RateMultiplier = service.PlatformOpenAI, 1
			h.AlphaSearch(c)
			require.Equal(t, 1, upstream.calls, "a known consumed search must not be regenerated")
			require.Len(t, billing.commands, 1)
			require.Positive(t, billing.commands[0].BalanceCost)
			if cancelClient {
				require.Equal(t, statusClientClosedRequest, c.Writer.Status())
				require.Empty(t, recorder.Body.String())
			} else {
				require.Equal(t, http.StatusBadGateway, recorder.Code)
			}
		})
	}
}
