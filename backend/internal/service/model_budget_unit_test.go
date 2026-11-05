//go:build unit

package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestModelBudgetCooldownUsesCanonicalModelWithoutChangingGlobalState(t *testing.T) {
	repo := &modelCooldownRepo{}
	s := newForwardPartialUsageServiceForTest(nil)
	s.rateLimitService.accountRepo = repo
	s.httpUpstream = &queuedHTTPUpstreamStub{responses: []*http.Response{{StatusCode: 402, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(budgetTestBody("claude-opus-5-5", false)))}}}
	a := newAnthropicAPIKeyAccountForTest()
	a.Credentials["model_mapping"] = map[string]any{"public-opus": "claude-opus-5-5", "claude-opus-5-5": "must-not-remap"}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
	parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(`{"model":"public-opus","messages":[{"role":"user","content":"synthetic"}]}`)), PlatformAnthropic)
	require.NoError(t, err)
	_, err = s.Forward(context.Background(), c, a, parsed)
	require.Error(t, err)
	require.Equal(t, 402, c.Writer.Status())
	require.Equal(t, 1, repo.modelRateLimitCalls)
	require.Equal(t, "claude-opus-5-5", repo.lastModelRateLimitScope)
	require.WithinDuration(t, time.Now().Add(time.Minute), repo.lastModelRateLimitReset, 2*time.Second)
	require.Zero(t, repo.rateLimitCalls)
	require.Zero(t, repo.tempUnschedCalls)
	require.Nil(t, a.RateLimitResetAt)
	require.Nil(t, a.TempUnschedulableUntil)
	require.True(t, a.Schedulable)
	// A disconnected client must not discard the cooldown acknowledgement.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.rateLimitService.persistModelBudgetCooldown(ctx, a, "claude-opus-5-5")
	require.NoError(t, repo.ctxErr)
	require.Equal(t, 2, repo.modelRateLimitCalls)
}

func TestModelBudgetStickySelectionSkipsOnlyCooledModel(t *testing.T) {
	for _, tc := range []struct {
		name, model string
		delta       time.Duration
		schedulable bool
		want        int64
	}{
		{"opus-cooled", "claude-opus-5-5", time.Minute, true, 2},
		{"sonnet-unchanged", "claude-sonnet-5-5", time.Minute, true, 1},
		{"expired-opus", "claude-opus-5-5", -time.Minute, true, 1},
		{"stopped-opus-stays-stopped", "claude-opus-5-5", -time.Minute, false, 2},
		{"stopped-sonnet-stays-stopped", "claude-sonnet-5-5", time.Minute, false, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := Account{ID: 1, Type: AccountTypeAPIKey, Platform: PlatformAnthropic, Priority: 100, Status: StatusActive, Schedulable: tc.schedulable,
				Extra: map[string]any{"model_rate_limits": map[string]any{"claude-opus-5-5": map[string]any{"rate_limit_reset_at": time.Now().Add(tc.delta).Format(time.RFC3339)}}}}
			repo := &mockAccountRepoForPlatform{accounts: []Account{a, {ID: 2, Type: AccountTypeAPIKey, Platform: PlatformAnthropic, Priority: 0, Status: StatusActive, Schedulable: true}}, accountsByID: map[int64]*Account{}}
			for i := range repo.accounts {
				repo.accountsByID[repo.accounts[i].ID] = &repo.accounts[i]
			}
			cache := &mockGatewayCacheForPlatform{sessionBindings: map[string]int64{"synthetic-session": 1}}
			svc := &GatewayService{accountRepo: repo, cache: cache, cfg: testConfig()}
			acc, err := svc.selectAccountForModelWithPlatform(context.Background(), nil, "synthetic-session", tc.model, nil, PlatformAnthropic)
			require.NoError(t, err)
			require.Equal(t, tc.want, acc.ID)
			require.Equal(t, tc.schedulable, repo.accountsByID[1].Schedulable)
			require.Nil(t, repo.accountsByID[1].TempUnschedulableUntil)
		})
	}
}
