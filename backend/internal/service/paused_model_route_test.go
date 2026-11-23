//go:build unit

package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type pausedRouteTestRepo struct {
	mockAccountRepoForPlatform
	paused *Account
}

func (r *pausedRouteTestRepo) GetByIDs(_ context.Context, ids []int64) ([]*Account, error) {
	for _, id := range ids {
		if id == r.paused.ID {
			return []*Account{r.paused}, nil
		}
	}
	return nil, nil
}
func TestPausedAnthropicRouteDiagnosis(t *testing.T) {
	const model = "claude-fable-5-2"
	group := &Group{ID: 25, Platform: PlatformAnthropic, Status: StatusActive, Hydrated: true, ModelRoutingEnabled: true, ModelRouting: map[string][]int64{model: {7}}}
	repo := &pausedRouteTestRepo{paused: &Account{ID: 7, Status: StatusActive, Platform: PlatformAnthropic, Type: AccountTypeAPIKey, AccountGroups: []AccountGroup{{GroupID: group.ID}}, Credentials: map[string]any{"model_mapping": map[string]any{model: model}}}}
	svc := &GatewayService{accountRepo: repo, cfg: testConfig()}
	ctx := svc.withGroupContext(t.Context(), group)
	check := func(ctx context.Context, model string, ex map[int64]struct{}) bool {
		return svc.DiagnoseModelAvailabilityExcluding(ctx, &group.ID, model, PlatformAnthropic, ex).HasPausedRoute
	}
	require.True(t, check(ctx, model, nil))
	require.False(t, check(ctx, "missing", nil))
	require.False(t, check(ctx, model, map[int64]struct{}{7: {}}))
	repo.paused.Extra = map[string]any{"affinity_tags": []string{"getoken:134"}}
	require.False(t, check(ctx, model, nil))
	owner := WithAccountAffinityTags(ctx, []string{"getoken:134"})
	require.True(t, check(owner, model, nil))
	repo.paused.AccountGroups[0].GroupID = 31
	require.False(t, check(owner, model, nil))
	repo.paused.AccountGroups[0].GroupID = group.ID
	repo.paused.Status = StatusError
	require.False(t, check(owner, model, nil))
}
