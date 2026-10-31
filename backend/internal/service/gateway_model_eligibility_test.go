//go:build unit

package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func newModelEligibilityFixture() *affinityScheduleFixture {
	f := newAffinityScheduleFixture(nil)
	f.repo.accounts[0].Extra = map[string]any{"anthropic_passthrough": true}
	f.repo.accounts[0].Priority = 10
	f.repo.accounts[0].Credentials = map[string]any{"model_mapping": map[string]any{"claude-fable-5-1": "claude-fable-5-1"}}
	f.repo.accounts[1].Priority = 0
	f.repo.accounts[1].Credentials = map[string]any{"model_mapping": map[string]any{"claude-opus-5-5": "claude-opus-5-5", "claude-sonnet-5-5": "claude-sonnet-5-5"}}
	return f
}

func TestModelEligibilityExplicitMappingSurvivesSelectionPaths(t *testing.T) {
	for _, path := range []string{"fresh", "sticky-pro", "legacy", "load-error", "busy", "provider-retry-excluded", "disabled"} {
		t.Run(path, func(t *testing.T) {
			f := newModelEligibilityFixture()
			var excluded map[int64]struct{}
			session := ""
			switch path {
			case "sticky-pro":
				session = "prior-opus"
				f.cache.sessionBindings[session] = 2
			case "legacy":
				f.svc.cfg.Gateway.Scheduling.LoadBatchEnabled = false
			case "load-error":
				f.concurrencyCache.loadBatchErr = errAffinityLoadBatch
			case "busy":
				f.concurrencyCache.acquireResults = map[int64]bool{1: false}
			case "provider-retry-excluded":
				excluded = map[int64]struct{}{1: {}}
			case "disabled":
				f.repo.accounts[0].Schedulable = false
			}
			selected, err := f.svc.SelectAccountWithLoadAwareness(context.Background(), nil, session, "claude-fable-5-1", excluded, "", 0)
			if path == "provider-retry-excluded" || path == "disabled" {
				require.ErrorIs(t, err, ErrNoAvailableAccounts)
				require.Nil(t, selected, "Fable must not escape to an available Pro account")
				return
			}
			require.NoError(t, err)
			require.Equal(t, int64(1), selected.Account.ID, "only the explicit Fable account may be selected")
			if selected.ReleaseFunc != nil {
				selected.ReleaseFunc()
			}
			if path == "busy" {
				require.NotNil(t, selected.WaitPlan)
				require.Equal(t, int64(1), selected.WaitPlan.AccountID)
			}
		})
	}
}

func TestModelEligibilityConfigurationSemantics(t *testing.T) {
	for _, test := range []struct {
		name        string
		credentials map[string]any
		supported   bool
	}{
		{"explicit-pro-allowlist", map[string]any{"model_mapping": map[string]any{"claude-opus-5-5": "claude-opus-5-5"}}, false},
		{"empty-mapping-allows-all", map[string]any{}, true},
		{"models-array-is-not-the-sub-scheduler-contract", map[string]any{"models": []any{"claude-opus-5-5"}}, true},
		{"claude-wildcard-also-claims-fable", map[string]any{"model_mapping": map[string]any{"claude-*": "claude-*"}}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			account := &Account{Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Credentials: test.credentials, Extra: map[string]any{"anthropic_passthrough": true}}
			require.Equal(t, test.supported, (&GatewayService{}).isModelSupportedByAccountWithContext(context.Background(), account, "claude-fable-5-1"))
		})
	}
}

func TestModelEligibilityPreferredRouteStillHonorsAccountScope(t *testing.T) {
	for _, route := range []struct {
		name      string
		accountID int64
	}{
		{"missing-preferred-account", 33861},
		{"preferred-pro-does-not-support-fable", 2},
	} {
		t.Run(route.name, func(t *testing.T) {
			for _, availability := range []struct {
				name     string
				excluded bool
			}{
				{"reclaude-available", false},
				{"reclaude-excluded", true},
			} {
				t.Run(availability.name, func(t *testing.T) {
					f := newModelEligibilityFixture()
					groupID := int64(25)
					f.svc.groupRepo = &mockGroupRepoForGateway{groups: map[int64]*Group{
						25: {ID: 25, Platform: PlatformAnthropic, Status: StatusActive, Hydrated: true,
							ModelRoutingEnabled: true, ModelRouting: map[string][]int64{"claude-fable-*": {route.accountID}}},
					}}
					for i := range f.repo.accounts {
						f.repo.accounts[i].GroupIDs = []int64{25}
					}
					var excluded map[int64]struct{}
					if availability.excluded {
						excluded = map[int64]struct{}{1: {}}
					}
					selected, err := f.svc.SelectAccountWithLoadAwareness(context.Background(), &groupID, "", "claude-fable-5-1", excluded, "", 0)
					if availability.excluded {
						require.ErrorIs(t, err, ErrNoAvailableAccounts)
						require.Nil(t, selected)
					} else {
						require.NoError(t, err)
						require.Equal(t, int64(1), selected.Account.ID)
						if selected.ReleaseFunc != nil {
							selected.ReleaseFunc()
						}
					}
				})
			}
		})
	}
}
