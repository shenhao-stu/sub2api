//go:build unit

package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func legacyGrokCooldownAccount(id int64, team string) Account {
	return Account{
		ID: id, Platform: PlatformGrok, Type: AccountTypeOAuth,
		Status: StatusActive, Schedulable: true, Concurrency: 1,
		Credentials: map[string]any{
			"team_id":       team,
			"model_mapping": map[string]any{"public-alias": "grok-4.6", "grok-4.7": "grok-4.7"},
		},
	}
}

func setLegacyGrokTestCooldown(t *testing.T, account *Account, kind string) func() {
	t.Helper()
	model := "grok-4.6"
	quotaKey := grokModelQuotaBlockKey(account.ID, model)
	teamKey := grokTeamModelRateLimitKey(grokTeamFingerprint(accountGrokTeamID(account)), model)
	t.Cleanup(func() {
		globalGrokModelQuotaBlocks.mu.Lock()
		delete(globalGrokModelQuotaBlocks.items, quotaKey)
		globalGrokModelQuotaBlocks.mu.Unlock()
		globalGrokTeamModelRateLimits.mu.Lock()
		delete(globalGrokTeamModelRateLimits.items, teamKey)
		globalGrokTeamModelRateLimits.mu.Unlock()
	})
	until := time.Now().Add(time.Minute)
	if kind == "quota" {
		markGrokModelQuotaBlock(account.ID, model, until)
	} else {
		markGrokTeamModelRateLimit(account, model, until)
	}
	return func() {
		expired := time.Now().Add(-time.Second)
		if kind == "quota" {
			globalGrokModelQuotaBlocks.mu.Lock()
			globalGrokModelQuotaBlocks.items[quotaKey] = grokModelQuotaBlock{Until: expired}
			globalGrokModelQuotaBlocks.mu.Unlock()
		} else {
			globalGrokTeamModelRateLimits.mu.Lock()
			globalGrokTeamModelRateLimits.items[teamKey] = grokTeamModelRateLimit{Until: expired}
			globalGrokTeamModelRateLimits.mu.Unlock()
		}
	}
}

func TestGrokLegacyCooldownRuntimeScope(t *testing.T) {
	for i, kind := range []string{"quota", "team"} {
		t.Run(kind, func(t *testing.T) {
			account := legacyGrokCooldownAccount(int64(925600+i), t.Name())
			expire := setLegacyGrokTestCooldown(t, &account, kind)
			svc := &OpenAIGatewayService{}
			require.True(t, svc.isOpenAIAccountRequestRuntimeBlocked(&account, "public-alias"))
			require.True(t, svc.isOpenAIAccountRequestRuntimeBlocked(&account, "grok-4.6"))
			require.False(t, svc.isOpenAIAccountRequestRuntimeBlocked(&account, "grok-4.7"))
			sibling := legacyGrokCooldownAccount(account.ID+100, accountGrokTeamID(&account))
			require.Equal(t, kind == "team", svc.isOpenAIAccountRequestRuntimeBlocked(&sibling, "public-alias"))
			other := legacyGrokCooldownAccount(account.ID+200, "another-"+t.Name())
			require.False(t, svc.isOpenAIAccountRequestRuntimeBlocked(&other, "public-alias"))
			nonGrok := account
			nonGrok.Platform = PlatformOpenAI
			require.False(t, svc.isOpenAIAccountRequestRuntimeBlocked(&nonGrok, "public-alias"))
			expire()
			require.False(t, svc.isOpenAIAccountRequestRuntimeBlocked(&account, "public-alias"))
			require.Nil(t, account.RateLimitResetAt)
			require.Nil(t, account.TempUnschedulableUntil)
			require.True(t, account.Schedulable)
		})
	}
}

func TestGrokLegacyCooldownSelection(t *testing.T) {
	for kindIndex, kind := range []string{"quota", "team"} {
		for _, loadBatch := range []bool{false, true} {
			for _, sticky := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/load=%t/sticky=%t", kind, loadBatch, sticky), func(t *testing.T) {
					blocked := legacyGrokCooldownAccount(int64(925610+kindIndex), t.Name())
					healthy := legacyGrokCooldownAccount(blocked.ID+100, "other-"+t.Name())
					healthy.Priority = 10
					setLegacyGrokTestCooldown(t, &blocked, kind)
					acquired := []int64{}
					svc := newLegacySchedulerDecisionTestService([]Account{blocked, healthy}, loadBatch,
						schedulerTestConcurrencyCache{acquiredIDs: &acquired})
					ctx := context.Background()
					session := ""
					if sticky {
						session = "legacy-cooldown"
						require.NoError(t, svc.setStickySessionAccountID(ctx, nil, session, blocked.ID, time.Hour))
					}
					selection, err := svc.selectAccountWithLoadAwareness(ctx, nil, PlatformGrok, session,
						"public-alias", nil, false, "", false)
					require.NoError(t, err)
					require.NotNil(t, selection)
					defer releaseLegacySchedulerDecisionSelection(selection)
					require.Equal(t, healthy.ID, selection.Account.ID)
					require.NotContains(t, acquired, blocked.ID, "quota rejection must precede slot acquisition")
				})
			}
		}
	}
}

func TestGrokLegacyCooldownDBRecheck(t *testing.T) {
	for i, kind := range []string{"quota", "team"} {
		t.Run(kind, func(t *testing.T) {
			account := legacyGrokCooldownAccount(int64(925620+i), t.Name())
			svc := &OpenAIGatewayService{
				accountRepo:       schedulerTestOpenAIAccountRepo{accounts: []Account{account}},
				schedulerSnapshot: &SchedulerSnapshotService{},
			}
			ctx := context.Background()
			require.NotNil(t, svc.recheckSelectedOpenAIAccountFromDB(ctx, &account, nil, PlatformGrok, "public-alias", false, ""))
			setLegacyGrokTestCooldown(t, &account, kind)
			require.Nil(t, svc.recheckSelectedOpenAIAccountFromDB(ctx, &account, nil, PlatformGrok, "public-alias", false, ""))
			require.NotNil(t, svc.recheckSelectedOpenAIAccountFromDB(ctx, &account, nil, PlatformGrok, "grok-4.7", false, ""))
		})
	}
}
