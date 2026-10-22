//go:build unit

package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var errAffinityLoadBatch = errors.New("load batch unavailable")

func timeNowPlusMinutes(m int) time.Time {
	return time.Now().Add(time.Duration(m) * time.Minute)
}

// affinityScheduleFixture：负载感知路径（load_batch + 并发服务），无分组。
// 账号 1 为保留账号（getoken:1，并发 1），账号 2 为开放账号（并发 100）。
// 两者优先级相同，开放账号 LastUsedAt 更早——若没有亲和偏好，LRU 会选中开放账号。
type affinityScheduleFixture struct {
	svc              *GatewayService
	repo             *mockAccountRepoForPlatform
	cache            *mockGatewayCacheForPlatform
	concurrencyCache *mockConcurrencyCache
}

func newAffinityScheduleFixture(sessionBindings map[string]int64) *affinityScheduleFixture {
	repo := &mockAccountRepoForPlatform{
		accounts: []Account{
			{
				ID: 1, Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Priority: 1, Status: StatusActive, Schedulable: true, Concurrency: 1,
				Extra: map[string]any{AffinityTagsExtraKey: []any{"getoken:1"}, "anthropic_passthrough": true},
			},
			{
				ID: 2, Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Priority: 1, Status: StatusActive, Schedulable: true, Concurrency: 100,
				Extra: map[string]any{"anthropic_passthrough": true},
			},
		},
		accountsByID: map[int64]*Account{},
	}
	for i := range repo.accounts {
		repo.accountsByID[repo.accounts[i].ID] = &repo.accounts[i]
	}
	if sessionBindings == nil {
		sessionBindings = map[string]int64{}
	}
	cache := &mockGatewayCacheForPlatform{sessionBindings: sessionBindings}
	cfg := testConfig()
	cfg.Gateway.Scheduling.LoadBatchEnabled = true
	concurrencyCache := &mockConcurrencyCache{}
	svc := &GatewayService{
		accountRepo:        repo,
		cache:              cache,
		cfg:                cfg,
		concurrencyService: NewConcurrencyService(concurrencyCache),
	}
	return &affinityScheduleFixture{svc: svc, repo: repo, cache: cache, concurrencyCache: concurrencyCache}
}

const affinityTestModel = "claude-opus-5-5"

func TestSelectAccountWithLoadAwareness_Affinity_UntaggedRequestNeverSeesReservedAccount(t *testing.T) {
	f := newAffinityScheduleFixture(nil)
	// 开放账号 2 槽位满：无标签请求宁可排队也不能落到保留账号 1
	f.concurrencyCache.acquireResults = map[int64]bool{2: false}

	result, err := f.svc.SelectAccountWithLoadAwareness(context.Background(), nil, "", affinityTestModel, nil, "", 0)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, int64(2), result.Account.ID)
	require.False(t, result.Acquired)
	require.NotNil(t, result.WaitPlan, "只能在开放账号上排队")
	require.Equal(t, int64(2), result.WaitPlan.AccountID)
}

func TestSelectAccountWithLoadAwareness_Affinity_UntaggedRequestWithOnlyReservedAccountsGetsNoAccounts(t *testing.T) {
	f := newAffinityScheduleFixture(nil)
	f.repo.accounts = f.repo.accounts[:1] // 只剩保留账号
	result, err := f.svc.SelectAccountWithLoadAwareness(context.Background(), nil, "", affinityTestModel, nil, "", 0)
	require.ErrorIs(t, err, ErrNoAvailableAccounts)
	require.Nil(t, result)
	require.Equal(t, 0, f.concurrencyCache.acquireAccountCalls)
}

func TestSelectAccountWithLoadAwareness_Affinity_MismatchedTagIsTreatedAsUntagged(t *testing.T) {
	f := newAffinityScheduleFixture(nil)
	ctx := WithAccountAffinityTags(context.Background(), []string{"getoken:999"})
	result, err := f.svc.SelectAccountWithLoadAwareness(ctx, nil, "", affinityTestModel, nil, "", 0)
	require.NoError(t, err)
	require.Equal(t, int64(2), result.Account.ID)
}

func TestSelectAccountWithLoadAwareness_Affinity_TaggedRequestPrefersReservedAccount(t *testing.T) {
	f := newAffinityScheduleFixture(nil)
	ctx := WithAccountAffinityTags(context.Background(), []string{"getoken:1"})
	result, err := f.svc.SelectAccountWithLoadAwareness(ctx, nil, "session-a", affinityTestModel, nil, "", 0)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, int64(1), result.Account.ID)
	require.True(t, result.Acquired)
	require.Equal(t, int64(1), f.cache.sessionBindings["session-a"], "粘性会话应绑定到保留账号")
}

func TestSelectAccountWithLoadAwareness_Affinity_TaggedRequestFallsBackWhenReservedBusy(t *testing.T) {
	f := newAffinityScheduleFixture(nil)
	// 保留账号并发 1 已被占用：负载率 100 → 不进 available；直接落开放账号，不排队
	f.concurrencyCache.loadMap = map[int64]*AccountLoadInfo{
		1: {AccountID: 1, CurrentConcurrency: 1, LoadRate: 100},
	}
	ctx := WithAccountAffinityTags(context.Background(), []string{"getoken:1"})
	result, err := f.svc.SelectAccountWithLoadAwareness(ctx, nil, "", affinityTestModel, nil, "", 0)
	require.NoError(t, err)
	require.Equal(t, int64(2), result.Account.ID)
	require.True(t, result.Acquired)
}

func TestSelectAccountWithLoadAwareness_Affinity_TaggedRequestFallsBackWhenReservedSlotRace(t *testing.T) {
	f := newAffinityScheduleFixture(nil)
	// 负载读数未满但抢槽失败（竞态）：移除保留账号后下一轮回到开放账号
	f.concurrencyCache.acquireResults = map[int64]bool{1: false}
	ctx := WithAccountAffinityTags(context.Background(), []string{"getoken:1"})
	result, err := f.svc.SelectAccountWithLoadAwareness(ctx, nil, "", affinityTestModel, nil, "", 0)
	require.NoError(t, err)
	require.Equal(t, int64(2), result.Account.ID)
	require.True(t, result.Acquired)
}

func TestSelectAccountWithLoadAwareness_Affinity_TaggedRequestFallsBackWhenReservedRateLimited(t *testing.T) {
	f := newAffinityScheduleFixture(nil)
	reset := timeNowPlusMinutes(30)
	f.repo.accounts[0].RateLimitResetAt = &reset
	ctx := WithAccountAffinityTags(context.Background(), []string{"getoken:1"})
	result, err := f.svc.SelectAccountWithLoadAwareness(ctx, nil, "", affinityTestModel, nil, "", 0)
	require.NoError(t, err)
	require.Equal(t, int64(2), result.Account.ID, "保留账号限流中 → 回落开放账号")
}

func TestSelectAccountWithLoadAwareness_Affinity_StickyOnOpenAccountIsKept(t *testing.T) {
	// 回退期间绑定到开放账号的会话，保留账号恢复后仍沿用粘性（保护 prompt cache）
	f := newAffinityScheduleFixture(map[string]int64{"session-b": 2})
	ctx := WithAccountAffinityTags(context.Background(), []string{"getoken:1"})
	result, err := f.svc.SelectAccountWithLoadAwareness(ctx, nil, "session-b", affinityTestModel, nil, "", 0)
	require.NoError(t, err)
	require.Equal(t, int64(2), result.Account.ID)
	require.Equal(t, 0, f.concurrencyCache.loadBatchCalls, "粘性命中应在 Layer 2 之前返回")
}

func TestSelectAccountWithLoadAwareness_Affinity_UntaggedStickyToReservedAccountIsDropped(t *testing.T) {
	// 极端情况：无标签请求的会话哈希曾绑定到保留账号（例如标签配置被移除）。
	// 保留账号对它不可见，粘性失效并重绑到开放账号。
	f := newAffinityScheduleFixture(map[string]int64{"session-c": 1})
	result, err := f.svc.SelectAccountWithLoadAwareness(context.Background(), nil, "session-c", affinityTestModel, nil, "", 0)
	require.NoError(t, err)
	require.Equal(t, int64(2), result.Account.ID)
	require.Equal(t, int64(2), f.cache.sessionBindings["session-c"])
}

func TestSelectAccountWithLoadAwareness_Affinity_AllBusyTaggedWaitsOnReserved(t *testing.T) {
	f := newAffinityScheduleFixture(nil)
	f.concurrencyCache.acquireResults = map[int64]bool{1: false, 2: false}
	ctx := WithAccountAffinityTags(context.Background(), []string{"getoken:1"})
	result, err := f.svc.SelectAccountWithLoadAwareness(ctx, nil, "", affinityTestModel, nil, "", 0)
	require.NoError(t, err)
	require.False(t, result.Acquired)
	require.NotNil(t, result.WaitPlan)
	require.Equal(t, int64(1), result.WaitPlan.AccountID, "全员槽位满时带标签请求在自己的保留账号上排队")
}

func TestSelectAccountWithLoadAwareness_Affinity_LegacyOrderFallbackPrefersReserved(t *testing.T) {
	f := newAffinityScheduleFixture(nil)
	f.concurrencyCache.loadBatchErr = errAffinityLoadBatch
	ctx := WithAccountAffinityTags(context.Background(), []string{"getoken:1"})
	result, err := f.svc.SelectAccountWithLoadAwareness(ctx, nil, "", affinityTestModel, nil, "", 0)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Account.ID)
}
