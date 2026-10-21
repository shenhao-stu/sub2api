//go:build unit

package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAffinityTagFromUserID(t *testing.T) {
	for _, tt := range []struct {
		name   string
		prefix string
		raw    string
		want   string
		ok     bool
	}{
		{"numeric", "getoken:", "123", "getoken:123", true},
		{"trimmed", "getoken:", "  7 ", "getoken:7", true},
		{"empty prefix uses default", "", "9", "getoken:9", true},
		{"custom prefix", "tenant:", "42", "tenant:42", true},
		{"empty", "getoken:", "", "", false},
		{"alpha", "getoken:", "abc", "", false},
		{"mixed", "getoken:", "12a", "", false},
		{"negative", "getoken:", "-1", "", false},
		{"too long", "getoken:", "123456789012345678901", "", false},
		{"injection", "getoken:", "1 OR 1=1", "", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := AffinityTagFromUserID(tt.prefix, tt.raw)
			require.Equal(t, tt.ok, ok)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestAccountAffinityTags_ExtraShapes(t *testing.T) {
	// JSON 反序列化出的 []any
	var extra map[string]any
	require.NoError(t, json.Unmarshal([]byte(`{"affinity_tags":["getoken:1"," getoken:2 ","getoken:1",""],"anthropic_passthrough":true}`), &extra))
	acc := &Account{ID: 1, Extra: extra}
	require.Equal(t, []string{"getoken:1", "getoken:2"}, acc.AffinityTags())
	require.True(t, acc.IsAffinityReserved())
	require.True(t, acc.MatchesAffinityTags([]string{"getoken:2"}))
	require.False(t, acc.MatchesAffinityTags([]string{"getoken:3"}))
	require.False(t, acc.MatchesAffinityTags(nil))

	// []string
	acc = &Account{ID: 2, Extra: map[string]any{AffinityTagsExtraKey: []string{"getoken:5"}}}
	require.Equal(t, []string{"getoken:5"}, acc.AffinityTags())

	// 单字符串容错
	acc = &Account{ID: 3, Extra: map[string]any{AffinityTagsExtraKey: "getoken:6"}}
	require.Equal(t, []string{"getoken:6"}, acc.AffinityTags())

	// 非法类型 / 空数组 / 缺失 / nil 账号 → 开放账号
	for _, a := range []*Account{
		{ID: 4, Extra: map[string]any{AffinityTagsExtraKey: 123}},
		{ID: 5, Extra: map[string]any{AffinityTagsExtraKey: []any{}}},
		{ID: 6, Extra: map[string]any{AffinityTagsExtraKey: []any{1, 2}}},
		{ID: 7, Extra: map[string]any{"anthropic_passthrough": true}},
		{ID: 8},
		nil,
	} {
		require.Nil(t, a.AffinityTags())
		require.False(t, a.IsAffinityReserved())
	}
}

func TestAccountAffinityTags_CapsCount(t *testing.T) {
	tags := make([]any, 0, 40)
	for i := 0; i < 40; i++ {
		tags = append(tags, "getoken:"+string(rune('a'+i%26))+string(rune('a'+i/26)))
	}
	acc := &Account{Extra: map[string]any{AffinityTagsExtraKey: tags}}
	require.Len(t, acc.AffinityTags(), maxAccountAffinityTagsPerAccount)
}

func TestWithAccountAffinityTags_Context(t *testing.T) {
	require.Nil(t, AccountAffinityTagsFromContext(context.Background()))
	require.Nil(t, AccountAffinityTagsFromContext(nil))

	ctx := WithAccountAffinityTags(context.Background(), []string{" getoken:1 ", "", "getoken:1", "getoken:2"})
	require.Equal(t, []string{"getoken:1", "getoken:2"}, AccountAffinityTagsFromContext(ctx))

	// 空集合不污染 context
	ctx = WithAccountAffinityTags(context.Background(), []string{"", "  "})
	require.Nil(t, AccountAffinityTagsFromContext(ctx))
}

func affinityTestAccounts() []Account {
	return []Account{
		{ID: 10, Priority: 1, Extra: map[string]any{AffinityTagsExtraKey: []any{"getoken:1"}}},
		{ID: 11, Priority: 1, Extra: map[string]any{AffinityTagsExtraKey: []any{"getoken:2"}}},
		{ID: 12, Priority: 1, Extra: map[string]any{"anthropic_passthrough": true}},
		{ID: 13, Priority: 2},
	}
}

func affinityAccountIDs(accounts []Account) []int64 {
	ids := make([]int64, 0, len(accounts))
	for i := range accounts {
		ids = append(ids, accounts[i].ID)
	}
	return ids
}

func TestFilterAccountsByAffinityVisibility(t *testing.T) {
	t.Run("无标签请求只见开放账号", func(t *testing.T) {
		got := filterAccountsByAffinityVisibility(context.Background(), affinityTestAccounts())
		require.Equal(t, []int64{12, 13}, affinityAccountIDs(got))
	})
	t.Run("匹配标签请求见自己的保留账号加开放账号", func(t *testing.T) {
		ctx := WithAccountAffinityTags(context.Background(), []string{"getoken:1"})
		got := filterAccountsByAffinityVisibility(ctx, affinityTestAccounts())
		require.Equal(t, []int64{10, 12, 13}, affinityAccountIDs(got))
	})
	t.Run("不匹配标签请求看不到任何保留账号", func(t *testing.T) {
		ctx := WithAccountAffinityTags(context.Background(), []string{"getoken:999"})
		got := filterAccountsByAffinityVisibility(ctx, affinityTestAccounts())
		require.Equal(t, []int64{12, 13}, affinityAccountIDs(got))
	})
	t.Run("无保留账号时原样返回同一切片", func(t *testing.T) {
		in := []Account{{ID: 1}, {ID: 2}}
		got := filterAccountsByAffinityVisibility(context.Background(), in)
		require.Equal(t, affinityAccountIDs(in), affinityAccountIDs(got))
		require.Len(t, got, 2)
	})
	t.Run("只有保留账号且不匹配 → 空", func(t *testing.T) {
		in := []Account{{ID: 1, Extra: map[string]any{AffinityTagsExtraKey: []any{"getoken:1"}}}}
		got := filterAccountsByAffinityVisibility(context.Background(), in)
		require.Empty(t, got)
	})
	t.Run("空输入", func(t *testing.T) {
		require.Empty(t, filterAccountsByAffinityVisibility(context.Background(), nil))
	})
}

func TestFilterByAffinityPreference(t *testing.T) {
	accounts := affinityTestAccounts()
	withLoad := make([]accountWithLoad, 0, len(accounts))
	for i := range accounts {
		withLoad = append(withLoad, accountWithLoad{account: &accounts[i], loadInfo: &AccountLoadInfo{AccountID: accounts[i].ID}})
	}

	t.Run("无标签原样", func(t *testing.T) {
		got := filterByAffinityPreference(context.Background(), withLoad)
		require.Len(t, got, 4)
	})
	t.Run("匹配只留保留账号", func(t *testing.T) {
		ctx := WithAccountAffinityTags(context.Background(), []string{"getoken:2"})
		got := filterByAffinityPreference(ctx, withLoad)
		require.Len(t, got, 1)
		require.Equal(t, int64(11), got[0].account.ID)
	})
	t.Run("匹配账号已被移除后回到全体", func(t *testing.T) {
		ctx := WithAccountAffinityTags(context.Background(), []string{"getoken:2"})
		rest := []accountWithLoad{withLoad[2], withLoad[3]}
		got := filterByAffinityPreference(ctx, rest)
		require.Len(t, got, 2)
	})
	t.Run("单元素直接返回", func(t *testing.T) {
		ctx := WithAccountAffinityTags(context.Background(), []string{"getoken:2"})
		got := filterByAffinityPreference(ctx, withLoad[:1])
		require.Len(t, got, 1)
	})
}

func TestStablePreferAffinityAccounts(t *testing.T) {
	accounts := affinityTestAccounts()
	ptrs := []*Account{&accounts[3], &accounts[2], &accounts[1], &accounts[0]}

	stablePreferAffinityAccounts(context.Background(), ptrs)
	require.Equal(t, []int64{13, 12, 11, 10}, []int64{ptrs[0].ID, ptrs[1].ID, ptrs[2].ID, ptrs[3].ID}, "无标签不改顺序")

	ctx := WithAccountAffinityTags(context.Background(), []string{"getoken:1"})
	stablePreferAffinityAccounts(ctx, ptrs)
	require.Equal(t, []int64{10, 13, 12, 11}, []int64{ptrs[0].ID, ptrs[1].ID, ptrs[2].ID, ptrs[3].ID}, "匹配账号前移，其余相对顺序不变")

	withLoad := []accountWithLoad{{account: &accounts[2]}, {account: &accounts[0]}}
	stablePreferAffinityAccountsWithLoad(ctx, withLoad)
	require.Equal(t, int64(10), withLoad[0].account.ID)
}

func TestAccountAffinitySettings_NormalizeAndTrust(t *testing.T) {
	s := &AccountAffinitySettings{Enabled: true, TrustedAPIKeyIDs: []int64{321, 214, 0, -3, 214, 320}, TagPrefix: "  "}
	s.Normalize()
	require.Equal(t, []int64{214, 320, 321}, s.TrustedAPIKeyIDs)
	require.Equal(t, "getoken:", s.TagPrefix)
	require.True(t, s.TrustsAPIKey(214))
	require.False(t, s.TrustsAPIKey(1))
	require.False(t, s.TrustsAPIKey(0))

	disabled := &AccountAffinitySettings{Enabled: false, TrustedAPIKeyIDs: []int64{214}}
	require.False(t, disabled.TrustsAPIKey(214), "关闭时任何 key 都不被信任")

	var nilSettings *AccountAffinitySettings
	nilSettings.Normalize()

	def := DefaultAccountAffinitySettings()
	require.False(t, def.Enabled)
	require.Empty(t, def.TrustedAPIKeyIDs)
	require.Equal(t, "getoken:", def.TagPrefix)
}
