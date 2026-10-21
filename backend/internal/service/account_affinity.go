package service

import (
	"context"
	"sort"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
)

// 账号亲和（保留账号）调度。
//
// 账号在 extra.affinity_tags 上声明一组标签后成为“保留账号”：
//   - 只有携带了匹配标签的请求才能看到它（硬排他，1 账号 : 1 标签主体）；
//   - 携带匹配标签的请求优先落到它；它忙/限流/不可调度时直接落回组内开放账号。
//
// 请求侧的标签来自可信调用方（例如 GeToken 自有池转发）在请求头中携带的用户标识，
// 由 middleware.AccountAffinity 核对 API Key 白名单后写入 context；
// 标签只参与账号选择，不改变计费归属（usage 仍记在鉴权通过的 API Key 上）。
const (
	// AffinityTagsExtraKey 账号 extra 中保存标签数组的键。
	AffinityTagsExtraKey = "affinity_tags"
	// AccountAffinityHeader 可信调用方传递用户标识的请求头。
	AccountAffinityHeader = "X-GeToken-User-Id"
	// defaultAccountAffinityTagPrefix 由用户标识派生标签时使用的默认前缀。
	defaultAccountAffinityTagPrefix = "getoken:"
	// maxAccountAffinityUserIDLength 用户标识（十进制数字）的最大长度。
	maxAccountAffinityUserIDLength = 20
	// maxAccountAffinityTagsPerAccount 单账号允许的标签上限，防止 extra 被灌垃圾。
	maxAccountAffinityTagsPerAccount = 16
)

// AccountAffinitySettings 账号亲和调度的管理端配置（settings 表 JSON）。
type AccountAffinitySettings struct {
	// Enabled 关闭时忽略所有请求标签：保留账号对所有请求不可见（安全默认）。
	Enabled bool `json:"enabled"`
	// TrustedAPIKeyIDs 允许携带标签头的 API Key ID 白名单；不在白名单的请求头被忽略。
	TrustedAPIKeyIDs []int64 `json:"trusted_api_key_ids"`
	// TagPrefix 由请求头用户标识派生标签时的前缀，默认 "getoken:"。
	TagPrefix string `json:"tag_prefix"`
}

// DefaultAccountAffinitySettings 默认关闭且无信任 key。
func DefaultAccountAffinitySettings() *AccountAffinitySettings {
	return &AccountAffinitySettings{
		Enabled:          false,
		TrustedAPIKeyIDs: []int64{},
		TagPrefix:        defaultAccountAffinityTagPrefix,
	}
}

// Normalize 去重/排序白名单、修正空前缀。
func (s *AccountAffinitySettings) Normalize() {
	if s == nil {
		return
	}
	seen := make(map[int64]struct{}, len(s.TrustedAPIKeyIDs))
	ids := make([]int64, 0, len(s.TrustedAPIKeyIDs))
	for _, id := range s.TrustedAPIKeyIDs {
		if id <= 0 {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	s.TrustedAPIKeyIDs = ids
	s.TagPrefix = strings.TrimSpace(s.TagPrefix)
	if s.TagPrefix == "" {
		s.TagPrefix = defaultAccountAffinityTagPrefix
	}
}

// TrustsAPIKey 判断 API Key 是否在白名单内（关闭状态下任何 key 都不被信任）。
func (s AccountAffinitySettings) TrustsAPIKey(apiKeyID int64) bool {
	if !s.Enabled || apiKeyID <= 0 {
		return false
	}
	for _, id := range s.TrustedAPIKeyIDs {
		if id == apiKeyID {
			return true
		}
	}
	return false
}

// AffinityTagFromUserID 把请求头里的用户标识转成标签。
// 只接受 1..20 位十进制数字，避免把任意字符串当成标签参与匹配。
func AffinityTagFromUserID(prefix, raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" || len(raw) > maxAccountAffinityUserIDLength {
		return "", false
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] < '0' || raw[i] > '9' {
			return "", false
		}
	}
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = defaultAccountAffinityTagPrefix
	}
	return prefix + raw, true
}

// WithAccountAffinityTags 把请求可见的标签写入 context；空集合不写。
func WithAccountAffinityTags(ctx context.Context, tags []string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	normalized := normalizeAffinityTags(tags)
	if len(normalized) == 0 {
		return ctx
	}
	return context.WithValue(ctx, ctxkey.AccountAffinityTags, normalized)
}

// AccountAffinityTagsFromContext 读取请求可见的标签；无则返回 nil。
func AccountAffinityTagsFromContext(ctx context.Context) []string {
	if ctx == nil {
		return nil
	}
	tags, _ := ctx.Value(ctxkey.AccountAffinityTags).([]string)
	return tags
}

func normalizeAffinityTags(raw []string) []string {
	if len(raw) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(raw))
	out := make([]string, 0, len(raw))
	for _, tag := range raw {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}
		if _, dup := seen[tag]; dup {
			continue
		}
		seen[tag] = struct{}{}
		out = append(out, tag)
		if len(out) >= maxAccountAffinityTagsPerAccount {
			break
		}
	}
	return out
}

// AffinityTags 返回账号 extra.affinity_tags（兼容 JSON 反序列化出的 []any 与 []string）。
func (a *Account) AffinityTags() []string {
	if a == nil || len(a.Extra) == 0 {
		return nil
	}
	raw, ok := a.Extra[AffinityTagsExtraKey]
	if !ok || raw == nil {
		return nil
	}
	switch v := raw.(type) {
	case []string:
		return normalizeAffinityTags(v)
	case []any:
		tags := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				tags = append(tags, s)
			}
		}
		return normalizeAffinityTags(tags)
	case string:
		// 容忍手工写成单个字符串的情况。
		return normalizeAffinityTags([]string{v})
	default:
		return nil
	}
}

// IsAffinityReserved 账号声明了标签即为保留账号。
func (a *Account) IsAffinityReserved() bool {
	return len(a.AffinityTags()) > 0
}

// MatchesAffinityTags 账号标签与请求标签存在交集。
func (a *Account) MatchesAffinityTags(requestTags []string) bool {
	if len(requestTags) == 0 {
		return false
	}
	accountTags := a.AffinityTags()
	if len(accountTags) == 0 {
		return false
	}
	for _, want := range requestTags {
		for _, have := range accountTags {
			if want == have {
				return true
			}
		}
	}
	return false
}

// isAccountVisibleForAffinity 保留账号只对携带匹配标签的请求可见；开放账号始终可见。
func isAccountVisibleForAffinity(requestTags []string, account *Account) bool {
	if account == nil {
		return false
	}
	if !account.IsAffinityReserved() {
		return true
	}
	return account.MatchesAffinityTags(requestTags)
}

// filterAccountsByAffinityVisibility 按请求标签裁掉不可见的保留账号。
// 这是硬 1:1 的唯一执行点：所有调度路径的候选列表都经过这里。
func filterAccountsByAffinityVisibility(ctx context.Context, accounts []Account) []Account {
	if len(accounts) == 0 {
		return accounts
	}
	requestTags := AccountAffinityTagsFromContext(ctx)
	hasReserved := false
	for i := range accounts {
		if accounts[i].IsAffinityReserved() {
			hasReserved = true
			break
		}
	}
	if !hasReserved {
		return accounts
	}
	filtered := make([]Account, 0, len(accounts))
	for i := range accounts {
		if isAccountVisibleForAffinity(requestTags, &accounts[i]) {
			filtered = append(filtered, accounts[i])
		}
	}
	return filtered
}

// isAffinityPreferred 请求携带匹配标签时，对应保留账号为优先候选。
func isAffinityPreferred(requestTags []string, account *Account) bool {
	return len(requestTags) > 0 && account.MatchesAffinityTags(requestTags)
}

// filterByAffinityPreference 负载感知分层筛选的第 0 层：存在匹配标签的候选时只保留它们，
// 否则原样返回。配合“tryAcquire 失败即移除重选”的循环，保留账号忙时自然落到开放账号。
func filterByAffinityPreference(ctx context.Context, accounts []accountWithLoad) []accountWithLoad {
	if len(accounts) <= 1 {
		return accounts
	}
	requestTags := AccountAffinityTagsFromContext(ctx)
	if len(requestTags) == 0 {
		return accounts
	}
	preferred := make([]accountWithLoad, 0, len(accounts))
	for _, item := range accounts {
		if isAffinityPreferred(requestTags, item.account) {
			preferred = append(preferred, item)
		}
	}
	if len(preferred) == 0 {
		return accounts
	}
	return preferred
}

// stablePreferAffinityAccounts 把匹配标签的账号稳定地移到切片前部（保持其余顺序）。
func stablePreferAffinityAccounts(ctx context.Context, accounts []*Account) {
	if len(accounts) <= 1 {
		return
	}
	requestTags := AccountAffinityTagsFromContext(ctx)
	if len(requestTags) == 0 {
		return
	}
	sort.SliceStable(accounts, func(i, j int) bool {
		return isAffinityPreferred(requestTags, accounts[i]) && !isAffinityPreferred(requestTags, accounts[j])
	})
}

// stablePreferAffinityAccountsWithLoad 同上，作用于带负载信息的候选切片。
func stablePreferAffinityAccountsWithLoad(ctx context.Context, accounts []accountWithLoad) {
	if len(accounts) <= 1 {
		return
	}
	requestTags := AccountAffinityTagsFromContext(ctx)
	if len(requestTags) == 0 {
		return
	}
	sort.SliceStable(accounts, func(i, j int) bool {
		return isAffinityPreferred(requestTags, accounts[i].account) && !isAffinityPreferred(requestTags, accounts[j].account)
	})
}
