//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestGetAccountAffinitySettings_DefaultsWhenNotSet(t *testing.T) {
	svc := NewSettingService(newMockSettingRepo(), &config.Config{})
	settings, err := svc.GetAccountAffinitySettings(context.Background())
	require.NoError(t, err)
	require.False(t, settings.Enabled)
	require.Empty(t, settings.TrustedAPIKeyIDs)
	require.Equal(t, "getoken:", settings.TagPrefix)
}

func TestGetAccountAffinitySettings_ReadsAndNormalizes(t *testing.T) {
	repo := newMockSettingRepo()
	repo.data[SettingKeyAccountAffinitySettings] = `{"enabled":true,"trusted_api_key_ids":[321,214,214],"tag_prefix":""}`
	svc := NewSettingService(repo, &config.Config{})
	settings, err := svc.GetAccountAffinitySettings(context.Background())
	require.NoError(t, err)
	require.True(t, settings.Enabled)
	require.Equal(t, []int64{214, 321}, settings.TrustedAPIKeyIDs)
	require.Equal(t, "getoken:", settings.TagPrefix)
}

func TestGetAccountAffinitySettings_InvalidJSONFallsBackToDisabled(t *testing.T) {
	repo := newMockSettingRepo()
	repo.data[SettingKeyAccountAffinitySettings] = `{not json`
	svc := NewSettingService(repo, &config.Config{})
	settings, err := svc.GetAccountAffinitySettings(context.Background())
	require.NoError(t, err)
	require.False(t, settings.Enabled)
}

func TestSetAccountAffinitySettings_Validation(t *testing.T) {
	svc := NewSettingService(newMockSettingRepo(), &config.Config{})
	ctx := context.Background()

	require.Error(t, svc.SetAccountAffinitySettings(ctx, nil))
	require.ErrorContains(t, svc.SetAccountAffinitySettings(ctx, &AccountAffinitySettings{Enabled: true}), "must not be empty when enabled")
	require.ErrorContains(t, svc.SetAccountAffinitySettings(ctx, &AccountAffinitySettings{Enabled: true, TrustedAPIKeyIDs: []int64{214, 0}}), "positive integers")

	tooMany := make([]int64, 0, maxAccountAffinityTrustedKeys+1)
	for i := int64(1); i <= maxAccountAffinityTrustedKeys+1; i++ {
		tooMany = append(tooMany, i)
	}
	require.ErrorContains(t, svc.SetAccountAffinitySettings(ctx, &AccountAffinitySettings{Enabled: true, TrustedAPIKeyIDs: tooMany}), "must not exceed")

	// 关闭状态允许空白名单
	require.NoError(t, svc.SetAccountAffinitySettings(ctx, &AccountAffinitySettings{Enabled: false}))

	require.NoError(t, svc.SetAccountAffinitySettings(ctx, &AccountAffinitySettings{Enabled: true, TrustedAPIKeyIDs: []int64{321, 214}}))
	got, err := svc.GetAccountAffinitySettings(ctx)
	require.NoError(t, err)
	require.True(t, got.Enabled)
	require.Equal(t, []int64{214, 321}, got.TrustedAPIKeyIDs)

	var stored AccountAffinitySettings
	require.NoError(t, json.Unmarshal([]byte(svc.settingRepo.(*mockSettingRepo).data[SettingKeyAccountAffinitySettings]), &stored))
	require.Equal(t, []int64{214, 321}, stored.TrustedAPIKeyIDs)
}

func TestGetAccountAffinitySettingsCached_UsesCacheAndRefreshesOnSet(t *testing.T) {
	repo := newMockSettingRepo()
	repo.data[SettingKeyAccountAffinitySettings] = `{"enabled":true,"trusted_api_key_ids":[214]}`
	svc := NewSettingService(repo, &config.Config{})
	ctx := context.Background()

	first := svc.GetAccountAffinitySettingsCached(ctx)
	require.True(t, first.TrustsAPIKey(214))
	calls := repo.getValueCalls

	second := svc.GetAccountAffinitySettingsCached(ctx)
	require.True(t, second.TrustsAPIKey(214))
	require.Equal(t, calls, repo.getValueCalls, "TTL 内不应再次访问 DB")

	// Set 立即刷新缓存
	require.NoError(t, svc.SetAccountAffinitySettings(ctx, &AccountAffinitySettings{Enabled: true, TrustedAPIKeyIDs: []int64{320}}))
	third := svc.GetAccountAffinitySettingsCached(ctx)
	require.False(t, third.TrustsAPIKey(214))
	require.True(t, third.TrustsAPIKey(320))
}

func TestGetAccountAffinitySettingsCached_DBErrorKeepsLastKnownThenDefault(t *testing.T) {
	repo := newMockSettingRepo()
	repo.data[SettingKeyAccountAffinitySettings] = `{"enabled":true,"trusted_api_key_ids":[214]}`
	svc := NewSettingService(repo, &config.Config{})
	ctx := context.Background()

	require.True(t, svc.GetAccountAffinitySettingsCached(ctx).TrustsAPIKey(214))

	// 过期后 DB 出错：沿用最近一次已知值
	svc.storeAccountAffinityCache(svc.GetAccountAffinitySettingsCached(ctx), -time.Second)
	repo.getValueErr = errors.New("db down")
	require.True(t, svc.GetAccountAffinitySettingsCached(ctx).TrustsAPIKey(214))

	// 完全没有缓存且 DB 出错：默认关闭（保留账号不可见）
	fresh := NewSettingService(repo, &config.Config{})
	require.False(t, fresh.GetAccountAffinitySettingsCached(ctx).Enabled)

	var nilSvc *SettingService
	require.False(t, nilSvc.GetAccountAffinitySettingsCached(ctx).Enabled)
}
