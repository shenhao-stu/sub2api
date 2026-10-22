package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

const (
	accountAffinityCacheTTL  = 30 * time.Second
	accountAffinityErrorTTL  = 5 * time.Second
	accountAffinityDBTimeout = 5 * time.Second
	// maxAccountAffinityTrustedKeys 白名单上限：亲和调度面向少数内部调用方，不是通用授权表。
	maxAccountAffinityTrustedKeys = 64
)

type cachedAccountAffinitySettings struct {
	settings  AccountAffinitySettings
	expiresAt int64 // unix nano
}

// GetAccountAffinitySettings 从 DB 读取账号亲和配置；未设置或损坏时返回默认（关闭）。
func (s *SettingService) GetAccountAffinitySettings(ctx context.Context) (*AccountAffinitySettings, error) {
	value, err := s.settingRepo.GetValue(ctx, SettingKeyAccountAffinitySettings)
	if err != nil {
		if errors.Is(err, ErrSettingNotFound) {
			return DefaultAccountAffinitySettings(), nil
		}
		return nil, fmt.Errorf("get account affinity settings: %w", err)
	}
	if value == "" {
		return DefaultAccountAffinitySettings(), nil
	}
	var settings AccountAffinitySettings
	if err := json.Unmarshal([]byte(value), &settings); err != nil {
		slog.Warn("account_affinity_settings_invalid_json", "error", err)
		return DefaultAccountAffinitySettings(), nil
	}
	settings.Normalize()
	return &settings, nil
}

// SetAccountAffinitySettings 校验并写入配置，同时刷新进程内缓存。
func (s *SettingService) SetAccountAffinitySettings(ctx context.Context, settings *AccountAffinitySettings) error {
	if settings == nil {
		return fmt.Errorf("settings cannot be nil")
	}
	for _, id := range settings.TrustedAPIKeyIDs {
		if id <= 0 {
			return fmt.Errorf("trusted_api_key_ids must be positive integers")
		}
	}
	settings.Normalize()
	if len(settings.TrustedAPIKeyIDs) > maxAccountAffinityTrustedKeys {
		return fmt.Errorf("trusted_api_key_ids must not exceed %d entries", maxAccountAffinityTrustedKeys)
	}
	if settings.Enabled && len(settings.TrustedAPIKeyIDs) == 0 {
		return fmt.Errorf("trusted_api_key_ids must not be empty when enabled")
	}
	data, err := json.Marshal(settings)
	if err != nil {
		return fmt.Errorf("marshal account affinity settings: %w", err)
	}
	if err := s.settingRepo.Set(ctx, SettingKeyAccountAffinitySettings, string(data)); err != nil {
		return err
	}
	s.storeAccountAffinityCache(*settings, accountAffinityCacheTTL)
	return nil
}

// GetAccountAffinitySettingsCached 返回账号亲和配置（进程内缓存，30s TTL）。
// DB 错误时沿用最近一次已知值（无缓存则默认关闭），并以短 TTL 快速重试；
// 任何失败都朝“保留账号不可见”的安全方向退化。
func (s *SettingService) GetAccountAffinitySettingsCached(ctx context.Context) AccountAffinitySettings {
	if s == nil || s.settingRepo == nil {
		return *DefaultAccountAffinitySettings()
	}
	if cached, ok := s.accountAffinityCache.Load().(*cachedAccountAffinitySettings); ok && cached != nil {
		if time.Now().UnixNano() < cached.expiresAt {
			return cached.settings
		}
	}

	result, _, _ := s.accountAffinitySF.Do("account_affinity_settings", func() (any, error) {
		if cached, ok := s.accountAffinityCache.Load().(*cachedAccountAffinitySettings); ok && cached != nil {
			if time.Now().UnixNano() < cached.expiresAt {
				return cached.settings, nil
			}
		}
		if ctx == nil {
			ctx = context.Background()
		}
		dbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), accountAffinityDBTimeout)
		defer cancel()

		settings, err := s.GetAccountAffinitySettings(dbCtx)
		if err != nil {
			slog.Warn("account_affinity_settings_read_failed", "error", err)
			fallback := *DefaultAccountAffinitySettings()
			if prior, ok := s.accountAffinityCache.Load().(*cachedAccountAffinitySettings); ok && prior != nil {
				fallback = prior.settings
			}
			s.storeAccountAffinityCache(fallback, accountAffinityErrorTTL)
			return fallback, nil
		}
		s.storeAccountAffinityCache(*settings, accountAffinityCacheTTL)
		return *settings, nil
	})
	if settings, ok := result.(AccountAffinitySettings); ok {
		return settings
	}
	return *DefaultAccountAffinitySettings()
}

func (s *SettingService) storeAccountAffinityCache(settings AccountAffinitySettings, ttl time.Duration) {
	s.accountAffinityCache.Store(&cachedAccountAffinitySettings{
		settings:  settings,
		expiresAt: time.Now().Add(ttl).UnixNano(),
	})
}
