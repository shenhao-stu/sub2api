package middleware

import (
	"context"
	"log/slog"

	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/gin-gonic/gin"
)

// AccountAffinitySettingsReader 读取账号亲和配置（热路径必须走进程内缓存）。
type AccountAffinitySettingsReader interface {
	GetAccountAffinitySettingsCached(ctx context.Context) service.AccountAffinitySettings
}

// AccountAffinity 把可信调用方携带的用户标识转成账号亲和标签写入请求 context。
//
// 挂载位置：apiKeyAuth 之后（需要已鉴权的 API Key ID）。
//
// 行为：
//   - 请求头 X-GeToken-User-Id 缺失 → 直接放行（最常见路径，零开销）。
//   - 配置关闭、API Key 不在白名单、标识不是 1..20 位十进制数字 → 忽略请求头放行；
//     结果等同于"无标签"，保留账号对该请求不可见。永不拒绝请求。
//   - 通过校验 → context 写入 []string{prefix + user_id}，只影响调度，不影响计费归属。
func AccountAffinity(settings AccountAffinitySettingsReader) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request == nil || settings == nil {
			c.Next()
			return
		}
		raw := c.GetHeader(service.AccountAffinityHeader)
		if raw == "" {
			c.Next()
			return
		}
		apiKey, ok := GetAPIKeyFromContext(c)
		if !ok || apiKey == nil {
			c.Next()
			return
		}
		cfg := settings.GetAccountAffinitySettingsCached(c.Request.Context())
		if !cfg.TrustsAPIKey(apiKey.ID) {
			slog.Debug("account_affinity_header_ignored",
				"api_key_id", apiKey.ID,
				"reason", "untrusted_api_key",
				"enabled", cfg.Enabled)
			c.Next()
			return
		}
		tag, ok := service.AffinityTagFromUserID(cfg.TagPrefix, raw)
		if !ok {
			slog.Debug("account_affinity_header_ignored",
				"api_key_id", apiKey.ID,
				"reason", "invalid_user_id")
			c.Next()
			return
		}
		ctx := service.WithAccountAffinityTags(c.Request.Context(), []string{tag})
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}
