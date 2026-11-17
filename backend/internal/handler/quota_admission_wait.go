package handler

import (
	"context"
	"slices"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ip"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

type quotaAdmissionWait struct {
	tag       string
	slots     chan struct{}
	heartbeat time.Duration
}

func newQuotaAdmissionWait(tag string) *quotaAdmissionWait {
	if tag == "" {
		return nil
	}
	return &quotaAdmissionWait{tag: tag, slots: make(chan struct{}, 2), heartbeat: 15 * time.Second}
}

func (q *quotaAdmissionWait) matches(ctx context.Context) bool {
	return q != nil && slices.Contains(service.AccountAffinityTagsFromContext(ctx), q.tag)
}

func (q *quotaAdmissionWait) canWait(seconds int) bool {
	return seconds > 0 && seconds <= 45*60
}

func isUndispatchedQuotaGuard(platform string, failure *service.UpstreamFailoverError) bool {
	return platform == service.PlatformAnthropic && failure != nil && failure.FirstAttemptQuotaGuard && failure.Reason == service.GatewayFailureReason(service.AnthropicQuotaGuardCode) &&
		service.IsAnthropicQuotaGuardRejection(failure.StatusCode, failure.ResponseBody)
}

// Retain the original HTTP request while waiting for a known reset. No model,
// receipt, or scheduler probe is issued by the timer. Client abort stops it.
func (q *quotaAdmissionWait) wait(c *gin.Context, delay time.Duration, streamStarted *bool) (bool, error) {
	ctx := c.Request.Context()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	select {
	case q.slots <- struct{}{}:
		defer func() { <-q.slots }()
	default:
		return false, nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	beats := time.NewTicker(q.heartbeat)
	defer beats.Stop()
	for {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-timer.C:
			err := ctx.Err()
			return err == nil, err
		case <-beats.C:
			if !c.Writer.Written() {
				c.Header("Content-Type", "text/event-stream")
				c.Header("Cache-Control", "no-cache")
				c.Header("X-Accel-Buffering", "no")
			}
			n, err := c.Writer.WriteString(string(SSEPingFormatClaude))
			recordGatewayStreamHeartbeat(c, n)
			if err != nil {
				return false, err
			}
			*streamStarted = true
			c.Writer.Flush()
		}
	}
}

// Re-read access and billing scope before a delayed selection. Only the
// original caller and group can resume the request.
func (h *GatewayHandler) recheckQuotaWaitKey(c *gin.Context, original *service.APIKey) (*service.APIKey, bool) {
	ctx := c.Request.Context()
	if h.apiKeyService == nil || h.settingService == nil {
		return nil, false
	}
	key, err := h.apiKeyService.GetByID(ctx, original.ID)
	if err != nil || key == nil || key.UserID != original.UserID || key.GroupID == nil || original.GroupID == nil || *key.GroupID != *original.GroupID ||
		!key.IsActive() || key.IsExpired() || key.IsQuotaExhausted() || key.User == nil || !key.User.IsActive() || key.Group == nil || !key.Group.IsActive() ||
		original.Group == nil || key.Group.Platform != original.Group.Platform || key.Group.SubscriptionType != original.Group.SubscriptionType || key.Group.ClaudeCodeOnly != original.Group.ClaudeCodeOnly {
		return nil, false
	}
	cfg := h.settingService.GetAccountAffinitySettingsCached(ctx)
	tag, ok := service.AffinityTagFromUserID(cfg.TagPrefix, c.GetHeader(service.AccountAffinityHeader))
	allowedIP, _ := ip.CheckIPRestrictionWithCompiledRules(ip.GetClientIP(c), key.CompiledIPWhitelist, key.CompiledIPBlacklist)
	return key, allowedIP && ok && tag == h.quotaWait.tag && cfg.TrustsAPIKey(key.ID)
}
