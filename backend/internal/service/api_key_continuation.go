package service

import (
	"context"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ip"
)

var ErrAPIKeyAccessRevoked = infraerrors.Unauthorized("API_KEY_ACCESS_REVOKED", "API key access revoked; reconnect with an active key")

// A connection's authentication snapshot cannot authorize new work indefinitely.
// Read the source of truth without auth caching; accepted work keeps its snapshot.
func (s *APIKeyService) RevalidateContinuation(ctx context.Context, accepted *APIKey, clientIP string) (*APIKey, error) {
	if s == nil || s.apiKeyRepo == nil || accepted == nil {
		return nil, ErrAPIKeyAccessRevoked
	}
	current, err := s.apiKeyRepo.GetByKey(ctx, accepted.Key)
	if err != nil {
		return nil, err
	}
	simple := s.cfg != nil && s.cfg.RunMode == config.RunModeSimple
	if err := validateAPIKeyContinuation(accepted, current, simple, time.Now()); err != nil {
		return nil, err
	}
	s.compileAPIKeyIPRules(current)
	if allowed, _ := ip.CheckIPRestrictionWithCompiledRules(clientIP, current.CompiledIPWhitelist, current.CompiledIPBlacklist); !allowed {
		return nil, ErrAPIKeyAccessRevoked
	}
	return current, nil
}

func validateAPIKeyContinuation(accepted, current *APIKey, simple bool, now time.Time) error {
	if current == nil || accepted == nil || current.ID != accepted.ID || current.UserID != accepted.UserID ||
		current.User == nil || current.User.ID != current.UserID || !current.User.IsActive() {
		return ErrAPIKeyAccessRevoked
	}
	if (current.GroupID == nil) != (accepted.GroupID == nil) ||
		current.GroupID != nil && *current.GroupID != *accepted.GroupID {
		return ErrAPIKeyAccessRevoked
	}
	if !current.IsActive() && !(simple && (current.Status == StatusAPIKeyExpired || current.Status == StatusAPIKeyQuotaExhausted)) {
		return ErrAPIKeyAccessRevoked
	}
	if current.GroupID != nil {
		g := current.Group
		if g == nil || !g.IsActive() || !g.IsSubscriptionType() && !current.User.CanBindGroup(g.ID, g.IsExclusive) {
			return ErrAPIKeyAccessRevoked
		}
	}
	if !simple {
		if current.ExpiresAt != nil && !current.ExpiresAt.After(now) {
			return ErrAPIKeyExpired
		}
		if current.IsQuotaExhausted() {
			return ErrAPIKeyQuotaExhausted
		}
	}
	return nil
}
