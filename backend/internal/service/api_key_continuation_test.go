package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type continuationKeyRepo struct {
	APIKeyRepository
	current *APIKey
	err     error
	reads   int
}

func (r *continuationKeyRepo) GetByKey(context.Context, string) (*APIKey, error) {
	r.reads++
	return r.current, r.err
}

func TestAPIKeyContinuationPolicy(t *testing.T) {
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name    string
		change  func(*APIKey)
		simple  bool
		allowed bool
	}{
		{"active", func(*APIKey) {}, false, true},
		{"expired", func(k *APIKey) { k.ExpiresAt = &now }, false, false},
		{"quota", func(k *APIKey) { k.Quota, k.QuotaUsed = 1, 1 }, false, false},
		{"disabled", func(k *APIKey) { k.Status = StatusAPIKeyDisabled }, true, false},
		{"wrong_owner", func(k *APIKey) { k.UserID++ }, false, false},
		{"changed_group", func(k *APIKey) { id := int64(2); k.GroupID = &id }, false, false},
		{"disabled_group", func(k *APIKey) { k.Group.Status = StatusDisabled }, false, false},
		{"exclusive_revoked", func(k *APIKey) { k.Group.IsExclusive = true }, false, false},
		{"disabled_user", func(k *APIKey) { k.User.Status = StatusDisabled }, false, false},
		{"simple_retains_billing_bypass", func(k *APIKey) { k.Status = StatusAPIKeyExpired; k.ExpiresAt = &now; k.Quota, k.QuotaUsed = 1, 1 }, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := int64(1)
			accepted := &APIKey{ID: 1, UserID: 1, GroupID: &id}
			current := &APIKey{ID: 1, UserID: 1, GroupID: &id, Status: StatusActive, User: &User{ID: 1, Status: StatusActive}, Group: &Group{ID: 1, Status: StatusActive}}
			tc.change(current)
			err := validateAPIKeyContinuation(accepted, current, tc.simple, now)
			require.Equal(t, tc.allowed, err == nil)
		})
	}
}

func TestRevalidateContinuationAlwaysReadsCurrentKey(t *testing.T) {
	accepted := &APIKey{ID: 1, UserID: 2, Key: "synthetic", Status: StatusActive, User: &User{ID: 2, Status: StatusActive}}
	current := *accepted
	r := &continuationKeyRepo{current: &current}
	s := NewAPIKeyService(r, nil, nil, nil, nil, nil, &config.Config{})
	_, err := s.RevalidateContinuation(context.Background(), accepted, "127.0.0.1")
	require.NoError(t, err)
	current.IPBlacklist = []string{"127.0.0.1"}
	_, err = s.RevalidateContinuation(context.Background(), accepted, "127.0.0.1")
	require.ErrorIs(t, err, ErrAPIKeyAccessRevoked)
	r.err = errors.New("database unavailable")
	_, err = s.RevalidateContinuation(context.Background(), accepted, "127.0.0.1")
	require.ErrorIs(t, err, r.err)
	require.Equal(t, 3, r.reads)
	require.Equal(t, StatusActive, accepted.Status)
	require.Empty(t, accepted.IPBlacklist)
}
