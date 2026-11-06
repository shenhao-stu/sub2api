package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/google/uuid"
)

func privateImagePool(targetField, poolField string, target int64) (int64, bool) {
	group, err := strconv.ParseInt(targetField, 10, 64)
	if err != nil || group <= 0 || group != target {
		return 0, false
	}
	pool, err := strconv.ParseInt(poolField, 10, 64)
	return pool, err == nil && pool > 0 && pool != group
}
func nanoImagePool(target int64) (int64, bool) {
	client, donor, ok := strings.Cut(os.Getenv("SUB2API_PERSONAL_NANO_IMAGE_POOL"), "/")
	if !ok {
		return 0, false
	}
	return privateImagePool(client, donor, target)
}
func PersonalGrokImageSource(policy string, target int64, model string) (int64, bool) {
	fields := strings.Split(policy, "/")
	if len(fields) != 3 {
		return 0, false
	}
	models := strings.Split(fields[2], ",")
	for _, item := range models {
		if item != "grok-imagine-image" && item != "grok-imagine-image-2.0" {
			return 0, false
		}
	}
	if !slices.Contains(models, model) {
		return 0, false
	}
	return privateImagePool(fields[0], fields[1], target)
}

// Claim all requests to a configured SKU, including the original provider's
// streamed/edited/unsupported requests. A duplicate cannot pay the other
// provider while an earlier request has acquired the last donor slot. This is
// a terminal acquisition receipt, not evidence that inference succeeded.
func PersonalImageIntent(ctx context.Context, key *APIKey, model string, headers http.Header) (*IdempotencyRecord, error) {
	if key == nil || key.Group == nil || key.GroupID == nil || *key.GroupID != key.Group.ID || key.Group.Platform != PlatformOpenAI {
		return nil, nil
	}
	if model == personalNanoModel {
		if _, ok := nanoImagePool(key.Group.ID); !ok {
			return nil, nil
		}
	} else if _, ok := PersonalGrokImageSource(os.Getenv("SUB2API_PERSONAL_GROK_IMAGE_POOL"), key.Group.ID, model); !ok {
		return nil, nil
	}
	if key.ID <= 0 || key.UserID <= 0 {
		return nil, errors.New("authenticated image principal is missing")
	}
	id := imageLogicalID(ctx, key, headers)
	return &IdempotencyRecord{Scope: "gateway.personal-image.intent", IdempotencyKeyHash: id, RequestFingerprint: id, Status: IdempotencyStatusSucceeded, ExpiresAt: time.Now().Add(30 * 24 * time.Hour)}, nil
}
func imageLogicalID(ctx context.Context, key *APIKey, headers http.Header) string {
	id, _ := ctx.Value(ctxkey.ClientRequestID).(string)
	kind := "correlation"
	for _, name := range []string{"Idempotency-Key", "X-Client-Request-ID"} {
		values := headers.Values(name)
		if len(values) == 1 && len(values[0]) > 0 && len(values[0]) <= 128 && !strings.ContainsAny(values[0], "\r\n\x00") {
			id = values[0]
			kind = name
			break
		}
	}
	if id == "" {
		id = uuid.NewString()
	}
	digest := sha256.Sum256([]byte(openAIRelayPrincipal(ctx, key.UserID, key.ID) + "\x00" + kind + "\x00" + id))
	return hex.EncodeToString(digest[:])
}
