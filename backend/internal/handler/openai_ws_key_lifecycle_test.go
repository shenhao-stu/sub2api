package handler

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

type continuationWSKeyRepo struct {
	service.APIKeyRepository
	key     *service.APIKey
	change  string
	changed atomic.Bool
}

func (r *continuationWSKeyRepo) GetByKey(context.Context, string) (*service.APIKey, error) {
	k := *r.key
	if r.changed.Load() {
		switch r.change {
		case "deleted":
			return nil, service.ErrAPIKeyNotFound
		case "disabled":
			k.Status = service.StatusAPIKeyDisabled
		case "user_disabled":
			u := *k.User
			u.Status = service.StatusDisabled
			k.User = &u
		case "group_changed":
			id := int64(999)
			k.GroupID = &id
		}
	}
	return &k, nil
}

func TestOpenAIResponsesWebSocketRejectsRevokedKey(t *testing.T) {
	for _, mode := range []string{service.OpenAIWSIngressModePassthrough, service.OpenAIWSIngressModeDedicated} {
		for _, change := range []string{"deleted", "disabled", "user_disabled", "group_changed"} {
			t.Run(mode+"/"+change, func(t *testing.T) {
				runOpenAIResponsesWebSocketUsageLogCase(t, openAIResponsesWSUsageLogCase{
					firstPayload:  `{"type":"response.create","model":"gpt-5.4","input":"first"}`,
					secondPayload: `{"type":"response.create","model":"gpt-5.4","input":"second"}`,
					ingressMode:   mode, keyChangeAfterFirst: change,
					secondTurnCloseExpected: true, closeReason: "access revoked",
				})
			})
		}
	}
}
