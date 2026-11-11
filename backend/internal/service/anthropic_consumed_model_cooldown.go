package service

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

const anthropicConsumedErrorLimit = 32 << 10

func isAnthropicConsumedError(account *Account, status int, body []byte) bool {
	return status == http.StatusBadRequest && account != nil && account.Platform == PlatformAnthropic &&
		account.Type == AccountTypeAPIKey && len(body) < anthropicConsumedErrorLimit && gjson.ValidBytes(body) &&
		gjson.GetBytes(body, "error.code").String() == "native_request_consumed_no_replay" &&
		gjson.GetBytes(body, "error.details.no_replay").Type == gjson.True
}

// Preserve the original bytes, one-shot read error and Close when this is not
// the complete bounded contract. An unrelated 400 still follows its old policy.
type anthropicErrorPrefixBody struct {
	prefix  *bytes.Reader
	source  io.ReadCloser
	readErr error
}

func (b *anthropicErrorPrefixBody) Read(p []byte) (int, error) {
	if b.prefix.Len() > 0 {
		return b.prefix.Read(p)
	}
	if b.readErr != nil {
		err := b.readErr
		b.readErr = nil
		return 0, err
	}
	return b.source.Read(p)
}

func (b *anthropicErrorPrefixBody) Close() error { return b.source.Close() }

func (s *GatewayService) isAnthropicConsumedResponse(c *gin.Context, account *Account, resp *http.Response) bool {
	if resp == nil || resp.Body == nil || resp.StatusCode != http.StatusBadRequest ||
		account == nil || account.Platform != PlatformAnthropic || account.Type != AccountTypeAPIKey {
		return false
	}
	source := resp.Body
	body, err := awaitAnthropicBody(c, source, func() ([]byte, error) {
		return io.ReadAll(io.LimitReader(source, anthropicConsumedErrorLimit))
	})
	resp.Body = &anthropicErrorPrefixBody{prefix: bytes.NewReader(body), source: source, readErr: err}
	return err == nil && isAnthropicConsumedError(account, resp.StatusCode, body)
}

// canonicalModel is the exact model already used for dispatch, never remapped.
func (s *RateLimitService) persistAnthropicConsumedModelCooldown(ctx context.Context, account *Account, body []byte, canonicalModel string) {
	if s == nil || !isAnthropicConsumedError(account, http.StatusBadRequest, body) {
		return
	}
	details := gjson.GetBytes(body, "error.details")
	if details.Get("upstream_type").String() != "rate_limit_error" ||
		canonicalModel == "" || len(canonicalModel) > 128 || details.Get("model").Type != gjson.String || details.Get("model").String() != canonicalModel {
		return
	}
	if !validNativeCooldownModel(canonicalModel) {
		return
	}
	window := details.Get("rate_limit_type").String()
	fable := window == "seven_day_overage_included" && strings.HasPrefix(canonicalModel, "claude-fable-")
	if window != "five_hour" && window != "seven_day" && !fable {
		return
	}
	reset, err := strconv.ParseInt(details.Get("reset_at").Raw, 10, 64)
	now := time.Now()
	if err != nil || reset <= now.Unix() || reset > now.Add(7*24*time.Hour).Unix() {
		return
	}
	stateCtx, cancel := openAIAccountStateContext(ctx)
	defer cancel()
	until := time.Unix(reset, 0).UTC()
	weekly := details.Get("weekly_utilization")
	if window == "seven_day" && details.Get("quota_scope").String() == "account" &&
		details.Get("api_error").String() == "usage_limit_reached" && weekly.Type == gjson.Number &&
		weekly.Float() >= 1 && weekly.Float() <= 1000000 {
		if repo, ok := s.accountRepo.(accountRateLimitExtender); ok {
			if err := repo.SetRateLimitedIfLater(stateCtx, account.ID, until); err != nil {
				slog.Warn("native_weekly_account_rate_limit_failed", "account_id", account.ID, "error", err)
			}
		}
		return
	}
	repo, ok := s.accountRepo.(accountModelRateLimitExtender)
	if !ok {
		return
	}
	models := map[string]bool{canonicalModel: true}
	if fable {
		// One Fable credits window covers its configured versions, not Opus/Sonnet.
		mapping, _ := account.Credentials["model_mapping"].(map[string]any)
		for _, value := range mapping {
			model, ok := value.(string)
			if ok && strings.HasPrefix(model, "claude-fable-") && validNativeCooldownModel(model) {
				models[model] = true
			}
		}
	}
	ordered := make([]string, 0, len(models))
	for model := range models {
		ordered = append(ordered, model)
	}
	sort.Strings(ordered)
	for _, model := range ordered {
		if err := repo.SetModelRateLimitIfLater(stateCtx, account.ID, model, until, "native_consumed_model_rate_limit"); err != nil {
			slog.Warn("native_consumed_model_rate_limit_failed", "account_id", account.ID, "model", model, "error", err)
		}
	}
}

func validNativeCooldownModel(model string) bool {
	if model == "" || len(model) > 128 {
		return false
	}
	for _, c := range model {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.') {
			return false
		}
	}
	return true
}
