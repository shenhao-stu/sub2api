package service

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const modelBudgetBodyLimit = 64 << 10
const modelBudgetBackoff = time.Minute

// Budget for one expensive model can be exhausted while cheaper models still
// work. Match the complete rejection, not arbitrary text mentioning a budget.
func isModelBudgetError(status int, body []byte, model string) bool {
	if status != http.StatusPaymentRequired || len(body) >= modelBudgetBodyLimit || model == "" || len(model) > 256 || strings.TrimSpace(model) != model || strings.ContainsAny(model, "\r\n") {
		return false
	}
	envelope, ok := receiptObject(body)
	if !ok {
		return false
	}
	if raw, exists := envelope["type"]; exists {
		var kind string
		if json.Unmarshal(raw, &kind) != nil || kind != "error" {
			return false
		}
	}
	fields, ok := receiptObject(envelope["error"])
	if !ok {
		return false
	}
	for _, object := range []map[string]json.RawMessage{envelope, fields} {
		if raw, exists := object["status_code"]; exists {
			var declared int
			if json.Unmarshal(raw, &declared) != nil || declared != status {
				return false
			}
		}
	}
	var kind, message string
	if json.Unmarshal(fields["type"], &kind) != nil || kind != "budget_exceeded" || json.Unmarshal(fields["message"], &message) != nil {
		return false
	}
	if raw, exists := fields["code"]; exists {
		var code string
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, &code) != nil || (code != "" && code != "budget_exceeded") {
			return false
		}
	}
	if raw, exists := fields["model"]; exists {
		var declared string
		if json.Unmarshal(raw, &declared) != nil || declared != model {
			return false
		}
	}
	english := "Insufficient API key budget: only 0 " + model + " request(s) can run at once right now. This is not a platform outage or a concurrency setting. Fix: top up, or use a cheaper model."
	bilingual := "API Key 剩余额度不足，当前只够同时进行 0 个 " + model + " 请求。这不是平台故障，也不是并发配置问题。解决：充值，或换用更便宜的模型。 | " + english
	return strings.TrimSpace(message) == english || strings.TrimSpace(message) == bilingual
}

func modelBudgetFields() openAIClientErrorFields {
	return openAIClientErrorFields{Type: "billing_error", Code: "upstream_model_budget_exhausted", Message: "Upstream API key budget is insufficient for the requested model. Please contact the administrator or choose a lower-cost model.", StatusCode: http.StatusPaymentRequired}
}

func (s *RateLimitService) persistModelBudgetCooldown(ctx context.Context, account *Account, canonicalModel string) {
	if s == nil {
		return
	}
	repo, ok := s.accountRepo.(accountModelRateLimitExtender)
	if !ok {
		slog.Warn("model_budget_cooldown_unavailable", "account_id", account.ID, "model", canonicalModel)
		return
	}
	stateCtx, cancel := openAIAccountStateContext(ctx)
	defer cancel()
	// Local retry backoff, not a claimed provider quota reset. An existing later
	// reset must survive concurrent rejections and this shorter pause.
	if err := repo.SetModelRateLimitIfLater(stateCtx, account.ID, canonicalModel, time.Now().Add(modelBudgetBackoff), "upstream_model_budget_exhausted"); err != nil {
		slog.Warn("model_budget_cooldown_failed", "account_id", account.ID, "model", canonicalModel, "error", err)
	}
}

func (s *GatewayService) handleModelBudgetResponse(c *gin.Context, account *Account, resp *http.Response, model, upstreamModel string, stream bool, start time.Time) (*ForwardResult, error, bool) {
	if account == nil || account.Type != AccountTypeAPIKey || resp == nil || resp.Body == nil || resp.StatusCode != http.StatusPaymentRequired {
		return nil, nil, false
	}
	source := resp.Body
	body, readErr := awaitAnthropicBody(c, source, func() ([]byte, error) { return io.ReadAll(io.LimitReader(source, modelBudgetBodyLimit)) })
	resp.Body = &anthropicErrorPrefixBody{prefix: bytes.NewReader(body), source: source, readErr: readErr}
	if readErr != nil || !isModelBudgetError(resp.StatusCode, body, upstreamModel) {
		return nil, nil, false
	}
	defer resp.Body.Close()
	ctx := context.Background()
	if c.Request != nil {
		ctx = c.Request.Context()
	}
	s.rateLimitService.persistModelBudgetCooldown(ctx, account, upstreamModel)
	result, err := s.handleRequestRejectionBody(c, account, resp, body, parseClaudeUsageFromResponseBody(body), model, upstreamModel, stream, start, modelBudgetFields())
	return result, err, true
}
