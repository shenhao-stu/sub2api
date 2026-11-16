package service

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// ValidateOpenAIChatReasoningEffort rejects a known unsupported combination
// before acknowledging a durable receipt. Resolve the selected account's model
// mapping first; a public alias alone is not evidence of the upstream model.
func (s *OpenAIGatewayService) ValidateOpenAIChatReasoningEffort(c *gin.Context, account *Account, body []byte) error {
	if account == nil || !account.IsOpenAI() {
		return nil
	}
	model := resolveOpenAIForwardModel(account, gjson.GetBytes(body, "model").String(), "")
	model = normalizeOpenAIModelForUpstream(account, model)
	if model != "gpt-6-astra" {
		return nil
	}
	param := "reasoning_effort"
	if IsResponsesShapedChatRequest(body) {
		param = "reasoning.effort"
	}
	if !strings.EqualFold(strings.TrimSpace(gjson.GetBytes(body, param).String()), "none") {
		return nil
	}
	message := "gpt-6-astra does not support reasoning effort none. Omit the effort to use the model default, or choose low, medium, high, xhigh, or max."
	writeOpenAIErrorFields(c, openAIClientErrorFields{
		Type: "invalid_request_error", Code: "unsupported_value", Param: param,
		Message: message, StatusCode: http.StatusBadRequest,
	}, false)
	return errors.New(message)
}
