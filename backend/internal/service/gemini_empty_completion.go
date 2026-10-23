package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// Metering and delivery are independent: an input-only completion is not a
// successful answer, and must not be retried just because it had no output.
type geminiChatDelivery struct {
	enabled, content, upstreamError bool
	prefixOverflow                  bool
	finish, native                  string
}

func newGeminiChatDelivery(models ...string) geminiChatDelivery {
	var d geminiChatDelivery
	for _, model := range models {
		model = model[strings.LastIndex(model, "/")+1:]
		d.enabled = d.enabled || strings.HasPrefix(strings.ToLower(model), "gemini-")
	}
	return d
}

func (d *geminiChatDelivery) observe(payload []byte) {
	if !d.enabled || !gjson.ValidBytes(payload) {
		return
	}
	if e := gjson.GetBytes(payload, "error"); e.Exists() && e.Type != gjson.Null {
		d.upstreamError = true
	}
	for _, ch := range gjson.GetBytes(payload, "choices").Array() {
		if r := ch.Get("finish_reason").String(); r != "" {
			d.finish = safeGeminiFinishReason(r)
		}
		if r := ch.Get("native_finish_reason").String(); r != "" {
			d.native = safeGeminiFinishReason(r)
		}
		for _, key := range []string{"delta", "message"} {
			v := ch.Get(key)
			for _, name := range []string{"content", "reasoning_content", "reasoning", "refusal"} {
				if value := v.Get(name); value.Type == gjson.String && strings.TrimSpace(value.String()) != "" {
					d.content = true
				}
			}
			for _, call := range v.Get("tool_calls").Array() {
				if call.Get("id").String() != "" || call.Get("function.name").String() != "" || call.Get("function.arguments").String() != "" {
					d.content = true
				}
			}
			for _, image := range v.Get("images").Array() {
				if image.Get("image_url.url").String() != "" {
					d.content = true
				}
			}
			if v.Get("function_call.name").String() != "" || v.Get("function_call.arguments").String() != "" || v.Get("audio.data").String() != "" {
				d.content = true
			}
		}
	}
}

func safeGeminiFinishReason(reason string) string {
	switch strings.ToLower(strings.TrimSpace(reason)) {
	case "stop", "length", "max_tokens", "tool_calls", "function_call", "content_filter", "safety", "recitation", "blocklist", "prohibited_content", "spii", "malformed_function_call", "unexpected_tool_call", "other", "language", "no_image", "image_safety", "image_prohibited_content", "image_recitation", "image_other", "too_many_tool_calls", "missing_thought_signature":
		return strings.ToLower(strings.TrimSpace(reason))
	default:
		return "unknown"
	}
}

func (d geminiChatDelivery) empty() bool {
	return d.enabled && (!d.content || d.prefixOverflow) && !d.upstreamError
}

func (d geminiChatDelivery) reject(c *gin.Context, account *Account, requestID string, usage *OpenAIUsage) error {
	message := "Upstream returned no response content"
	if d.prefixOverflow {
		message = "Upstream completion prefix exceeded the safe buffer limit"
	}
	if d.finish != "" {
		message += "; finish_reason=" + d.finish
	}
	if d.native != "" {
		message += "; native_finish_reason=" + d.native
	}
	if openAIUsageHasTokens(usage) {
		message += "; reported token usage is retained"
	}
	message += "; automatic retry disabled"
	setOpsUpstreamError(c, http.StatusOK, message, "")
	appendOpsUpstreamError(c, OpsUpstreamErrorEvent{Platform: account.Platform, AccountID: account.ID, AccountName: account.Name, UpstreamStatusCode: http.StatusOK, UpstreamRequestID: requestID, Kind: "http_error", Message: message})
	body := openAIMeteredErrorMetadata(c, openAIErrorBodyWithUsage(gin.H{"error": gin.H{"type": "upstream_error", "code": "upstream_empty_completion", "status_code": http.StatusBadGateway, "message": message, "finish_reason": d.finish, "native_finish_reason": d.native}}, usage))
	MarkResponseCommitted(c)
	if c.Writer.Written() && strings.HasPrefix(c.Writer.Header().Get("Content-Type"), "text/event-stream") {
		data, _ := json.Marshal(body)
		_, _ = fmt.Fprintf(c.Writer, "data: %s\n\n", data)
		c.Writer.Flush()
	} else {
		c.JSON(http.StatusBadGateway, body)
	}
	// The handler recognizes this already-communicated, non-failover error.
	return fmt.Errorf("upstream response failed: %s", message)
}
