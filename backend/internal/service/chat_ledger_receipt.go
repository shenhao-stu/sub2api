package service

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type chatLedgerCloseout struct {
	claim     *ChatUsageReceiptClaim
	accountID int64
	terminal  string
	status    int
}

// ChatLedgerReceipt closes a claim in the same transaction as the Sub2API debit.
type ChatLedgerReceipt struct {
	Claim   *ChatUsageReceiptClaim
	Receipt *UsageReceipt
}

func geminiChatLedgerModel(model string) bool {
	return model == "gemini-4-argon" || model == "gemini-3.8-flash"
}

func chatLedgerCloseoutFor(capture *chatReceiptCapture, c *gin.Context, account *Account, result *OpenAIForwardResult, forwardErr error) *chatLedgerCloseout {
	if !capture.ledgerTerminal || capture.claim == nil || !geminiChatLedgerModel(capture.claim.Model) ||
		!strings.EqualFold(UpstreamRequestIDHeaderName(account), "X-CPA-Request-ID") ||
		!ValidUsageReceiptID(UpstreamRequestIDFromHeaders(account, result.UpstreamHeaders)) ||
		result.VideoCount != 0 || result.SearchCount != 0 || result.ClientDisconnect {
		return nil
	}
	closeout := &chatLedgerCloseout{claim: capture.claim, accountID: account.ID, terminal: capture.terminal, status: http.StatusOK}
	if forwardErr != nil || closeout.terminal == "failed" {
		closeout.terminal, closeout.status = "failed", http.StatusBadGateway
		if status := c.Writer.Status(); status >= 400 && status <= 599 {
			closeout.status = status
		}
		if value, ok := c.Get(OpsStreamErrorKey); ok {
			if e, ok := value.(OpsStreamError); ok && e.IntendedStatus >= 400 && e.IntendedStatus <= 599 {
				closeout.status = e.IntendedStatus
			}
		}
	}
	return closeout
}

func (result *OpenAIForwardResult) HasChatLedgerCloseout() bool {
	return result != nil && result.chatLedgerCloseout != nil
}

func (result *OpenAIForwardResult) chatLedgerReceipt(log *UsageLog, cost *CostBreakdown) *ChatLedgerReceipt {
	closeout := result.chatLedgerCloseout
	if closeout == nil || cost.ActualCost <= 0 || log.APIKeyID != closeout.claim.APIKeyID || log.AccountID != closeout.accountID ||
		log.RequestedModel != closeout.claim.Model || log.InboundEndpoint == nil || *log.InboundEndpoint != closeout.claim.Endpoint ||
		log.BillingMode == nil || *log.BillingMode != "token" || log.ImageInputTokens != 0 ||
		log.ImageOutputTokens != 0 || log.CacheCreation5mTokens != 0 || log.CacheCreation1hTokens != 0 {
		return nil
	}
	receipt := &UsageReceipt{
		Object: "sub2api.usage_receipt", Version: 2, Source: ChatLedgerReceiptSource,
		RequestID: closeout.claim.RequestID, Model: closeout.claim.Model, InboundEndpoint: closeout.claim.Endpoint,
		BillingMode: "token", RecordedAt: time.Now().UTC(), TerminalState: closeout.terminal, HTTPStatus: closeout.status,
		InputTokens: log.InputTokens, OutputTokens: log.OutputTokens, CacheReadTokens: log.CacheReadTokens,
		CacheCreationTokens: log.CacheCreationTokens, ServiceTier: optionalStringValue(log.ServiceTier),
	}
	if !ValidateChatUsageReceipt(receipt) {
		return nil
	}
	return &ChatLedgerReceipt{Claim: closeout.claim, Receipt: receipt}
}
