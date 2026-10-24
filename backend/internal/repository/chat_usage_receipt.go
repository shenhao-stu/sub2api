package repository

import (
	"context"
	"encoding/json"
	"errors"
	"os"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

type chatReceiptEnvelope struct {
	State            string                         `json:"state"`
	Claim            *service.ChatUsageReceiptClaim `json:"claim"`
	BillingRequestID string                         `json:"billing_request_id,omitempty"`
	AccountID        int64                          `json:"account_id,omitempty"`
	Receipt          *service.UsageReceipt          `json:"receipt,omitempty"`
}

func (r *usageLogRepository) ClaimChatUsageReceipt(ctx context.Context, claim *service.ChatUsageReceiptClaim) (bool, error) {
	if claim == nil || claim.APIKeyID <= 0 || !service.ValidUsageReceiptID(claim.RequestID) || !service.ValidUsageReceiptID(claim.Token) || claim.Endpoint != service.ChatReceiptEndpoint || claim.Model == "" || len(claim.PayloadHash) != 64 {
		return false, service.ErrChatReceiptUnsupported
	}
	if err := r.PrepareChatUsageReceipt(ctx, claim); err != nil {
		return false, err
	}
	payload, err := json.Marshal(chatReceiptEnvelope{State: "pending", Claim: claim})
	if err != nil {
		return false, err
	}
	result, err := r.sql.ExecContext(ctx, `INSERT INTO provider_usage_receipts(api_key_id,request_id,payload,created_at) VALUES($1,$2,$3,NOW()) ON CONFLICT(api_key_id,request_id) DO NOTHING`, claim.APIKeyID, claim.RequestID, string(payload))
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	if err == nil && count != 1 {
		_ = r.ReleaseChatUsageReceipt(ctx, claim)
	}
	return count == 1, err
}

func (r *usageLogRepository) CompleteChatUsageReceipt(ctx context.Context, claim *service.ChatUsageReceiptClaim, accountID int64, billingRequestID string, receipt *service.UsageReceipt) error {
	if claim == nil || !service.ValidateChatUsageReceipt(receipt) || accountID <= 0 || billingRequestID == "" || receipt.Version != 2 || receipt.RequestID != claim.RequestID || receipt.Model != claim.Model || receipt.InboundEndpoint != claim.Endpoint {
		return service.ErrChatReceiptConflict
	}
	final := chatReceiptEnvelope{State: "final", Claim: claim, AccountID: accountID, BillingRequestID: billingRequestID, Receipt: receipt}
	if r.chatJournalErr != nil {
		return r.chatJournalErr
	}
	if r.chatJournal != nil {
		if err := r.chatJournal.finalize(final); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return r.confirmChatReceiptEnvelope(ctx, final)
			}
			return err
		}
	}
	if err := r.completeChatReceiptEnvelope(ctx, final); err != nil {
		return err
	}
	if r.chatJournal != nil {
		return r.chatJournal.remove(final)
	}
	return nil
}

func (r *usageLogRepository) PrepareChatUsageReceipt(_ context.Context, claim *service.ChatUsageReceiptClaim) error {
	if r.chatJournalErr != nil {
		return r.chatJournalErr
	}
	if r.chatJournal != nil {
		return r.chatJournal.reserve(claim)
	}
	return nil
}

func (r *usageLogRepository) ReleaseChatUsageReceipt(_ context.Context, claim *service.ChatUsageReceiptClaim) error {
	if r.chatJournal != nil {
		return r.chatJournal.remove(chatReceiptEnvelope{State: "pending", Claim: claim})
	}
	return nil
}

func (r *usageLogRepository) completeChatReceiptEnvelope(ctx context.Context, final chatReceiptEnvelope) error {
	claim := final.Claim
	payload, err := json.Marshal(final)
	if err != nil {
		return err
	}
	claimJSON, err := json.Marshal(claim)
	if err != nil {
		return err
	}
	result, err := r.sql.ExecContext(ctx, `UPDATE provider_usage_receipts SET payload=$3 WHERE api_key_id=$1 AND request_id=$2 AND payload->>'state'='pending' AND payload->'claim'=$4::jsonb`, claim.APIKeyID, claim.RequestID, string(payload), string(claimJSON))
	if err != nil {
		return err
	}
	_, err = result.RowsAffected()
	if err != nil {
		return err
	}
	// Read back even after an acknowledged UPDATE. This also handles an earlier
	// successful commit whose response was lost; only exact equality permits
	// removal of the immutable journal entry.
	return r.confirmChatReceiptEnvelope(ctx, final)
}

func (r *usageLogRepository) confirmChatReceiptEnvelope(ctx context.Context, final chatReceiptEnvelope) error {
	payload, err := json.Marshal(final)
	if err != nil {
		return err
	}
	claim := final.Claim
	rows, err := r.sql.QueryContext(ctx, `SELECT payload=$3::jsonb FROM provider_usage_receipts WHERE api_key_id=$1 AND request_id=$2`, claim.APIKeyID, claim.RequestID, string(payload))
	if err != nil {
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return service.ErrChatReceiptConflict
	}
	var equal bool
	if err = rows.Scan(&equal); err != nil {
		return err
	}
	if !equal {
		return service.ErrChatReceiptConflict
	}
	return nil
}

func decodeProviderReceipt(payload []byte, apiKeyID int64, requestID string) (*service.UsageReceipt, error) {
	var envelope chatReceiptEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil, err
	}
	if envelope.State != "" {
		if envelope.State == "pending" {
			return nil, service.ErrUsageLogNotFound
		}
		if envelope.State != "final" || envelope.Receipt == nil || envelope.Claim == nil || envelope.AccountID <= 0 || envelope.BillingRequestID == "" || envelope.Claim.APIKeyID != apiKeyID || envelope.Claim.RequestID != requestID {
			return nil, errors.New("invalid receipt envelope")
		}
		receipt := envelope.Receipt
		if receipt.Version != 2 || receipt.RequestID != envelope.Claim.RequestID || receipt.Model != envelope.Claim.Model || receipt.InboundEndpoint != envelope.Claim.Endpoint {
			return nil, errors.New("receipt binding mismatch")
		}
		return receipt, nil
	}
	var receipt service.UsageReceipt
	if err := json.Unmarshal(payload, &receipt); err != nil {
		return nil, err
	}
	return &receipt, nil
}
