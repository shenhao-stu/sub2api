package repository

import (
	"context"
	"encoding/json"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

func (r *usageLogRepository) GetUsageReceipt(ctx context.Context, apiKeyID int64, requestID string) (*service.UsageReceipt, error) {
	if apiKeyID <= 0 || !service.ValidUsageReceiptID(requestID) {
		return nil, service.ErrUsageLogNotFound
	}
	// This runs only behind the existing authenticated key-scoped endpoint.
	// Recovery performs the original immutable CAS; it never dispatches a model,
	// creates a claim, changes prices or invokes the usage billing pipeline.
	if r.chatJournal != nil {
		final, err := r.chatJournal.load(apiKeyID, requestID)
		if err != nil {
			return nil, err
		}
		if final != nil {
			if err = r.completeChatReceiptEnvelope(ctx, *final); err != nil {
				return nil, err
			}
			if err = r.chatJournal.remove(*final); err != nil {
				return nil, err
			}
		}
	}
	var payload []byte
	rows, err := r.sql.QueryContext(ctx, `SELECT payload FROM provider_usage_receipts WHERE api_key_id=$1 AND request_id=$2`, apiKeyID, requestID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, service.ErrUsageLogNotFound
	}
	if err = rows.Scan(&payload); err != nil {
		return nil, err
	}
	return decodeProviderReceipt(payload, apiKeyID, requestID)
}

func (r *usageLogRepository) SaveUsageReceipt(ctx context.Context, apiKeyID int64, receipt *service.UsageReceipt) error {
	payload, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	_, err = r.sql.ExecContext(ctx, `INSERT INTO provider_usage_receipts (api_key_id,request_id,payload,created_at)
 VALUES ($1,$2,$3,$4) ON CONFLICT (api_key_id,request_id) DO NOTHING`, apiKeyID, receipt.RequestID, string(payload), receipt.RecordedAt)
	return err
}
