package repository

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

func (r *usageLogRepository) GetUsageReceipt(ctx context.Context, apiKeyID int64, requestID string) (*service.UsageReceipt, error) {
	if apiKeyID <= 0 || !service.ValidUsageReceiptID(requestID) {
		return nil, service.ErrUsageLogNotFound
	}
	// Existing PG evidence remains authoritative when the new journal has a
	// filesystem failure. Do not make healthy v1/v2 reads depend on local disk.
	receipt, readErr := r.readUsageReceipt(ctx, apiKeyID, requestID)
	if readErr != nil && !errors.Is(readErr, service.ErrUsageLogNotFound) {
		return nil, readErr
	}
	// This runs only behind the existing authenticated key-scoped endpoint.
	// Recovery performs the original immutable CAS; it never dispatches a model,
	// creates a claim, changes prices or invokes the usage billing pipeline.
	if r.chatJournal != nil {
		final, err := r.chatJournal.load(apiKeyID, requestID)
		if err != nil {
			if receipt != nil && !errors.Is(err, service.ErrChatReceiptConflict) {
				slog.Error("provider_chat_usage_receipt_journal_read_failed_pg_available", "api_key_id", apiKeyID, "request_id", requestID)
				return receipt, nil
			}
			return nil, err
		}
		if final != nil {
			if receipt != nil {
				err = r.confirmChatReceiptEnvelope(ctx, *final)
			} else {
				err = r.completeChatReceiptEnvelope(ctx, *final)
			}
			if err != nil {
				return nil, err
			}
			r.removeConfirmedChatJournal(*final)
			return final.Receipt, nil
		}
		if receipt == nil {
			// Another authenticated reader may have committed and removed the
			// journal after our first pending snapshot. Read once more; never
			// recreate the claim or reuse the stale not-found observation.
			return r.readUsageReceipt(ctx, apiKeyID, requestID)
		}
	}
	return receipt, readErr
}

func (r *usageLogRepository) readUsageReceipt(ctx context.Context, apiKeyID int64, requestID string) (*service.UsageReceipt, error) {
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
