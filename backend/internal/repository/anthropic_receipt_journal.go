package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

// V1 has no pre-dispatch claim. This envelope freezes only an already observed
// final, using the same private, bounded, fsynced journal as Chat receipts.
func anthropicReceiptEnvelope(key int64, receipt *service.UsageReceipt) (chatReceiptEnvelope, error) {
	if key <= 0 || !validAnthropicReceipt(receipt) {
		return chatReceiptEnvelope{}, service.ErrChatReceiptConflict
	}
	copy := *receipt
	e := chatReceiptEnvelope{State: "final", Receipt: &copy, Claim: &service.ChatUsageReceiptClaim{
		APIKeyID: key, RequestID: copy.RequestID, Token: copy.RequestID,
		Model: copy.Model, Endpoint: copy.InboundEndpoint, PayloadHash: anthropicReceiptHash(copy),
	}}
	if !validJournalClaim(e.Claim) {
		return chatReceiptEnvelope{}, service.ErrChatReceiptConflict
	}
	return e, nil
}

func validAnthropicReceipt(r *service.UsageReceipt) bool {
	if r == nil || r.Object != "sub2api.usage_receipt" || r.Version != 1 || r.Source != "wire_final" ||
		!service.ValidUsageReceiptID(r.RequestID) || r.Model == "" || r.InboundEndpoint != "/v1/messages" ||
		r.BillingMode != "token" || r.RecordedAt.IsZero() || r.TerminalState != "" || r.HTTPStatus != 0 ||
		r.InputTokens < 0 || r.OutputTokens < 0 || r.CacheReadTokens < 0 || r.CacheCreationTokens < 0 ||
		r.CacheCreation5mTokens < 0 || r.CacheCreation1hTokens < 0 || r.WebSearchCalls < 0 || r.ReasoningTokens != 0 {
		return false
	}
	return r.CacheCreation5mTokens <= r.CacheCreationTokens && r.CacheCreation1hTokens <= r.CacheCreationTokens-r.CacheCreation5mTokens
}

func anthropicReceiptHash(r service.UsageReceipt) string {
	// Retrying persistence can observe the same final at a later time; it must
	// retain the first stored timestamp without making a different meter equal.
	r.RecordedAt = time.Time{}
	data, _ := json.Marshal(r)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func validAnthropicJournalEnvelope(e chatReceiptEnvelope) bool {
	return e.State == "final" && e.AccountID == 0 && e.BillingRequestID == "" &&
		validAnthropicReceipt(e.Receipt) && e.Claim.Token == e.Claim.RequestID &&
		e.Receipt.RequestID == e.Claim.RequestID && e.Receipt.Model == e.Claim.Model &&
		e.Claim.PayloadHash == anthropicReceiptHash(*e.Receipt)
}

func (j *chatReceiptJournal) persistAnthropicFinal(e chatReceiptEnvelope) (chatReceiptEnvelope, error) {
	if err := j.reserve(e.Claim); err == nil {
		if err = j.finalize(e); !errors.Is(err, service.ErrChatReceiptConflict) {
			return e, err
		}
	} else if !errors.Is(err, service.ErrChatReceiptConflict) {
		return e, err
	}
	// A concurrent writer may have finished first. Only the same bound meter is
	// idempotent; never overwrite it or fabricate a missing final.
	prior, err := j.load(e.Claim.APIKeyID, e.Claim.RequestID)
	if err != nil {
		return e, err
	}
	if prior == nil || !validAnthropicJournalEnvelope(*prior) || *prior.Claim != *e.Claim {
		return e, service.ErrChatReceiptConflict
	}
	return *prior, nil
}

func (r *usageLogRepository) saveAnthropicReceipt(ctx context.Context, e chatReceiptEnvelope) error {
	payload, err := json.Marshal(e.Receipt)
	if err != nil {
		return err
	}
	_, err = r.sql.ExecContext(ctx, `INSERT INTO provider_usage_receipts (api_key_id,request_id,payload,created_at)
 VALUES ($1,$2,$3,$4) ON CONFLICT (api_key_id,request_id) DO NOTHING`, e.Claim.APIKeyID, e.Claim.RequestID, string(payload), e.Receipt.RecordedAt)
	if err != nil {
		return err
	}
	rows, err := r.sql.QueryContext(ctx, `SELECT (payload-'recorded_at')=($3::jsonb-'recorded_at') FROM provider_usage_receipts WHERE api_key_id=$1 AND request_id=$2`, e.Claim.APIKeyID, e.Claim.RequestID, string(payload))
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
	if err := rows.Scan(&equal); err != nil {
		return err
	}
	if !equal {
		return service.ErrChatReceiptConflict
	}
	return nil
}

func (r *usageLogRepository) persistAnthropicReceipt(ctx context.Context, key int64, receipt *service.UsageReceipt) error {
	e, err := anthropicReceiptEnvelope(key, receipt)
	if err != nil {
		return err
	}
	journalErr := r.anthropicJournalErr
	if r.anthropicJournal != nil {
		e, journalErr = r.anthropicJournal.persistAnthropicFinal(e)
	}
	if errors.Is(journalErr, service.ErrChatReceiptConflict) {
		return journalErr
	}
	if err := r.saveAnthropicReceipt(ctx, e); err != nil {
		return errors.Join(err, journalErr)
	}
	if journalErr == nil {
		r.removeAnthropicJournal(e)
	} else {
		slog.Error("provider_usage_receipt_journal_failed_pg_confirmed", "api_key_id", key, "request_id", receipt.RequestID)
	}
	return nil
}

func (r *usageLogRepository) removeAnthropicJournal(e chatReceiptEnvelope) {
	if r.anthropicJournal != nil {
		if err := r.anthropicJournal.remove(e); err != nil {
			slog.Error("provider_usage_receipt_journal_cleanup_failed", "api_key_id", e.Claim.APIKeyID, "request_id", e.Claim.RequestID)
		}
	}
}

func (r *usageLogRepository) recoverAnthropicReceipt(ctx context.Context, key int64, id string, receipt *service.UsageReceipt, readErr error) (*service.UsageReceipt, error) {
	if r.anthropicJournal == nil {
		return receipt, readErr
	}
	final, err := r.anthropicJournal.load(key, id)
	if err != nil {
		if receipt != nil && !errors.Is(err, service.ErrChatReceiptConflict) {
			slog.Error("provider_usage_receipt_journal_read_failed_pg_available", "api_key_id", key, "request_id", id)
			return receipt, nil
		}
		return nil, err
	}
	if final == nil {
		if receipt == nil {
			return r.readUsageReceipt(ctx, key, id)
		}
		return receipt, readErr
	}
	if !validAnthropicJournalEnvelope(*final) {
		return nil, service.ErrChatReceiptConflict
	}
	if err := r.saveAnthropicReceipt(ctx, *final); err != nil {
		return nil, err
	}
	r.removeAnthropicJournal(*final)
	return final.Receipt, nil
}
