package repository

import (
	"context"
	"database/sql"
	"errors"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

// The usage row and the billing transaction are written separately. Require both
// for this exact key and server-generated request id. A zero-cost estimate, an
// absent row, or a pending wire receipt alone cannot close a reservation.
func (r *usageLogRepository) GetResponsesLedgerReceipt(ctx context.Context, key int64, id string) (*service.UsageReceipt, error) {
	const query = `SELECT u.model, u.input_tokens, u.output_tokens, u.cache_read_tokens,
     COALESCE(u.service_tier,''),u.created_at
 FROM usage_logs u WHERE u.api_key_id=$1 AND u.request_id='client:'||$2
 AND u.model='gpt-6.1-sol' AND u.requested_model=u.model AND u.upstream_model=u.model
 AND u.inbound_endpoint='/v1/responses' AND u.upstream_endpoint=u.inbound_endpoint
 AND u.stream AND u.billing_mode='token' AND NOT u.openai_ws_mode
 AND u.cache_creation_tokens=0 AND u.cache_creation_5m_tokens=0 AND u.cache_creation_1h_tokens=0
 AND u.cache_creation_30m_tokens=0 AND u.cache_creation_12h_tokens=0
 AND u.image_input_tokens=0 AND u.image_output_tokens=0 AND u.image_count=0 AND u.video_count=0
 AND u.input_tokens>=0 AND u.output_tokens>=0 AND u.cache_read_tokens>=0
 AND (EXISTS (SELECT 1 FROM usage_billing_dedup d WHERE d.api_key_id=u.api_key_id AND d.request_id=u.request_id)
  OR EXISTS (SELECT 1 FROM usage_billing_dedup_archive d WHERE d.api_key_id=u.api_key_id AND d.request_id=u.request_id))
 AND ((u.actual_cost>0 AND u.total_cost>0 AND GREATEST(u.input_tokens,u.output_tokens,u.cache_read_tokens)>0)
   OR (u.actual_cost=0 AND u.total_cost=0 AND u.input_cost=0 AND u.output_cost=0 AND u.cache_read_cost=0 AND u.cache_creation_cost=0
    AND u.input_tokens=0 AND u.output_tokens=0 AND u.cache_read_tokens=0
    AND EXISTS (SELECT 1 FROM ops_error_logs e WHERE e.client_request_id=$2 AND e.api_key_id=u.api_key_id AND e.group_id=u.group_id
      AND e.account_id=u.account_id AND e.model=u.model AND e.status_code BETWEEN 500 AND 599)))
 AND NOT EXISTS (SELECT 1 FROM provider_usage_receipts p WHERE p.api_key_id=u.api_key_id AND p.request_id=$2::text::uuid)`
	receipt := service.UsageReceipt{Version: 4, Object: "sub2api.usage_receipt", Source: service.ChatLedgerReceiptSource, RequestID: id, InboundEndpoint: "/v1/responses", BillingMode: "token", TerminalState: "ledger_committed"}
	err := scanSingleRow(ctx, r.sql, query, []any{key, id}, &receipt.Model, &receipt.InputTokens, &receipt.OutputTokens, &receipt.CacheReadTokens, &receipt.ServiceTier, &receipt.RecordedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrUsageLogNotFound
	}
	if err != nil {
		return nil, err
	}
	return &receipt, nil
}
