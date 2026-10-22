-- Immutable, key-scoped final provider meters, separate from pricing policies.
-- Additive: old binaries can continue using the existing usage ledger unchanged.
CREATE TABLE IF NOT EXISTS provider_usage_receipts (
 api_key_id BIGINT NOT NULL,
 request_id UUID NOT NULL,
 payload JSONB NOT NULL,
 created_at TIMESTAMPTZ NOT NULL,
 PRIMARY KEY (api_key_id,request_id)
);
