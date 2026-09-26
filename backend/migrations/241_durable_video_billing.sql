CREATE TABLE IF NOT EXISTS pending_video_billing (
    task_id TEXT NOT NULL,
    api_key_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    account_id BIGINT NOT NULL,
    group_id BIGINT,
    snapshot JSONB NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'settled', 'failed', 'review')),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_check_at TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '30 seconds',
    lease_until TIMESTAMPTZ,
    last_error VARCHAR(512) NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (task_id, api_key_id)
);
CREATE INDEX IF NOT EXISTS pending_video_billing_due_idx
    ON pending_video_billing (next_check_at) WHERE state = 'pending';
