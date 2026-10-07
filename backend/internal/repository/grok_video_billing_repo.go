package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

var _ service.VideoBillingRepository = (*usageBillingRepository)(nil)

func (r *usageBillingRepository) StoreVideoBilling(ctx context.Context, task string, pending service.GrokVideoPendingBilling) error {
	owner := pending.Owner
	if task == "" || owner == nil || owner.UserID <= 0 || owner.APIKeyID <= 0 || owner.AccountID <= 0 || (owner.GroupID != nil && *owner.GroupID <= 0) {
		return errors.New("video billing owner missing")
	}
	payload, err := json.Marshal(pending)
	if err != nil {
		return err
	}
	_, err = r.db.ExecContext(ctx, `INSERT INTO pending_video_billing (task_id,api_key_id,user_id,account_id,group_id,snapshot)
		VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (task_id,api_key_id) DO NOTHING`, task, owner.APIKeyID, owner.UserID, owner.AccountID, owner.GroupID, payload)
	if err != nil {
		return err
	}
	saved, err := r.GetVideoBilling(ctx, task, owner.UserID, owner.APIKeyID)
	if err != nil {
		return err
	}
	if saved == nil || saved.Snapshot.Owner == nil || saved.Snapshot.Owner.AccountID != owner.AccountID || !videoBillingGroupEqual(saved.Snapshot.Owner.GroupID, owner.GroupID) {
		return errors.New("video billing receipt owner conflict")
	}
	return nil
}

func videoBillingGroupEqual(a, b *int64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func (r *usageBillingRepository) GetVideoBilling(ctx context.Context, task string, userID, keyID int64) (*service.PendingVideoBilling, error) {
	var job service.PendingVideoBilling
	var payload []byte
	err := r.db.QueryRowContext(ctx, `SELECT task_id,snapshot,state,attempts,created_at FROM pending_video_billing
		WHERE task_id=$1 AND user_id=$2 AND api_key_id=$3`, task, userID, keyID).Scan(&job.TaskID, &payload, &job.State, &job.Attempts, &job.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(payload, &job.Snapshot); err != nil {
		return nil, err
	}
	return &job, nil
}

func (r *usageBillingRepository) ClaimVideoBillingDue(ctx context.Context, limit int) ([]service.PendingVideoBilling, error) {
	if limit < 1 || limit > 10 {
		return nil, fmt.Errorf("invalid video billing batch size")
	}
	rows, err := r.db.QueryContext(ctx, `WITH due AS (
		SELECT task_id,api_key_id FROM pending_video_billing WHERE state='pending' AND next_check_at<=NOW()
		AND (lease_until IS NULL OR lease_until<NOW()) ORDER BY next_check_at LIMIT $1 FOR UPDATE SKIP LOCKED
	) UPDATE pending_video_billing p SET lease_until=NOW()+INTERVAL '5 minutes',attempts=p.attempts+1,updated_at=NOW()
	FROM due WHERE p.task_id=due.task_id AND p.api_key_id=due.api_key_id
	RETURNING p.task_id,p.snapshot,p.state,p.attempts,p.created_at`, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var jobs []service.PendingVideoBilling
	for rows.Next() {
		var job service.PendingVideoBilling
		var payload []byte
		if err := rows.Scan(&job.TaskID, &payload, &job.State, &job.Attempts, &job.CreatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(payload, &job.Snapshot); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (r *usageBillingRepository) UpdateVideoBilling(ctx context.Context, task string, keyID int64, state string, next time.Time, message string) error {
	// Provider errors are not stored: preserve only a bounded internal reason code.
	if len(message) > 128 {
		message = message[:128]
	}
	_, err := r.db.ExecContext(ctx, `UPDATE pending_video_billing SET state=$3,next_check_at=$4,lease_until=NULL,
		last_error=$5,updated_at=NOW() WHERE task_id=$1 AND api_key_id=$2 AND state<>'settled'
		AND (state='pending' OR $3='settled')`, task, keyID, state, next, message)
	return err
}
