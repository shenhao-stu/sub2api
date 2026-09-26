package service

import (
	"context"
	"errors"
	"strings"
	"time"
)

// VideoBillingOwner contains only billing identifiers and key limits, never credentials.
type VideoBillingOwner struct {
	UserID         int64   `json:"user_id"`
	APIKeyID       int64   `json:"api_key_id"`
	AccountID      int64   `json:"account_id"`
	GroupID        *int64  `json:"group_id,omitempty"`
	SubscriptionID *int64  `json:"subscription_id,omitempty"`
	Quota          float64 `json:"quota"`
	RateLimit5h    float64 `json:"rate_limit_5h"`
	RateLimit1d    float64 `json:"rate_limit_1d"`
	RateLimit7d    float64 `json:"rate_limit_7d"`
}

type PendingVideoBilling struct {
	TaskID    string
	Snapshot  GrokVideoPendingBilling
	State     string
	Attempts  int
	CreatedAt time.Time
}

type VideoBillingRepository interface {
	StoreVideoBilling(context.Context, string, GrokVideoPendingBilling) error
	GetVideoBilling(context.Context, string, int64, int64) (*PendingVideoBilling, error)
	ClaimVideoBillingDue(context.Context, int) ([]PendingVideoBilling, error)
	UpdateVideoBilling(context.Context, string, int64, string, time.Time, string) error
}

func (s *OpenAIGatewayService) VideoBillingRepository() VideoBillingRepository {
	if s == nil {
		return nil
	}
	repo, _ := s.usageBillingRepo.(VideoBillingRepository)
	return repo
}

type videoCreationReceiptKey struct{}

func WithVideoCreationReceipt(ctx context.Context, persist func(*OpenAIForwardResult) error) context.Context {
	return context.WithValue(ctx, videoCreationReceiptKey{}, persist)
}

func persistVideoCreationReceipt(ctx context.Context, result *OpenAIForwardResult) error {
	if persist, ok := ctx.Value(videoCreationReceiptKey{}).(func(*OpenAIForwardResult) error); ok {
		return persist(result)
	}
	return nil
}

func (s *OpenAIGatewayService) CompleteVideoBilling(ctx context.Context, input *OpenAIRecordUsageInput) error {
	if input == nil || input.Result == nil || input.APIKey == nil {
		return errors.New("video billing input missing")
	}
	if repo := s.VideoBillingRepository(); repo != nil {
		job, err := repo.GetVideoBilling(ctx, input.Result.ResponseID, input.APIKey.UserID, input.APIKey.ID)
		if err != nil {
			return err
		}
		if job != nil {
			if job.Snapshot.Owner == nil || input.Account == nil || job.Snapshot.Owner.AccountID != input.Account.ID || derefGroupID(job.Snapshot.Owner.GroupID) != derefGroupID(input.APIKey.GroupID) {
				return errors.New("video billing owner mismatch")
			}
			if job.State == "settled" {
				return nil
			}
			input.HistoricalVideo = true
		}
	}
	if err := s.RecordUsage(ctx, input); err != nil {
		return err
	}
	return nil
}

// MergeVideoCompletion is shared by client lookups and reconciliation so their
// durable request IDs, billable units and fingerprints cannot drift apart.
func MergeVideoCompletion(result *OpenAIForwardResult, pending *GrokVideoPendingBilling, taskID string) *OpenAIForwardResult {
	if result == nil {
		return nil
	}
	merged := *result
	if pending != nil {
		if merged.Model == "" {
			merged.Model = firstNonEmpty(pending.BillingModel, pending.Model, pending.OriginalModel)
		}
		merged.BillingModel = firstNonEmpty(pending.BillingModel, pending.Model, merged.BillingModel, merged.Model)
		if merged.UpstreamModel == "" {
			merged.UpstreamModel = pending.UpstreamModel
		}
		if pending.VideoResolution != "" {
			merged.VideoResolution = pending.VideoResolution
		}
		if merged.VideoDurationSeconds <= 0 {
			merged.VideoDurationSeconds = pending.VideoDurationSeconds
		}
		if d := GrokVideoE2EDuration(pending.CreatedAt, time.Now()); d > 0 {
			merged.Duration = d
		}
	}
	merged.ResponseID = strings.TrimSpace(taskID)
	merged.RequestID = StableGrokVideoBillingRequestID(taskID)
	if strings.HasPrefix(taskID, "seedance:") {
		if pending != nil {
			merged.Model = pending.Model
			merged.BillingModel = firstNonEmpty(pending.BillingModel, pending.Model)
			merged.UpstreamModel = firstNonEmpty(pending.UpstreamModel, result.UpstreamModel)
		}
		return &merged
	}
	merged.Model = firstNonEmpty(merged.Model, "grok-imagine-video")
	merged.BillingModel = firstNonEmpty(merged.BillingModel, merged.Model)
	merged.VideoCount = 1
	merged.ImageCount = 0
	merged.VideoResolution = NormalizeVideoBillingResolutionOrDefault(merged.VideoResolution)
	merged.VideoDurationSeconds = NormalizeVideoBillingDurationSecondsOrDefault(merged.VideoDurationSeconds)
	return &merged
}
