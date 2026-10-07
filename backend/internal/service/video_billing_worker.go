package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
)

// VideoBillingWorker only reads status for accepted tasks. It never generates,
// downloads artifacts, chooses another account, or modifies scheduling state.
type VideoBillingWorker struct {
	gateway    *OpenAIGatewayService
	repo       VideoBillingRepository
	keys       APIKeyRepository
	groups     GroupRepository
	keyService *APIKeyService
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

func ProvideVideoBillingWorker(gateway *OpenAIGatewayService, keys APIKeyRepository, groups GroupRepository, keyService *APIKeyService) *VideoBillingWorker {
	w := &VideoBillingWorker{gateway: gateway, repo: gateway.VideoBillingRepository(), keys: keys, groups: groups, keyService: keyService}
	if w.repo == nil {
		return w
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				w.runOnce(ctx)
			}
		}
	}()
	return w
}

func (w *VideoBillingWorker) Stop() {
	if w != nil && w.cancel != nil {
		w.cancel()
		w.wg.Wait()
	}
}

func (w *VideoBillingWorker) runOnce(ctx context.Context) {
	for range 5 {
		if ctx.Err() != nil {
			return
		}
		claimCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		jobs, err := w.repo.ClaimVideoBillingDue(claimCtx, 1)
		cancel()
		if err != nil {
			logger.L().Error("video_billing.claim_failed", zap.Error(err))
			return
		}
		if len(jobs) == 0 {
			return
		}
		w.process(ctx, jobs[0])
	}
}

func (w *VideoBillingWorker) process(parent context.Context, job PendingVideoBilling) {
	owner := job.Snapshot.Owner
	if owner == nil {
		logger.L().Error("video_billing.owner_missing", zap.String("task_id", job.TaskID))
		return
	}
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	state, reason := "pending", "status_pending"
	next := time.Now().Add(videoBillingRetryDelay(job.Attempts))
	defer func() {
		updateCtx, release := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
		defer release()
		if state == "pending" && time.Since(job.CreatedAt) > 7*24*time.Hour {
			state = "review"
		}
		if err := w.repo.UpdateVideoBilling(updateCtx, job.TaskID, owner.APIKeyID, state, next, reason); err != nil {
			logger.L().Error("video_billing.state_update_failed", zap.String("task_id", job.TaskID), zap.Error(err))
		}
		if state == "review" {
			logger.L().Error("video_billing.review_required", zap.String("task_id", job.TaskID), zap.String("reason", reason))
		}
	}()
	account, err := w.gateway.accountRepo.GetByID(ctx, owner.AccountID)
	if err != nil {
		reason = "account_unavailable"
		if time.Since(job.CreatedAt) > 7*24*time.Hour {
			state = "review"
		}
		return
	}
	result, status, err := w.poll(ctx, account, job.TaskID, &job.Snapshot)
	if err != nil {
		reason = "status_read_failed"
		if time.Since(job.CreatedAt) > 7*24*time.Hour {
			state = "review"
		}
		return
	}
	if result == nil || (result.VideoCount <= 0 && (!strings.HasPrefix(job.TaskID, "seedance:") || result.Usage.OutputTokens <= 0)) {
		switch status {
		case "failed", "canceled", "cancelled":
			state, reason = "failed", "provider_terminal_failure"
			return
		case "expired":
			state, reason = "review", "expired_without_completion_evidence"
			return
		}
		if time.Since(job.CreatedAt) > 7*24*time.Hour {
			state, reason = "review", "completion_not_observed"
		}
		return
	}
	key, err := w.keys.GetByID(ctx, owner.APIKeyID)
	if errors.Is(err, ErrAPIKeyNotFound) {
		key = &APIKey{ID: owner.APIKeyID, UserID: owner.UserID, Quota: owner.Quota, RateLimit5h: owner.RateLimit5h, RateLimit1d: owner.RateLimit1d, RateLimit7d: owner.RateLimit7d}
	} else if err != nil {
		reason = "billing_key_read_failed"
		return
	}
	if key.UserID != owner.UserID {
		state, reason = "review", "billing_owner_mismatch"
		return
	}
	var group *Group
	if owner.GroupID != nil {
		group, err = w.groups.GetByID(ctx, *owner.GroupID)
		if err != nil {
			reason = "billing_group_read_failed"
			return
		}
	}
	keyCopy := *key
	key = &keyCopy
	key.GroupID = owner.GroupID
	key.Group = group
	key.User = &User{ID: owner.UserID}
	var subscription *UserSubscription
	if owner.SubscriptionID != nil {
		subscription = &UserSubscription{ID: *owner.SubscriptionID, UserID: owner.UserID, GroupID: derefGroupID(owner.GroupID)}
	}
	result = MergeVideoCompletion(result, &job.Snapshot, job.TaskID)
	input := &OpenAIRecordUsageInput{Result: result, APIKey: key, User: key.User, Account: account, Subscription: subscription,
		APIKeyService: w.keyService, QuotaPlatform: account.Platform, RequestPayloadHash: HashUsageRequestPayload([]byte(job.TaskID)),
		InboundEndpoint: "/v1/videos/:request_id", UpstreamEndpoint: "/v1/videos/:request_id",
		ChannelUsageFields: ChannelUsageFields{OriginalModel: job.Snapshot.OriginalModel, ChannelMappedModel: result.Model}}
	if err := w.gateway.CompleteVideoBilling(ctx, input); err != nil {
		reason = "durable_billing_failed"
		logger.L().Error("video_billing.record_failed", zap.String("task_id", job.TaskID), zap.Error(err))
		return
	}
	state, reason = "settled", ""
}

func videoBillingRetryDelay(attempts int) time.Duration {
	return time.Duration(min(max(attempts, 1), 30)) * 30 * time.Second
}

func (w *VideoBillingWorker) poll(ctx context.Context, account *Account, task string, pending *GrokVideoPendingBilling) (*OpenAIForwardResult, string, error) {
	seedance := strings.HasPrefix(task, "seedance:")
	var target string
	var err error
	if seedance {
		var base string
		base, err = w.gateway.validateUpstreamBaseURL(account.GetCredential("base_url"))
		if err == nil {
			target, err = buildSeedanceURL(base, SeedanceEndpointStatus, strings.TrimPrefix(task, "seedance:"))
		}
	} else {
		target, err = buildGrokMediaURL(account, w.gateway.cfg, GrokMediaEndpointVideoStatus, task)
	}
	if err != nil {
		return nil, "", err
	}
	token, _, err := w.gateway.GetAccessToken(ctx, account)
	if err != nil {
		return nil, "", err
	}
	req, err := http.NewRequestWithContext(WithHTTPUpstreamRedirectsDisabled(ctx), http.MethodGet, target, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	if account.IsGrokOAuth() && isGrokCLIProxyTarget(target) {
		applyGrokCLIHeaders(req.Header)
	}
	account.ApplyHeaderOverrides(req.Header)
	proxy := ""
	if account.ProxyID != nil && account.Proxy != nil {
		proxy = account.Proxy.URL()
	}
	resp, err := w.gateway.httpUpstream.Do(req, proxy, account.ID, account.Concurrency)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", fmt.Errorf("video status HTTP %d", resp.StatusCode)
	}
	body, err := readUpstreamResponseBodyLimited(resp.Body, 1<<20)
	if err != nil {
		return nil, "", err
	}
	status := strings.TrimSpace(gjson.GetBytes(body, "status").String())
	if seedance {
		output := 0
		if status == "succeeded" {
			output = max(0, int(gjson.GetBytes(body, "usage.completion_tokens").Int()))
		}
		return &OpenAIForwardResult{ResponseID: task, UpstreamModel: gjson.GetBytes(body, "model").String(),
			Usage: OpenAIUsage{OutputTokens: output}}, status, nil
	}
	return ExtractGrokVideoBillingFromStatusBody(body, pending, task), status, nil
}
