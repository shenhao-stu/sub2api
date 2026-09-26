//go:build unit

package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/stretchr/testify/require"
)

type videoBillingMemory struct {
	UsageBillingRepository
	mu       sync.Mutex
	job      PendingVideoBilling
	commands []*UsageBillingCommand
	dedup    map[string]string
}

func (r *videoBillingMemory) StoreVideoBilling(_ context.Context, task string, p GrokVideoPendingBilling) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.job = PendingVideoBilling{TaskID: task, Snapshot: p, State: "pending", CreatedAt: time.Now()}
	return nil
}
func (r *videoBillingMemory) GetVideoBilling(_ context.Context, task string, user, key int64) (*PendingVideoBilling, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.job.TaskID != task || r.job.Snapshot.Owner == nil || r.job.Snapshot.Owner.UserID != user || r.job.Snapshot.Owner.APIKeyID != key {
		return nil, nil
	}
	copy := r.job
	return &copy, nil
}
func (r *videoBillingMemory) ClaimVideoBillingDue(context.Context, int) ([]PendingVideoBilling, error) {
	return nil, nil
}
func (r *videoBillingMemory) UpdateVideoBilling(_ context.Context, _ string, _ int64, state string, _ time.Time, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.job.State != "settled" && (r.job.State == "pending" || state == "settled") {
		r.job.State = state
	}
	return nil
}
func (r *videoBillingMemory) Apply(_ context.Context, cmd *UsageBillingCommand) (*UsageBillingApplyResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cmd.Normalize()
	if r.dedup == nil {
		r.dedup = map[string]string{}
	}
	if fingerprint, ok := r.dedup[cmd.RequestID]; ok {
		if fingerprint != cmd.RequestFingerprint {
			return nil, ErrUsageBillingRequestConflict
		}
		return &UsageBillingApplyResult{}, nil
	}
	r.dedup[cmd.RequestID] = cmd.RequestFingerprint
	r.commands = append(r.commands, cmd)
	if cmd.HistoricalVideo {
		r.job.State = "settled"
	}
	return &UsageBillingApplyResult{Applied: true}, nil
}

type videoBillingAccountRepo struct {
	AccountRepository
	account *Account
}

func (r videoBillingAccountRepo) GetByID(context.Context, int64) (*Account, error) {
	return r.account, nil
}

type videoBillingKeyRepo struct {
	APIKeyRepository
	key     *APIKey
	deleted bool
}

func (r videoBillingKeyRepo) GetByID(context.Context, int64) (*APIKey, error) {
	if r.deleted {
		return nil, ErrAPIKeyNotFound
	}
	return r.key, nil
}

type videoBillingGroupRepo struct {
	GroupRepository
	group *Group
}

func (r videoBillingGroupRepo) GetByID(context.Context, int64) (*Group, error) {
	if r.group == nil {
		return nil, errors.New("nil group must not be loaded")
	}
	return r.group, nil
}

func newVideoBillingWorkerFixture(t *testing.T, grouped bool) (*VideoBillingWorker, *videoBillingMemory) {
	t.Helper()
	owner := &VideoBillingOwner{UserID: 1, APIKeyID: 10, AccountID: 20, Quota: 10}
	key := &APIKey{ID: 10, UserID: 1, Quota: 10, User: &User{ID: 1}}
	if grouped {
		id := int64(30)
		owner.GroupID = &id
		key.GroupID = &id
		key.Group = &Group{ID: id, Platform: PlatformGrok, RateMultiplier: 1}
	}
	repo := &videoBillingMemory{job: PendingVideoBilling{TaskID: "task", State: "pending", CreatedAt: time.Now(), Snapshot: GrokVideoPendingBilling{
		Owner: owner, Model: "grok-imagine-video", BillingModel: "grok-imagine-video", UpstreamModel: "grok-imagine-video", OriginalModel: "grok-imagine-video", VideoResolution: "720p", VideoDurationSeconds: 12, CreatedAt: time.Now().Add(-48 * time.Hour).Format(time.RFC3339Nano)}}}
	cfg := &config.Config{}
	cfg.Default.RateMultiplier = 1
	account := &Account{ID: 20, Platform: PlatformGrok, Type: AccountTypeAPIKey, Credentials: map[string]any{"api_key": "fixture", "base_url": "https://api.x.ai/v1"}}
	svc := &OpenAIGatewayService{cfg: cfg, cache: &stubGatewayCache{}, accountRepo: videoBillingAccountRepo{account: account}, usageBillingRepo: repo,
		billingService: NewBillingService(cfg, nil), deferredService: &DeferredService{}}
	return &VideoBillingWorker{gateway: svc, repo: repo, keys: videoBillingKeyRepo{key: key}, groups: videoBillingGroupRepo{group: key.Group}}, repo
}

func TestVideoBillingReconcilesWithoutClientPoll(t *testing.T) {
	for _, scenario := range []string{"grouped", "receipt model", "no group", "deleted key", "pending", "failed", "expired", "http404"} {
		t.Run(scenario, func(t *testing.T) {
			w, repo := newVideoBillingWorkerFixture(t, scenario != "no group")
			if scenario == "deleted key" {
				key := w.keys.(videoBillingKeyRepo)
				key.deleted = true
				w.keys = key
			}
			if scenario == "receipt model" {
				repo.job.Snapshot.Model = "grok-imagine-video-1.5"
				repo.job.Snapshot.BillingModel = "grok-imagine-video-1.5"
			}
			calls := 0
			w.gateway.httpUpstream = completionFailureUpstream{do: func(req *http.Request) (*http.Response, error) {
				calls++
				require.Equal(t, http.MethodGet, req.Method)
				require.Nil(t, req.Body)
				require.Equal(t, "/v1/videos/task", req.URL.Path)
				_, bounded := req.Context().Deadline()
				require.True(t, bounded)
				body := `{"status":"done","video":{"url":"https://vidgen.x.ai/test.mp4"}}`
				if scenario == "pending" || scenario == "failed" || scenario == "expired" {
					body = `{"status":"` + scenario + `"}`
				}
				status := 200
				if scenario == "http404" {
					status = 404
				}
				return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, nil
			}}
			w.process(context.Background(), repo.job)
			require.Equal(t, 1, calls)
			switch scenario {
			case "pending", "http404":
				require.Empty(t, repo.commands)
				require.Equal(t, "pending", repo.job.State)
			case "failed":
				require.Empty(t, repo.commands)
				require.Equal(t, "failed", repo.job.State)
			case "expired":
				require.Empty(t, repo.commands)
				require.Equal(t, "review", repo.job.State)
			default:
				require.Len(t, repo.commands, 1)
				require.Equal(t, "settled", repo.job.State)
				require.Equal(t, "grok-video:task", repo.commands[0].RequestID)
				require.Positive(t, repo.commands[0].BalanceCost)
				require.True(t, repo.commands[0].HistoricalVideo)
				require.Equal(t, repo.job.Snapshot.BillingModel, repo.commands[0].Model)
				expected := w.gateway.billingService.CalculateVideoCost(repo.job.Snapshot.BillingModel, "720p", 1, 12, nil, 1)
				require.InDelta(t, expected.ActualCost, repo.commands[0].BalanceCost, 1e-8)
				w.process(context.Background(), repo.job)
				require.Len(t, repo.commands, 1, "repeat completion must use durable dedup")
			}
		})
	}
}

func TestDurableVideoSnapshotSurvivesCacheLossAndOwnerIsolation(t *testing.T) {
	w, repo := newVideoBillingWorkerFixture(t, true)
	ctx := context.Background()
	pending, err := w.gateway.LoadGrokVideoPendingBilling(ctx, "task", 1, 10)
	require.NoError(t, err)
	require.NotNil(t, pending)
	require.Equal(t, "720p", pending.VideoResolution)
	account, err := w.gateway.ResolveGrokMediaVideoRequestAccount(ctx, repo.job.Snapshot.Owner.GroupID, "task", 1, 10)
	require.NoError(t, err)
	require.Equal(t, int64(20), account)
	wrongGroup := int64(99)
	account, _ = w.gateway.ResolveGrokMediaVideoRequestAccount(ctx, &wrongGroup, "task", 1, 10)
	require.Zero(t, account)
	account, _ = w.gateway.ResolveGrokMediaVideoRequestAccount(ctx, repo.job.Snapshot.Owner.GroupID, "task", 2, 10)
	require.Zero(t, account)
}

func TestVideoReceiptPersistsBeforeHandleDelivery(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmtBool(fail), func(t *testing.T) {
			c, rec := grokMediaContentTestContext(http.MethodPost, "/v1/videos/generations", nil)
			svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: completionFailureUpstream{do: func(*http.Request) (*http.Response, error) {
				return grokMediaContentStatusResponse(`{"request_id":"accepted"}`), nil
			}}}
			called := false
			ctx := WithVideoCreationReceipt(context.Background(), func(result *OpenAIForwardResult) error {
				called = true
				require.Equal(t, "accepted", result.ResponseID)
				require.Empty(t, rec.Body.String())
				if fail {
					return errors.New("fixture persistence unavailable")
				}
				return nil
			})
			_, err := svc.ForwardGrokMedia(ctx, c, grokMediaContentTestAccount(), GrokMediaEndpointVideosGenerations, "", []byte(`{"model":"grok-imagine-video","prompt":"fixture"}`), "application/json")
			require.True(t, called)
			if fail {
				require.Error(t, err)
				require.NotContains(t, rec.Body.String(), "accepted")
			} else {
				require.NoError(t, err)
				require.Contains(t, rec.Body.String(), "accepted")
			}
		})
	}
}

func fmtBool(b bool) string {
	if b {
		return "store failure"
	}
	return "stored"
}

func TestVideoSettlementReplayDoesNotOverwriteUsageLog(t *testing.T) {
	for _, applied := range []bool{true, false} {
		t.Run(fmtBool(applied), func(t *testing.T) {
			usage := &openAIRecordUsageLogRepoStub{}
			billing := &openAIRecordUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: applied}}
			svc := newOpenAIRecordUsageServiceWithBillingRepoForTest(usage, billing, &openAIRecordUsageUserRepoStub{}, &openAIRecordUsageSubRepoStub{}, nil)
			ctx := context.WithValue(context.Background(), ctxkey.ClientRequestID, "per-poll-client-id")
			err := svc.RecordUsage(ctx, &OpenAIRecordUsageInput{HistoricalVideo: true, Result: &OpenAIForwardResult{ResponseID: "seedance:task", RequestID: "grok-video:seedance:task", Model: "gpt-5.1", Usage: OpenAIUsage{OutputTokens: 100}}, APIKey: &APIKey{ID: 10, UserID: 1}, User: &User{ID: 1}, Account: &Account{ID: 20}})
			require.NoError(t, err)
			require.Equal(t, "grok-video:seedance:task", billing.lastCmd.RequestID)
			if applied {
				require.NotNil(t, usage.lastLog)
			} else {
				require.Nil(t, usage.lastLog, "already settled replay must not race to publish a repriced log")
			}
		})
	}
}
