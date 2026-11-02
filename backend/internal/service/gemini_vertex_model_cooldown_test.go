//go:build unit

package service

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

const vertexCapacityFixture = `{"error":{"code":429,"message":"Resource exhausted. Please try again later.","status":"RESOURCE_EXHAUSTED"}}`

type vertexCooldownRepo struct {
	mockAccountRepoForGemini
	mu                sync.Mutex
	writes            int
	accountWrites     int
	stateContextError error
	fail              error
}

func (r *vertexCooldownRepo) SetRateLimited(context.Context, int64, time.Time) error {
	r.accountWrites++
	return nil
}
func (r *vertexCooldownRepo) SetModelRateLimitIfLater(ctx context.Context, id int64, model string, until time.Time, reason ...string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writes++
	r.stateContextError = ctx.Err()
	if r.fail != nil {
		return r.fail
	}
	a := r.accountsByID[id]
	if old := a.modelRateLimitResetAt(model); old != nil && old.After(until) {
		return nil
	}
	setAccountModelRateLimitSnapshot(a, model, until, reason[0], time.Now())
	return nil
}
func vertexCooldownFixture() (*vertexCooldownRepo, *Account) {
	a := geminiVertexAPIKeyTestAccount("fixture-project")
	a.Status = StatusActive
	a.Schedulable = true
	a.Credentials["model_mapping"] = map[string]any{"alias": "gemini-4-argon", "gemini-4-argon": "must-not-map-twice", "gemini-3.8-flash": "gemini-3.8-flash"}
	a.Priority = 0
	other := Account{ID: 812, Platform: PlatformGemini, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Priority: 10, Credentials: map[string]any{"api_key": "fixture"}}
	r := &vertexCooldownRepo{mockAccountRepoForGemini: mockAccountRepoForGemini{accounts: []Account{*a, other}, accountsByID: map[int64]*Account{}}}
	r.accountsByID[a.ID] = &r.accounts[0]
	r.accountsByID[other.ID] = &r.accounts[1]
	return r, a
}
func TestVertexModelCooldownStopsImmediateRetryAndPreservesOtherModels(t *testing.T) {
	for _, endpoint := range []string{"chat", "responses", "messages", "native", "native-bound"} {
		t.Run(endpoint, func(t *testing.T) {
			r, a := vertexCooldownFixture()
			up := &geminiVertex429SequenceUpstream{responses: []*http.Response{
				{StatusCode: 429, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(vertexCapacityFixture))},
				{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(geminiVertexAPIKeyTestResponse))},
			}}
			svc := &GeminiMessagesCompatService{httpUpstream: up, accountRepo: r, cache: &mockGatewayCacheForGemini{}, cfg: &config.Config{Gateway: config.GatewayConfig{MaxSameAccountRetriesGemini: 1}}}
			c, _ := newGeminiNativeTestContext(t)
			var result *ForwardResult
			var err error
			switch endpoint {
			case "chat":
				result, err = svc.ForwardAsChatCompletions(context.Background(), c, a, []byte(`{"model":"alias","messages":[{"role":"user","content":"fixture"}]}`))
			case "responses":
				result, err = svc.ForwardAsResponses(context.Background(), c, a, []byte(`{"model":"alias","input":"fixture"}`))
			case "messages":
				result, err = svc.Forward(context.Background(), c, a, []byte(`{"model":"alias","max_tokens":16,"messages":[{"role":"user","content":"fixture"}]}`))
			default:
				ctx := context.Background()
				if endpoint == "native-bound" {
					ctx = WithGeminiCachedContentBound(ctx)
				}
				result, err = svc.ForwardNative(ctx, c, a, "alias", "generateContent", false, []byte(`{"contents":[{"role":"user","parts":[{"text":"fixture"}]}]}`))
			}
			require.Error(t, err)
			require.Nil(t, result)
			var failover *UpstreamFailoverError
			require.ErrorAs(t, err, &failover)
			require.Equal(t, 429, failover.StatusCode)
			require.False(t, failover.RetryableOnSameAccount)
			require.JSONEq(t, vertexCapacityFixture, string(failover.ResponseBody))
			rawEvents, ok := c.Get(OpsUpstreamErrorsKey)
			require.True(t, ok)
			events := rawEvents.([]*OpsUpstreamErrorEvent)
			require.Len(t, events, 1)
			require.Equal(t, "failover", events[0].Kind)
			require.Equal(t, 1, up.calls, "an explicit capacity rejection must not generate four immediate retries")
			require.Equal(t, 1, r.writes)
			require.Zero(t, r.accountWrites)
			stored := r.accountsByID[a.ID]
			require.NotNil(t, stored.modelRateLimitResetAt("gemini-4-argon"))
			require.Nil(t, stored.modelRateLimitResetAt("must-not-map-twice"))
			require.True(t, stored.IsSchedulableForModel("gemini-3.8-flash"))
			require.False(t, stored.IsSchedulableForModel("alias"))
			selected, err := svc.SelectAccountForModel(context.Background(), nil, "", "alias")
			require.NoError(t, err)
			require.Equal(t, int64(812), selected.ID)
			selected, err = svc.SelectAccountForModel(context.Background(), nil, "", "gemini-3.8-flash")
			require.NoError(t, err)
			require.Equal(t, a.ID, selected.ID)
			setAccountModelRateLimitSnapshot(stored, "gemini-4-argon", time.Now().Add(-time.Second), "expired", time.Now())
			selected, err = svc.SelectAccountForModel(context.Background(), nil, "", "alias")
			require.NoError(t, err)
			require.Equal(t, a.ID, selected.ID)
		})
	}
}

func TestVertexModelCooldownHintsAndScope(t *testing.T) {
	now := time.Date(2026, 11, 2, 5, 0, 0, 0, time.UTC)
	_, a := vertexCooldownFixture()
	retry := func(delay string) string {
		return strings.TrimSuffix(vertexCapacityFixture, "}}") + `,"details":[{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"` + delay + `"}]}}`
	}
	cases := []struct {
		name, body, header string
		delay              time.Duration
		reason             string
	}{
		{"capacity", vertexCapacityFixture, "", time.Minute, "vertex_shared_capacity_backoff"},
		{"wrapped", "[" + vertexCapacityFixture + "]", "", time.Minute, "vertex_shared_capacity_backoff"},
		{"hint", retry("39s"), "", 39 * time.Second, "vertex_model_retry_hint"},
		{"fractional", retry("1.001s"), "", 1001 * time.Millisecond, "vertex_model_retry_hint"},
		{"both_use_longer", retry("39s"), "120", 120 * time.Second, "vertex_model_retry_hint"},
		{"retry_after_seconds", vertexCapacityFixture, "93", 93 * time.Second, "vertex_model_retry_hint"},
		{"retry_after_date", vertexCapacityFixture, now.Add(137 * time.Second).Format(http.TimeFormat), 137 * time.Second, "vertex_model_retry_hint"},
		{"cap_hint", retry("999999s"), "", geminiRetryInfoMaxDelay, "vertex_model_retry_hint"},
		{"cap_header", vertexCapacityFixture, "999999", geminiRetryInfoMaxDelay, "vertex_model_retry_hint"},
		{"negative", retry("-39s"), "-2", time.Minute, "vertex_shared_capacity_backoff"},
		{"invalid_hint", retry("1h2m"), "", time.Minute, "vertex_shared_capacity_backoff"},
		{"invalid_date", vertexCapacityFixture, "bad", time.Minute, "vertex_shared_capacity_backoff"},
		{"past_date", vertexCapacityFixture, now.Add(-time.Second).Format(http.TimeFormat), time.Minute, "vertex_shared_capacity_backoff"},
		{"overflow", retry("9999999999999999999999999999s"), "99999999999999999999", time.Minute, "vertex_shared_capacity_backoff"},
		{"truncated", vertexCapacityFixture[:len(vertexCapacityFixture)-1], "120", 0, ""},
		{"extra_usage", strings.TrimSuffix(vertexCapacityFixture, "}") + `,"usageMetadata":{"promptTokenCount":1}}`, "", 0, ""},
		{"candidates", strings.TrimSuffix(vertexCapacityFixture, "}") + `,"candidates":[]}`, "", 0, ""},
		{"multi_array", "[" + vertexCapacityFixture + "," + vertexCapacityFixture + "]", "", 0, ""},
		{"quota", strings.Replace(vertexCapacityFixture, "Resource exhausted.", "Quota resource exhausted.", 1), "", 0, ""},
		{"quota_failure", strings.TrimSuffix(vertexCapacityFixture, "}}") + `,"details":[{"@type":"type.googleapis.com/google.rpc.QuotaFailure","violations":[]}]}}`, "", 0, ""},
		{"wrong_status", strings.Replace(vertexCapacityFixture, "RESOURCE_EXHAUSTED", "UNAVAILABLE", 1), "", 0, ""},
		{"wrong_code", strings.Replace(vertexCapacityFixture, ":429", ":500", 1), "", 0, ""},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, reason := vertexModelCooldown(a, 429, http.Header{"Retry-After": []string{tt.header}}, []byte(tt.body), "gemini-4-argon", now)
			require.Equal(t, tt.delay, got)
			require.Equal(t, tt.reason, reason)
		})
	}
	for _, tt := range []struct {
		name   string
		modify func(*Account)
	}{
		{"aistudio", func(a *Account) { a.Type = AccountTypeAPIKey }},
		{"non_gemini", func(a *Account) { a.Platform = PlatformAnthropic }},
		{"pool_policy", func(a *Account) { a.Type = AccountTypeAPIKey; a.Credentials["pool_mode"] = true }},
		{"custom_excludes_429", func(a *Account) {
			a.Type = AccountTypeAPIKey
			a.Credentials["custom_error_codes_enabled"] = true
			a.Credentials["custom_error_codes"] = []any{float64(500)}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, a := vertexCooldownFixture()
			tt.modify(a)
			d, _ := vertexModelCooldown(a, 429, nil, []byte(vertexCapacityFixture), "gemini-4-argon", now)
			require.Zero(t, d)
		})
	}
	d, _ := vertexModelCooldown(a, 200, nil, []byte(vertexCapacityFixture), "gemini-4-argon", now)
	require.Zero(t, d)
	d, _ = vertexModelCooldown(a, 429, nil, []byte(vertexCapacityFixture), "", now)
	require.Zero(t, d)
}

func TestVertexModelCooldownStateSurvivesCanceledCallerAndOnlyExtends(t *testing.T) {
	r, a := vertexCooldownFixture()
	svc := &GeminiMessagesCompatService{accountRepo: r}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	body := []byte(vertexCapacityFixture)
	require.True(t, svc.persistVertexModelCooldown(ctx, a, 429, http.Header{"Retry-After": []string{"140"}}, body, "gemini-4-argon"))
	require.NoError(t, r.stateContextError)
	first := *r.accountsByID[a.ID].modelRateLimitResetAt("gemini-4-argon")
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); svc.persistVertexModelCooldown(ctx, a, 429, nil, body, "gemini-4-argon") }()
	}
	wg.Wait()
	require.Equal(t, first, *r.accountsByID[a.ID].modelRateLimitResetAt("gemini-4-argon"))
	require.Nil(t, a.Extra, "do not race-mutate shared scheduler input")
	require.Zero(t, r.accountWrites)
}

func TestVertexModelCooldownPersistenceFailureDoesNotRetryGeneration(t *testing.T) {
	r, a := vertexCooldownFixture()
	r.fail = io.ErrClosedPipe
	up := &geminiVertex429SequenceUpstream{responses: []*http.Response{{StatusCode: 429, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(vertexCapacityFixture))}}}
	svc := &GeminiMessagesCompatService{httpUpstream: up, accountRepo: r, cfg: &config.Config{Gateway: config.GatewayConfig{MaxSameAccountRetriesGemini: 4}}}
	c, _ := newGeminiNativeTestContext(t)
	result, err := svc.ForwardAsChatCompletions(context.Background(), c, a, []byte(`{"model":"alias","messages":[{"role":"user","content":"fixture"}]}`))
	require.Error(t, err)
	require.Nil(t, result)
	require.Equal(t, 1, up.calls)
	require.Equal(t, 1, r.writes)
	require.Zero(t, r.accountWrites)
}

func TestVertexModelCooldownPreservesTokenCountingFallback(t *testing.T) {
	r, a := vertexCooldownFixture()
	up := &geminiVertex429SequenceUpstream{responses: []*http.Response{{StatusCode: 429, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(vertexCapacityFixture))}}}
	svc := &GeminiMessagesCompatService{httpUpstream: up, accountRepo: r, cfg: &config.Config{}}
	c, rec := newGeminiNativeTestContext(t)
	result, err := svc.ForwardNative(context.Background(), c, a, "alias", "countTokens", false, []byte(`{"contents":[{"role":"user","parts":[{"text":"fixture"}]}]}`))
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 200, rec.Code)
	require.Contains(t, rec.Body.String(), "totalTokens")
	require.Zero(t, result.Usage.InputTokens)
	require.Zero(t, result.Usage.OutputTokens)
	require.Equal(t, 1, up.calls)
	require.Zero(t, r.writes)
	require.Zero(t, r.accountWrites)
}
