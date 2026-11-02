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
	"github.com/stretchr/testify/require"
)

const geminiPaymentFixture = `{"error":{"code":402,"status":"RESOURCE_EXHAUSTED","message":"Your prepayment credits are depleted. Please go to AI Studio at https://provider.example/billing to manage your project and billing. Learn more at https://provider.example/help"}}`

type geminiPaymentRepo struct {
	mockAccountRepoForGemini
	mu                sync.Mutex
	writes            int
	stateContextError error
	fail              error
}

func (r *geminiPaymentRepo) SetRateLimitedIfLater(ctx context.Context, id int64, until time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writes++
	r.stateContextError = ctx.Err()
	if r.fail != nil {
		return r.fail
	}
	a := r.accountsByID[id]
	if a.RateLimitResetAt == nil || a.RateLimitResetAt.Before(until) {
		a.RateLimitResetAt = &until
	}
	return nil
}
func geminiPaymentAccounts() (*geminiPaymentRepo, *Account) {
	a := &Account{ID: 811, Platform: PlatformGemini, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 4, Credentials: map[string]any{"api_key": "fixture", "model_mapping": map[string]any{"alias": "gemini-4-argon", "gemini-3.8-flash": "gemini-3.8-flash"}}}
	b := geminiVertexAPIKeyTestAccount("fixture-project")
	b.ID = 812
	b.Status = StatusActive
	b.Schedulable = true
	b.Priority = 20
	r := &geminiPaymentRepo{mockAccountRepoForGemini: mockAccountRepoForGemini{accounts: []Account{*a, *b}, accountsByID: map[int64]*Account{}}}
	for i := range r.accounts {
		r.accountsByID[r.accounts[i].ID] = &r.accounts[i]
	}
	return r, r.accountsByID[811]
}

func TestGeminiPaymentFailoverUsesOneRejectionAndPreservesBoundCache(t *testing.T) {
	for _, endpoint := range []string{"chat", "responses", "messages", "native", "native-bound"} {
		t.Run(endpoint, func(t *testing.T) {
			repo, a := geminiPaymentAccounts()
			upstream := &geminiVertex429SequenceUpstream{responses: []*http.Response{{StatusCode: 402, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(geminiPaymentFixture)), ContentLength: int64(len(geminiPaymentFixture))}}}
			svc := &GeminiMessagesCompatService{accountRepo: repo, httpUpstream: upstream, cache: &mockGatewayCacheForGemini{}, cfg: &config.Config{Gateway: config.GatewayConfig{MaxSameAccountRetriesGemini: 4}}}
			c, _ := newGeminiNativeTestContext(t)
			ctx := context.Background()
			var result *ForwardResult
			var err error
			switch endpoint {
			case "chat":
				result, err = svc.ForwardAsChatCompletions(ctx, c, a, []byte(`{"model":"alias","messages":[{"role":"user","content":"fixture"}]}`))
			case "responses":
				result, err = svc.ForwardAsResponses(ctx, c, a, []byte(`{"model":"alias","input":"fixture"}`))
			case "messages":
				result, err = svc.Forward(ctx, c, a, []byte(`{"model":"alias","max_tokens":16,"messages":[{"role":"user","content":"fixture"}]}`))
			default:
				if endpoint == "native-bound" {
					ctx = WithGeminiCachedContentBound(ctx)
				}
				result, err = svc.ForwardNative(ctx, c, a, "alias", "generateContent", false, []byte(`{"contents":[{"role":"user","parts":[{"text":"fixture"}]}]}`))
			}
			require.Error(t, err)
			require.Nil(t, result)
			var failure *UpstreamFailoverError
			require.ErrorAs(t, err, &failure)
			require.Equal(t, GeminiPaymentRequiredReason, failure.Reason)
			require.Equal(t, 402, failure.StatusCode)
			require.False(t, failure.RetryableOnSameAccount)
			require.True(t, failure.ExcludeFromRetry)
			require.Equal(t, endpoint != "native-bound", failure.ShouldRetryNextAccount())
			require.False(t, c.Writer.Written())
			require.Equal(t, 1, upstream.calls)
			require.Equal(t, 1, repo.writes)
			require.Equal(t, StatusActive, a.Status)
			require.True(t, a.Schedulable)
			require.NotNil(t, a.RateLimitResetAt)
			require.WithinDuration(t, time.Now().Add(10*time.Minute), *a.RateLimitResetAt, time.Second)
			require.Nil(t, repo.accountsByID[812].RateLimitResetAt, "healthy Vertex Flash must remain eligible")
			for _, model := range []string{"alias", "gemini-3.8-flash"} {
				selected, err := svc.SelectAccountForModel(ctx, nil, "", model)
				require.NoError(t, err)
				require.Equal(t, int64(812), selected.ID)
			}
			past := time.Now().Add(-time.Second)
			a.RateLimitResetAt = &past
			selected, err := svc.SelectAccountForModel(context.Background(), nil, "", "alias")
			require.NoError(t, err)
			require.Equal(t, a.ID, selected.ID, "expired local backoff permits natural recovery")
		})
	}
}

func TestGeminiPaymentClassificationIsNarrow(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       bool
	}{
		{"reported depletion", geminiPaymentFixture, true},
		{"plain depletion", `{"error":{"code":402,"status":"RESOURCE_EXHAUSTED","message":"Your prepayment credits are depleted."}}`, true},
		{"request budget", `{"error":{"code":402,"status":"RESOURCE_EXHAUSTED","message":"Request estimated cost exceeds remaining credits"}}`, false},
		{"unknown payment", `{"error":{"code":402,"status":"RESOURCE_EXHAUSTED","message":"Insufficient credits"}}`, false},
		{"null details", strings.Replace(geminiPaymentFixture, `"code":402`, `"details":null,"code":402`, 1), true},
		{"empty details", strings.Replace(geminiPaymentFixture, `"code":402`, `"details":[],"code":402`, 1), true},
		{"unknown details", strings.Replace(geminiPaymentFixture, `"code":402`, `"details":[{"usageMetadata":{"totalTokenCount":10}}],"code":402`, 1), false},
		{"wrong details type", strings.Replace(geminiPaymentFixture, `"code":402`, `"details":{},"code":402`, 1), false},
		{"ordinary429", strings.Replace(geminiPaymentFixture, `"code":402`, `"code":429`, 1), false},
		{"usage also present", strings.TrimSuffix(geminiPaymentFixture, "}") + `,"usageMetadata":{"promptTokenCount":5}}`, false},
		{"candidate also present", strings.TrimSuffix(geminiPaymentFixture, "}") + `,"candidates":[]}`, false},
		{"error duplicates", strings.Replace(geminiPaymentFixture, `"code":402`, `"code":429,"code":402`, 1), false},
		{"envelope duplicates", strings.Replace(geminiPaymentFixture, `{"error":`, `{"error":{},"error":`, 1), false},
		{"error contains usage", strings.Replace(geminiPaymentFixture, `"code":402`, `"usageMetadata":{},"code":402`, 1), false},
		{"truncated", geminiPaymentFixture[:len(geminiPaymentFixture)-1], false},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, geminiPaymentDepleted([]byte(tc.body))) })
	}
}

type geminiPaymentBrokenReader struct{ raw []byte }

func (r *geminiPaymentBrokenReader) Read(p []byte) (int, error) {
	if len(r.raw) > 0 {
		n := copy(p, r.raw)
		r.raw = r.raw[n:]
		return n, nil
	}
	return 0, io.ErrUnexpectedEOF
}
func (*geminiPaymentBrokenReader) Close() error { return nil }

func TestGeminiPaymentRequiresCompleteTransport(t *testing.T) {
	for _, broken := range []string{"read-error", "content-length", "oversized"} {
		t.Run(broken, func(t *testing.T) {
			repo, a := geminiPaymentAccounts()
			svc := &GeminiMessagesCompatService{accountRepo: repo}
			c, _ := newGeminiNativeTestContext(t)
			resp := &http.Response{StatusCode: 402, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(geminiPaymentFixture)), ContentLength: int64(len(geminiPaymentFixture))}
			switch broken {
			case "read-error":
				resp.Body = &geminiPaymentBrokenReader{raw: []byte(geminiPaymentFixture)}
			case "content-length":
				resp.ContentLength++
			case "oversized":
				resp.ContentLength = -1
				resp.Body = io.NopCloser(strings.NewReader(geminiPaymentFixture + strings.Repeat(" ", 8192)))
			}
			require.Nil(t, svc.geminiPaymentFailover(context.Background(), c, a, resp, "gemini-4-argon"))
			require.Zero(t, repo.writes)
		})
	}
}

func TestGeminiPaymentCooldownPreservesLongerWindowAndSurvivesCancellation(t *testing.T) {
	repo, a := geminiPaymentAccounts()
	later := time.Now().Add(2 * time.Hour)
	a.RateLimitResetAt = &later
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	svc := &GeminiMessagesCompatService{accountRepo: repo}
	c, _ := newGeminiNativeTestContext(t)
	resp := &http.Response{StatusCode: 402, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(geminiPaymentFixture))}
	failure := svc.geminiPaymentFailover(ctx, c, a, resp, "gemini-4-argon")
	require.NotNil(t, failure)
	require.NoError(t, repo.stateContextError)
	require.Equal(t, later, *a.RateLimitResetAt)
	repo.fail = errors.New("isolated write failure")
	resp.Body = io.NopCloser(strings.NewReader(geminiPaymentFixture))
	failure = svc.geminiPaymentFailover(ctx, c, a, resp, "gemini-4-argon")
	require.NotNil(t, failure)
	require.False(t, failure.RetryableOnSameAccount)
	require.True(t, failure.ExcludeFromRetry)
	require.Equal(t, later, *a.RateLimitResetAt)
}

func TestGeminiPaymentHonorsPolicyAndProviderBoundaries(t *testing.T) {
	for _, mode := range []string{"custom-skip", "custom-match", "pool", "other-platform", "vertex"} {
		t.Run(mode, func(t *testing.T) {
			repo, a := geminiPaymentAccounts()
			svc := &GeminiMessagesCompatService{accountRepo: repo, rateLimitService: NewRateLimitService(repo, nil, &config.Config{}, nil, nil)}
			c, _ := newGeminiNativeTestContext(t)
			switch mode {
			case "custom-skip":
				a.Credentials["custom_error_codes_enabled"] = true
				a.Credentials["custom_error_codes"] = []any{float64(429)}
			case "custom-match":
				a.Credentials["custom_error_codes_enabled"] = true
				a.Credentials["custom_error_codes"] = []any{float64(402)}
			case "pool":
				a.Credentials["pool_mode"] = true
			case "other-platform":
				a.Platform = PlatformOpenAI
			case "vertex":
				a.Type = AccountTypeServiceAccount
			}
			resp := &http.Response{StatusCode: 402, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(geminiPaymentFixture))}
			fail := svc.geminiPaymentFailover(context.Background(), c, a, resp, "gemini-4-argon")
			if mode == "other-platform" {
				require.Nil(t, fail)
				require.Zero(t, repo.writes)
				return
			}
			require.NotNil(t, fail)
			require.False(t, fail.RetryableOnSameAccount)
			if mode == "custom-skip" || mode == "pool" {
				require.Zero(t, repo.writes)
			} else {
				require.Equal(t, 1, repo.writes)
			}
		})
	}
}

func TestGeminiPaymentCannotRescueExcludedStickyOrDisabledAccount(t *testing.T) {
	repo, a := geminiPaymentAccounts()
	cache := &mockGatewayCacheForGemini{sessionBindings: map[string]int64{"fixture-session": a.ID}}
	svc := &GeminiMessagesCompatService{accountRepo: repo, cache: cache}
	chosen, err := svc.SelectAccountForModelWithExclusions(context.Background(), nil, "fixture-session", "alias", map[int64]struct{}{a.ID: {}})
	require.NoError(t, err)
	require.Equal(t, int64(812), chosen.ID, "sticky cannot rescue an excluded wallet even if persistence failed")
	past := time.Now().Add(-time.Second)
	a.RateLimitResetAt = &past
	a.Schedulable = false
	chosen, err = svc.SelectAccountForModel(context.Background(), nil, "", "alias")
	require.NoError(t, err)
	require.Equal(t, int64(812), chosen.ID, "expiry never re-enables an operator-disabled account")
}
