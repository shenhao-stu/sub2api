//go:build unit

package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestV0216CachePostCreateQuoteFailureBillsConfirmedInput(t *testing.T) {
	env := newCachedBillingEnv(t, 500, 900)
	env.upstream.createBody = cachedBillingCreateBody(time.Now().Add(time.Hour))
	env.upstream.beforeRequest = func(req *http.Request) {
		if req.Method == http.MethodPost {
			// The storage override disappears after upstream confirms real input;
			// the independent input price remains available for settlement.
			env.pricing.GetModelPricing("gemini-alias").CacheStorageCostPerTokenPerHour = 0
		}
	}
	rec := env.serve(http.MethodPost, "/v1beta/cachedContents",
		`{"model":"models/gemini-alias","ttl":"3600s","contents":[{"parts":[{"text":"doc"}]}]}`,
		nil, env.h.GeminiCachedContentsCreate)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	require.Equal(t, 1, env.upstream.methodCalls(http.MethodPost))
	require.Equal(t, 1, env.upstream.methodCalls(http.MethodDelete))
	require.Empty(t, env.repo.records)
	totals := env.waitForUsage(t, 1)
	require.Len(t, totals, 1)
	require.InDelta(t, cachedBillingTokens*cachedBillingInputPrice, totals[0], 1e-9)
}

type cachedLifecycleConcurrency struct {
	fakeConcurrencyCache
	mu              sync.Mutex
	users, accounts map[string]bool
}

func (c *cachedLifecycleConcurrency) AcquireUserSlot(_ context.Context, _ int64, limit int, requestID string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.users) >= limit {
		return false, nil
	}
	c.users[requestID] = true
	return true, nil
}

func (c *cachedLifecycleConcurrency) ReleaseUserSlot(_ context.Context, _ int64, requestID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.users, requestID)
	return nil
}

func (c *cachedLifecycleConcurrency) AcquireAccountSlot(_ context.Context, _ int64, limit int, requestID string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.accounts) >= limit {
		return false, nil
	}
	c.accounts[requestID] = true
	return true, nil
}

func (c *cachedLifecycleConcurrency) ReleaseAccountSlot(_ context.Context, _ int64, requestID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.accounts, requestID)
	return nil
}

func (c *cachedLifecycleConcurrency) counts() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.users), len(c.accounts)
}

func TestV0216CacheCancellationHoldsSlotsThroughUpstreamAndCompensation(t *testing.T) {
	cache := &cachedLifecycleConcurrency{users: map[string]bool{}, accounts: map[string]bool{}}
	env := newCachedBillingEnvWithConcurrency(t, 1, cache, 900)
	env.accounts[900].Concurrency = 1
	env.upstream.createBody = cachedBillingCreateBody(time.Now().Add(24 * time.Hour))
	postStarted, deleteStarted := make(chan struct{}), make(chan struct{})
	finishPost, finishDelete := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-finishPost:
		default:
			close(finishPost)
		}
		select {
		case <-finishDelete:
		default:
			close(finishDelete)
		}
	}()
	env.upstream.beforeRequest = func(req *http.Request) {
		switch req.Method {
		case http.MethodPost:
			close(postStarted)
			<-finishPost
		case http.MethodDelete:
			close(deleteStarted)
			<-finishDelete
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1beta/cachedContents", strings.NewReader(
		`{"model":"models/gemini-2.5-pro","ttl":"86400s","contents":[{"parts":[{"text":"doc"}]}]}`,
	)).WithContext(context.WithValue(ctx, ctxkey.Group, env.group))
	c.Set(string(middleware.ContextKeyAPIKey), env.apiKey)
	c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: env.apiKey.UserID, Concurrency: 1})
	done := make(chan struct{})
	go func() { defer close(done); env.h.GeminiCachedContentsCreate(c) }()
	select {
	case <-postStarted:
	case <-time.After(time.Second):
		t.Fatal("upstream create did not start")
	}
	cancel()
	assertHeld := func() {
		t.Helper()
		users, accounts := cache.counts()
		require.Equal(t, 1, users)
		require.Equal(t, 1, accounts)
		allowed, err := cache.AcquireUserSlot(context.Background(), env.apiKey.UserID, 1, "second-user")
		require.NoError(t, err)
		require.False(t, allowed)
		allowed, err = cache.AcquireAccountSlot(context.Background(), 900, 1, "second-account")
		require.NoError(t, err)
		require.False(t, allowed)
	}
	// Give cancellation callbacks a chance to run; the invariant must remain
	// true throughout the detached work, not merely before the callback fires.
	require.Never(t, func() bool { u, a := cache.counts(); return u != 1 || a != 1 }, 30*time.Millisecond, time.Millisecond)
	assertHeld()
	close(finishPost)
	select {
	case <-deleteStarted:
	case <-time.After(time.Second):
		t.Fatal("compensation did not start")
	}
	assertHeld()
	close(finishDelete)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler did not finish")
	}
	users, accounts := cache.counts()
	require.Zero(t, users)
	require.Zero(t, accounts)
	require.NotEqual(t, http.StatusOK, rec.Code)
	require.Empty(t, env.repo.records)
	require.Len(t, env.waitForUsage(t, 1), 1)
}

func TestV0216CacheCreateRejectsMissingInputPriceBeforeUpstream(t *testing.T) {
	env := newCachedBillingEnv(t, 500, 900)
	rec := env.serve(http.MethodPost, "/v1beta/cachedContents",
		`{"model":"models/gemini-unpriced-flash","ttl":"3600s","contents":[{"parts":[{"text":"doc"}]}]}`,
		nil, env.h.GeminiCachedContentsCreate)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	require.Empty(t, env.upstream.calls)
	require.Empty(t, env.repo.records)
	require.Empty(t, env.usage.totals())
}

func TestV0216CacheCreateSaveFailureBillsActualInputOnly(t *testing.T) {
	env := newCachedBillingEnv(t, 500, 900)
	env.repo.failCreate = true
	env.upstream.createBody = cachedBillingCreateBody(time.Now().Add(24 * time.Hour))
	rec := env.create()
	require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	require.Equal(t, 1, env.upstream.methodCalls(http.MethodDelete))
	require.Empty(t, env.repo.records)
	totals := env.waitForUsage(t, 1)
	require.Len(t, totals, 1)
	require.InDelta(t, cachedBillingTokens*cachedBillingInputPrice, totals[0], 1e-9)
}

func TestV0216CacheCreateCleanupFailureIsPrivateAndRecoverable(t *testing.T) {
	env := newCachedBillingEnv(t, 1, 900)
	env.upstream.createBody = cachedBillingCreateBody(time.Now().Add(24 * time.Hour))
	env.upstream.failures = map[string]cachedBillingUpstreamFailure{
		http.MethodDelete: {status: http.StatusServiceUnavailable, body: `{"error":{"message":"retry later"}}`},
	}
	rec := env.create()
	require.NotEqual(t, http.StatusOK, rec.Code)
	require.NotContains(t, rec.Body.String(), "gcache")
	require.Len(t, env.repo.records, 1)
	for _, record := range env.repo.records {
		require.True(t, record.CleanupPending)
		require.True(t, env.repo.pendingMutation(record.ID))
		_, err := env.h.geminiCachedContentService.Resolve(context.Background(), env.apiKey.ID, env.apiKey.GroupID, record.PublicName())
		require.ErrorIs(t, err, service.ErrGeminiCachedContentNotFound)
	}
	totals := env.waitForUsage(t, 1)
	require.InDelta(t, cachedBillingTokens*cachedBillingInputPrice, totals[0], 1e-9)
	delete(env.upstream.failures, http.MethodDelete)
	recovered, err := env.h.geminiCachedContentService.RecoverStaleMutations(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, recovered)
	require.Len(t, env.repo.deleted, 1)
	require.Equal(t, 2, env.upstream.methodCalls(http.MethodDelete))
	require.Len(t, env.usage.totals(), 1, "cleanup recovery never charges again")
}
