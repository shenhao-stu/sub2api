package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// This synthetic regression documents the current C2A bridge's 429 contract.
// The provider already waited for its bounded admission queue before returning
// concurrency + Retry-After: 3. No model request or production account is used.
func TestC2APoolConcurrencyRetryHonorsProviderDelay(t *testing.T) {
	s := &OpenAIGatewayService{}
	account := &Account{ID: 465, Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
		Credentials: map[string]any{"pool_mode": true, "pool_mode_retry_count": 10}}
	body := []byte(`{"error":{"type":"concurrency","message":"synthetic capacity rejection"}}`)
	failure := s.newOpenAIAccountFailoverError(account, http.StatusTooManyRequests,
		http.Header{"Retry-After": {"3"}}, body, "synthetic capacity rejection", false, true)
	require.True(t, failure.RetryableOnSameAccount, "short capacity pressure can recover")
	require.Equal(t, 3*time.Second, failure.SameAccountRetryDelay,
		"pool retry must not fall back to 500ms before an explicit provider Retry-After")
	require.Equal(t, "3", failure.ResponseHeaders.Get("Retry-After"))
}

func TestC2APoolConcurrencyDoesNotTripHealthBreaker(t *testing.T) {
	encoded, err := json.Marshal(OpenAIAPIKeyHealthBreakerSettings{Enabled: true, WindowMinutes: 1, FailureThreshold: 3, CooldownMinutes: 5})
	require.NoError(t, err)
	settings := NewSettingService(&openAIAPIKeyHealthSettingRepo{value: string(encoded)}, &config.Config{})
	cache := &openAIAPIKeyHealthCacheStub{tripped: true}
	repo := &openAIAPIKeyHealthAccountRepo{}
	limiter := NewRateLimitService(repo, nil, &config.Config{}, nil, cache)
	limiter.SetSettingService(settings)
	limiter.SetOpenAIAPIKeyHealthCache(cache)
	for _, delay := range []string{"3", "30"} {
		a := openAIHealthPoolAccount()
		failure := (&OpenAIGatewayService{}).newOpenAIAccountFailoverError(a, 429, http.Header{"Retry-After": {delay}}, []byte(`{"error":{"type":"concurrency"}}`), "", false, true)
		require.False(t, limiter.ObserveOpenAIAPIKeyHealthFailure(context.Background(), a, failure), delay)
		require.Nil(t, a.TempUnschedulableUntil)
	}
	require.Zero(t, cache.recordCalls)
	require.Zero(t, cache.setCalls)
	require.Zero(t, repo.setCalls)
}

func TestC2APoolConcurrencyForwardRespectsRetryAfterAcrossProtocols(t *testing.T) {
	for _, path := range []string{"/v1/responses", "/v1/chat/completions", "/v1/messages"} {
		for _, delay := range []string{"3", "30"} {
			t.Run(fmt.Sprintf("%s/%s", path, delay), func(t *testing.T) {
				body := []byte(`{"model":"gpt-5.4","max_tokens":32,"input":"synthetic","messages":[{"role":"user","content":"synthetic"}],"stream":false}`)
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
				response := newOpenAIUpstreamErrorResponse(429, `{"error":{"type":"concurrency","message":"synthetic pool busy"}}`)
				response.Header.Set("Retry-After", delay)
				upstream := &httpUpstreamRecorder{resp: response}
				repo := &tempUnschedulableOpenAIAccountRepo{}
				s := &OpenAIGatewayService{cfg: &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{AllowInsecureHTTP: true}}}, httpUpstream: upstream, rateLimitService: NewRateLimitService(repo, nil, &config.Config{}, nil, nil)}
				a := &Account{ID: 465, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Concurrency: 1, Credentials: map[string]any{"api_key": "sk-synthetic", "base_url": "http://upstream.example", "pool_mode": true, "model_provider": "env-openai"}}
				var result *OpenAIForwardResult
				var err error
				switch path {
				case "/v1/responses":
					result, err = s.Forward(context.Background(), c, a, body)
				case "/v1/messages":
					result, err = s.ForwardAsAnthropic(context.Background(), c, a, body, "", "")
				default:
					result, err = s.ForwardAsChatCompletions(context.Background(), c, a, body, "", "")
				}
				var failure *UpstreamFailoverError
				require.ErrorAs(t, err, &failure)
				require.Nil(t, result)
				require.Len(t, upstream.requests, 1)
				require.Equal(t, 429, failure.StatusCode)
				require.Equal(t, delay, failure.ResponseHeaders.Get("Retry-After"))
				if delay == "3" {
					require.Equal(t, 3*time.Second, failure.SameAccountRetryDelay)
					require.True(t, failure.RetryableOnSameAccount)
					require.True(t, failure.ShouldRetryNextAccount())
				} else {
					require.False(t, failure.RetryableOnSameAccount)
					require.False(t, failure.ShouldRetryNextAccount())
				}
				require.Zero(t, repo.modelRateLimitAccountID)
				require.False(t, failure.ForceCacheBilling)
				require.False(t, IsResponseCommitted(c))
			})
		}
	}
}

func TestC2APoolConcurrencyRetryAfterBounds(t *testing.T) {
	now := time.Date(2026, 10, 24, 6, 0, 0, 0, time.UTC)
	t.Run("subsecond date does not accelerate retries", func(t *testing.T) {
		a := &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"pool_mode": true}}
		f := newOpenAIUpstreamFailoverError(429, http.Header{"Retry-After": {now.Add(time.Second).Format(http.TimeFormat)}}, []byte(`{"error":{"type":"concurrency"}}`), "", true)
		applyOpenAIPoolConcurrencyRetryAfter(a, f, now.Add(900*time.Millisecond))
		require.Equal(t, 500*time.Millisecond, f.SameAccountRetryDelay)
		require.True(t, f.RetryableOnSameAccount)
		require.True(t, f.ShouldRetryNextAccount())
	})
	for _, tc := range []struct {
		name, header string
		delay        time.Duration
		stop         bool
	}{
		{"seconds", "3", 3 * time.Second, false},
		{"http date", now.Add(5 * time.Second).Format(http.TimeFormat), 5 * time.Second, false},
		{"existing upper bound", "8", 8 * time.Second, false},
		{"beyond wait budget", "9", 0, true},
		{"bounded large wait", "604800", 0, true},
		{"future date beyond wait budget", now.Add(time.Minute).Format(http.TimeFormat), 0, true},
		{"missing", "", 0, false},
		{"zero", "0", 0, false},
		{"past date", now.Add(-time.Minute).Format(http.TimeFormat), 0, false},
		{"negative", "-3", 0, false},
		{"float", "3.5", 0, false},
		{"invalid", "not-a-delay", 0, false},
		{"untrusted large seconds", "604801", 0, false},
		{"overflow", "18446744073709551616", 0, false},
		{"untrusted date", now.Add(8 * 24 * time.Hour).Format(http.TimeFormat), 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"pool_mode": true}}
			body := []byte(`{"error":{"type":"concurrency","message":"synthetic capacity rejection"}}`)
			f := newOpenAIUpstreamFailoverError(429, http.Header{"Retry-After": {tc.header}}, body, "", true)
			applyOpenAIPoolConcurrencyRetryAfter(a, f, now)
			require.Equal(t, tc.delay, f.SameAccountRetryDelay)
			require.Equal(t, !tc.stop, f.RetryableOnSameAccount)
			require.Equal(t, !tc.stop, f.ShouldRetryNextAccount())
			if tc.stop {
				require.Equal(t, GatewayFailureScopeRequest, f.Scope)
			}
			require.Equal(t, 429, f.StatusCode)
			require.Equal(t, body, f.ResponseBody)
			require.Equal(t, tc.header, f.ResponseHeaders.Get("Retry-After"))
			require.True(t, f.SameAccountRetryDeadline.IsZero())
			require.Zero(t, f.SameAccountRetryMax)
			require.False(t, f.ForceCacheBilling)
		})
	}
}

func TestC2APoolConcurrencyRetryPreservesOtherPolicies(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(*Account, *UpstreamFailoverError)
	}{
		{"not pool", func(a *Account, _ *UpstreamFailoverError) { a.Credentials["pool_mode"] = false }},
		{"disabled retry budget", func(a *Account, _ *UpstreamFailoverError) { a.Credentials["pool_mode_retry_count"] = 0 }},
		{"different platform", func(a *Account, _ *UpstreamFailoverError) { a.Platform = PlatformAnthropic }},
		{"oauth", func(a *Account, _ *UpstreamFailoverError) { a.Type = AccountTypeOAuth }},
		{"wrong status", func(_ *Account, f *UpstreamFailoverError) { f.StatusCode = 503 }},
		{"not retryable", func(_ *Account, f *UpstreamFailoverError) { f.RetryableOnSameAccount = false }},
		{"excluded", func(_ *Account, f *UpstreamFailoverError) { f.ExcludeFromRetry = true }},
		{"terminal policy", func(_ *Account, f *UpstreamFailoverError) { f.NextAccountAction = NextAccountStop }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"pool_mode": true}}
			f := newOpenAIUpstreamFailoverError(429, http.Header{"Retry-After": {"30"}}, []byte(`{"error":{"type":"concurrency"}}`), "", true)
			tc.configure(a, f)
			before := *f
			applyOpenAIPoolConcurrencyRetryAfter(a, f, time.Now())
			require.Equal(t, before, *f)
		})
	}
	for _, body := range []string{
		`{"error":{"type":"account_quota_exhausted"}}`,
		`{"error":{"type":"insufficient_quota"}}`,
		`{"error":{"type":"rate_limit_error"}}`,
		`{"error":{"code":"conversation_quota_exhausted","type":"concurrency"}}`,
		`{"error":{"TYPE":"concurrency"}}`, `{"ERROR":{"type":"concurrency"}}`,
		`{"error":{"type":"Concurrency"}}`, `{"error":{"type":123}}`,
		`{"error":{"message":"concurrency"}}`, `{"error":"concurrency"}`,
		`{"error":[{"type":"concurrency"}]}`, `[{"error":{"type":"concurrency"}}]`,
		`null`, `{"error":{"type":"concurrency"}`, `{"error":{"type":"concurrency","message":"` + strings.Repeat("x", 64<<10) + `"}}`,
	} {
		a := &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"pool_mode": true}}
		f := newOpenAIUpstreamFailoverError(429, http.Header{"Retry-After": {"30"}}, []byte(body), "", true)
		before := *f
		applyOpenAIPoolConcurrencyRetryAfter(a, f, time.Now())
		require.Equal(t, before, *f)
	}
	// Existing explicit account policy still wins before the new wait policy.
	s := &OpenAIGatewayService{}
	a := &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"pool_mode": true}}
	f := s.newOpenAIAccountFailoverError(a, 429, http.Header{"Retry-After": {"30"}}, []byte(`{"error":{"type":"concurrency"}}`), "", true, true)
	require.Zero(t, f.SameAccountRetryDelay)
	require.True(t, f.ShouldRetryNextAccount())
}
