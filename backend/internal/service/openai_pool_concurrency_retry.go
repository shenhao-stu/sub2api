package service

import (
	"encoding/json"
	"net/http"
	"time"
)

// Pool admission can recover without changing accounts. Respect its explicit
// wait before retrying; a wait beyond our existing OpenAI retry-delay budget
// must return to the client, not immediately hit another shared pool entry.
func applyOpenAIPoolConcurrencyRetryAfter(account *Account, failure *UpstreamFailoverError, now time.Time) {
	if account == nil || account.Platform != PlatformOpenAI || account.Type != AccountTypeAPIKey || !account.IsPoolMode() || account.GetPoolModeRetryCount() <= 0 ||
		failure == nil || failure.StatusCode != http.StatusTooManyRequests || !failure.RetryableOnSameAccount ||
		failure.ExcludeFromRetry || !failure.ShouldRetryNextAccount() || len(failure.ResponseBody) > 64<<10 {
		return
	}
	// Exact, case-sensitive object keys keep quota/continuation guards and other
	// providers' free-form messages outside this admission-specific contract.
	var doc map[string]json.RawMessage
	var upstreamError map[string]json.RawMessage
	var errorType string
	if json.Unmarshal(failure.ResponseBody, &doc) != nil ||
		json.Unmarshal(doc["error"], &upstreamError) != nil ||
		json.Unmarshal(upstreamError["type"], &errorType) != nil || errorType != "concurrency" {
		return
	}
	delay, ok := parseRetryAfterDelay(now, failure.ResponseHeaders)
	if !ok || delay <= 0 {
		return
	}
	if delay > openAIOAuth429MaxRetryDelay {
		failure.RetryableOnSameAccount = false
		failure.NextAccountAction = NextAccountStop
		// Ending this request's wait budget is not an account-health failure.
		failure.Scope = GatewayFailureScopeRequest
		return
	}
	// Do not set a deadline: pool_mode_retry_count remains the retry budget.
	// A near-future HTTP date must not accelerate the existing retry cadence.
	if delay < openAIOAuth429RetryDelay {
		delay = openAIOAuth429RetryDelay
	}
	failure.SameAccountRetryDelay = delay
}
