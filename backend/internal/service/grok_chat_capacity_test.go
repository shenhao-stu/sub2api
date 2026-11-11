//go:build unit

package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

const grokChatCapacityFrame = `data: {"error":{"code":"resource-exhausted","type":"server_error","message":"The model is currently at capacity due to high demand. Please try again in a few minutes, or use a higher service tier for priority processing: https://provider.invalid/developers/advanced-api-usage/priority-processing"}}` + "\n\n"

func runGrokChatCapacity(t *testing.T, prefix, suffix string, accountID int64) (*OpenAIForwardResult, error, *gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig()}
	account := &Account{ID: accountID, Platform: PlatformGrok, Type: AccountTypeOAuth}
	resp := &http.Response{StatusCode: 200, Header: http.Header{"X-Request-Id": {"capacity-synthetic"}}, Body: io.NopCloser(strings.NewReader(prefix + grokChatCapacityFrame + suffix))}
	result, err := svc.streamRawChatCompletions(c, resp, account, "grok-4.7", "grok-4.7", "grok-4.7", nil, nil, time.Now(), 100)
	return result, err, c, rec
}

func TestGrokChatCapacityBeforeOutputUsesExistingBoundedFailover(t *testing.T) {
	for i, prefix := range []string{"", "event: error\n", ": heartbeat\n\ndata: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n"} {
		_, err, c, rec := runGrokChatCapacity(t, prefix, "", 925001+int64(i))
		var failure *UpstreamFailoverError
		require.ErrorAs(t, err, &failure)
		require.Equal(t, http.StatusTooManyRequests, failure.StatusCode)
		require.True(t, failure.RetryableOnSameAccount)
		require.Equal(t, 1, failure.SameAccountRetryMax)
		require.Equal(t, 500*time.Millisecond, failure.SameAccountRetryDelay)
		require.False(t, c.Writer.Written())
		require.Empty(t, rec.Body.String())
		events, exists := c.Get(OpsUpstreamErrorsKey)
		require.True(t, exists)
		require.Len(t, events.([]*OpsUpstreamErrorEvent), 1)
		require.Equal(t, 429, events.([]*OpsUpstreamErrorEvent)[0].UpstreamStatusCode)
	}
}

func TestGrokChatCapacityAfterOutputOrUsageNeverReplays(t *testing.T) {
	for i, prefix := range []string{
		`data: {"choices":[{"delta":{"content":"partial"}}]}` + "\n\n",
		`data: {"choices":[{"delta":{"tool_calls":[{"id":"call_fixture","function":{"name":"lookup","arguments":"{}"}}]}}]}` + "\n\n",
		`data: {"choices":[{"delta":{"reasoning_content":"partial reasoning"}}]}` + "\n\n",
		`data: {"choices":[],"usage":{"prompt_tokens":42,"completion_tokens":0}}` + "\n\n",
	} {
		result, err, c, rec := runGrokChatCapacity(t, prefix, "", 925010+int64(i))
		require.Error(t, err)
		var failure *UpstreamFailoverError
		require.False(t, errors.As(err, &failure))
		require.NotNil(t, result)
		require.True(t, IsResponseCommitted(c))
		require.Equal(t, 1, strings.Count(rec.Body.String(), "resource-exhausted"))
		require.NotContains(t, rec.Body.String(), "upstream_stream_truncated")
		require.NotContains(t, rec.Body.String(), "provider.invalid")
		if i == 3 {
			require.Equal(t, 42, result.Usage.InputTokens)
		}
	}
}

func TestGrokChatCapacityLateUsagePreventsReplay(t *testing.T) {
	result, err, _, rec := runGrokChatCapacity(t, "", "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":23,\"completion_tokens\":4}}\n\ndata: [DONE]\n\n", 925020)
	require.Error(t, err)
	var failure *UpstreamFailoverError
	require.False(t, errors.As(err, &failure))
	require.NotNil(t, result)
	require.Equal(t, 23, result.Usage.InputTokens)
	require.Equal(t, 4, result.Usage.OutputTokens)
	require.Equal(t, 429, rec.Code)
	require.Equal(t, 1, strings.Count(rec.Body.String(), "resource-exhausted"))
}

func TestGrokChatCapacityDoesNotContaminateNextAttemptReceipt(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	capture := &chatReceiptCapture{grok: true, claim: &ChatUsageReceiptClaim{RequestID: "receipt-capacity-synthetic"}}
	c.Request = httptest.NewRequest(http.MethodPost, ChatReceiptEndpoint, nil).WithContext(context.WithValue(context.Background(), chatReceiptContextKey{}, capture))
	svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig()}
	account := &Account{ID: 925030, Platform: PlatformGrok, Type: AccountTypeOAuth}
	forward := func(body string) (*OpenAIForwardResult, error) {
		return svc.streamRawChatCompletions(c, &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, account, "grok-4.7", "grok-4.7", "grok-4.7", nil, nil, time.Now(), 100)
	}
	_, err := forward(grokChatCapacityFrame)
	var failure *UpstreamFailoverError
	require.ErrorAs(t, err, &failure)
	require.Empty(t, capture.rawTerminal)
	_, err = forward("data: {\"choices\":[{\"delta\":{\"content\":\"OK\"},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1,\"total_tokens\":4}}\n\ndata: [DONE]\n\n")
	require.NoError(t, err)
	require.False(t, capture.rawInvalidTail)
	require.Equal(t, "completed", capture.terminal)
	require.NotNil(t, capture.meter)
	require.EqualValues(t, 3, capture.meter.InputTokens)
}

func TestGrokChatCapacityUnknownOrZeroMeterNeverReplays(t *testing.T) {
	for i, meter := range []string{`{}`, `{"prompt_tokens":0,"completion_tokens":0}`, `{"prompt_tokens":"unknown"}`} {
		_, err, _, _ := runGrokChatCapacity(t, "", "data: {\"choices\":[],\"usage\":"+meter+"}\n\n", 925040+int64(i))
		require.Error(t, err)
		var failure *UpstreamFailoverError
		require.False(t, errors.As(err, &failure))
	}
}

func TestGrokChatCapacityUncertainOrTerminalPrefixNeverReplays(t *testing.T) {
	for i, prefix := range []string{"data: broken\n\n", "data: {}\n\n", "data: [DONE]\n\n", ": " + strings.Repeat("x", 65*1024) + "\n\n"} {
		_, err, _, _ := runGrokChatCapacity(t, prefix, "", 925050+int64(i))
		require.Error(t, err)
		var failure *UpstreamFailoverError
		require.False(t, errors.As(err, &failure))
	}
}

func TestGrokChatCapacityLateMeterReceiptRetainsSemanticStatus(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	capture := &chatReceiptCapture{grok: true, claim: &ChatUsageReceiptClaim{RequestID: "receipt-capacity-late"}}
	c.Request = httptest.NewRequest(http.MethodPost, ChatReceiptEndpoint, nil).WithContext(context.WithValue(context.Background(), chatReceiptContextKey{}, capture))
	body := grokChatCapacityFrame + grokChatCapacityFrame + "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":23,\"completion_tokens\":4,\"total_tokens\":27}}\n\ndata: [DONE]\n\n"
	svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig()}
	_, err := svc.streamRawChatCompletions(c, &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, &Account{ID: 925060, Platform: PlatformGrok}, "grok-4.7", "grok-4.7", "grok-4.7", nil, nil, time.Now(), 100)
	require.Error(t, err)
	require.Equal(t, "failed", capture.terminal)
	require.Equal(t, 429, capture.status)
	require.NotNil(t, capture.meter)
	require.EqualValues(t, 23, capture.meter.InputTokens)
	require.Equal(t, 1, strings.Count(rec.Body.String(), "resource-exhausted"))
}

func TestGrokChatCapacityPreservesNormalStream(t *testing.T) {
	body := ": heartbeat\n\ndata: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"OK\"},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1,\"total_tokens\":4}}\n\ndata: [DONE]\n\n"
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, ChatReceiptEndpoint, nil)
	svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig()}
	result, err := svc.streamRawChatCompletions(c, &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, &Account{ID: 925070, Platform: PlatformGrok}, "grok-4.7", "grok-4.7", "grok-4.7", nil, nil, time.Now(), 100)
	require.NoError(t, err)
	require.Equal(t, body, rec.Body.String())
	require.Equal(t, 3, result.Usage.InputTokens)
	require.Equal(t, 1, result.Usage.OutputTokens)
}

func TestGrokChatCapacityMatcherDoesNotReclassifyUnrelatedFailures(t *testing.T) {
	account := &Account{Platform: PlatformGrok}
	for _, payload := range []string{`{"error":{"code":"resource-exhausted","message":"monthly quota exhausted"}}`, `{"error":{"code":"resource-exhausted","message":"The model is currently at capacity","status_code":403}}`, `{"choices":[{"delta":{"content":"The model is currently at capacity"}}]}`} {
		require.Nil(t, grokRawChatRejectionPayload(account, []byte(payload)))
	}
	require.Nil(t, grokRawChatRejectionPayload(&Account{Platform: PlatformOpenAI}, []byte(strings.TrimSpace(strings.TrimPrefix(grokChatCapacityFrame, "data: ")))))
}

func TestGrokChatCapacityTypedConcurrencyRejectionUses429(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, ChatReceiptEndpoint, nil)
	svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig()}
	body := `data: {"error":{"type":"rate_limit_error","message":"Concurrency limit exceeded for account, please retry later"}}` + "\n\n"
	_, err := svc.streamRawChatCompletions(c, &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, &Account{ID: 925080, Platform: PlatformGrok, Type: AccountTypeAPIKey}, "grok-4.7", "grok-4.7", "grok-4.7", nil, nil, time.Now(), 100)
	var failure *UpstreamFailoverError
	require.ErrorAs(t, err, &failure)
	require.Equal(t, 429, failure.StatusCode)
	require.False(t, failure.RetryableOnSameAccount)
	require.Empty(t, rec.Body.String())
}

type grokChatInterruptedReader struct{}

func (grokChatInterruptedReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestGrokChatCapacityCanceledOrInterruptedNeverReplays(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var reader io.Reader = strings.NewReader(grokChatCapacityFrame)
		if canceled {
			cancel()
		} else {
			reader = io.MultiReader(reader, grokChatInterruptedReader{})
		}
		c.Request = httptest.NewRequest(http.MethodPost, ChatReceiptEndpoint, nil).WithContext(ctx)
		svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig()}
		_, err := svc.streamRawChatCompletions(c, &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(reader)}, &Account{ID: 925090, Platform: PlatformGrok}, "grok-4.7", "grok-4.7", "grok-4.7", nil, nil, time.Now(), 100)
		require.Error(t, err)
		var failure *UpstreamFailoverError
		require.False(t, errors.As(err, &failure))
	}
}
