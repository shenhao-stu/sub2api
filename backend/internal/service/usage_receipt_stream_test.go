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

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/gin-gonic/gin"
)

func TestReceiptActualStreamingPathsKeepFinalWireMeterAfterDisconnect(t *testing.T) {
	const body = "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":1000,\"output_tokens\":2,\"cache_creation_input_tokens\":100,\"cache_creation\":{\"ephemeral_1h_input_tokens\":100}}}}\n\n" +
		"data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":64000,\"server_tool_use\":{\"web_search_requests\":2}}}\n\n" +
		"data: {\"type\":\"message_stop\"}\n\n"
	for _, passthrough := range []bool{false, true} {
		for _, override := range []bool{false, true} {
			svc := newMinimalGatewayService()
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
			c.Writer = &failWriteResponseWriter{ResponseWriter: c.Writer}
			account := &Account{ID: 1, Platform: PlatformAnthropic, Type: AccountTypeOAuth, Extra: map[string]any{"cache_ttl_override_enabled": override, "cache_ttl_override_target": "5m"}}
			resp := &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}
			var result *streamingResult
			var err error
			if passthrough {
				result, err = svc.handleStreamingResponseAnthropicAPIKeyPassthrough(context.Background(), resp, c, account, time.Now(), "model")
			} else {
				result, err = svc.handleStreamingResponse(context.Background(), resp, c, account, time.Now(), "model", "model", false)
			}
			if err != nil || result == nil || !result.clientDisconnect {
				t.Fatalf("path=%v override=%v disconnect/result: %v", passthrough, override, err)
			}
			receipt := result.usage.receiptMeasured
			if receipt == nil || receipt.OutputTokens != 64000 || receipt.InputTokens != 1000 || receipt.WebSearchCalls != 2 {
				t.Fatalf("actual streaming path lost final meter: %+v", receipt)
			}
			want1h := 100
			if override && !passthrough {
				want1h = 0
			}
			if receipt.CacheCreation1hTokens != want1h {
				t.Fatalf("receipt differs from wire cache policy: %d want %d", receipt.CacheCreation1hTokens, want1h)
			}
			applyCacheTTLOverride(result.usage, "5m")
			if receipt.CacheCreation1hTokens != want1h {
				t.Fatal("later billing policy changed frozen evidence")
			}
		}
	}
}

type receiptFailRepo struct {
	openAIRecordUsageLogRepoStub
	calls int
}

func (r *receiptFailRepo) SaveUsageReceipt(context.Context, int64, *UsageReceipt) error {
	r.calls++
	return errors.New("synthetic persistence failure")
}
func TestReceiptWriteFailureCannotSkipSubBilling(t *testing.T) {
	repo := &receiptFailRepo{}
	bill := &openAIRecordUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: true}}
	svc := newGatewayRecordUsageServiceWithBillingRepoForTest(repo, bill, &openAIRecordUsageUserRepoStub{}, &openAIRecordUsageSubRepoStub{})
	var usage ClaudeUsage
	svc.parseSSEUsage(`{"type":"message_start","message":{"usage":{"input_tokens":1000}}}`, &usage)
	svc.parseSSEUsage(`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":64000}}`, &usage)
	ctx := context.WithValue(context.Background(), ctxkey.ClientRequestID, "2c4e888b-836b-4992-9ff1-dc1699488902")
	err := svc.RecordUsage(ctx, &RecordUsageInput{Result: &ForwardResult{Usage: usage, Stream: true, Model: "claude-sonnet-4", Duration: time.Second}, APIKey: &APIKey{ID: 501, Quota: 100}, User: &User{ID: 601}, Account: &Account{ID: 701}, ForceCacheBilling: true})
	if err != nil || repo.calls != 1 || bill.lastCmd == nil {
		t.Fatalf("receipt persistence failure skipped billing: %v calls=%d", err, repo.calls)
	}
}
