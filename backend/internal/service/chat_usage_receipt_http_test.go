//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type chatReceiptCaptureRepo struct {
	UsageLogRepository
	mu        sync.Mutex
	claims    map[string]bool
	receipt   *UsageReceipt
	billingID string
	saveError error
}

func (r *chatReceiptCaptureRepo) ClaimChatUsageReceipt(_ context.Context, c *ChatUsageReceiptClaim) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.claims == nil {
		r.claims = map[string]bool{}
	}
	if r.claims[c.RequestID] {
		return false, nil
	}
	r.claims[c.RequestID] = true
	return true, nil
}
func (r *chatReceiptCaptureRepo) CompleteChatUsageReceipt(ctx context.Context, c *ChatUsageReceiptClaim, account int64, billingID string, receipt *UsageReceipt) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	copy := *receipt
	r.receipt = &copy
	r.billingID = billingID
	return r.saveError
}

func TestChatReceiptRealHTTPLateMeter(t *testing.T) {
	for _, mode := range []string{"responses_stream", "responses_buffer", "cancel_before_headers", "raw_stream", "raw_buffer", "raw_http503", "responses_http503", "raw_http503_cancel", "terminal_zero", "missing_usage", "partial_usage", "duplicate_usage", "http400", "terminal_failed", "save_failure"} {
		t.Run(mode, func(t *testing.T) {
			var native string
			switch mode {
			case "terminal_zero":
				native = `"usage":{"input_tokens":0,"output_tokens":0}`
			case "missing_usage":
				native = `"synthetic":true`
			case "partial_usage":
				native = `"usage":{"input_tokens":1000}`
			case "duplicate_usage":
				native = `"usage":{"input_tokens":1000,"output_tokens":7},"usage":{"input_tokens":0,"output_tokens":0}`
			default:
				native = `"usage":{"input_tokens":1000,"output_tokens":7,"input_tokens_details":{"cached_tokens":900},"output_tokens_details":{"reasoning_tokens":3}}`
			}
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				if mode == "http400" {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(400)
					io.WriteString(w, `{"error":{"type":"invalid_request_error","message":"synthetic invalid"}}`)
					return
				}
				if mode == "cancel_before_headers" || mode == "raw_http503_cancel" {
					time.Sleep(150 * time.Millisecond)
				}
				if strings.Contains(mode, "http503") {
					usage := native
					if strings.HasPrefix(mode, "raw") {
						usage = `"usage":{"prompt_tokens":1000,"completion_tokens":7,"prompt_tokens_details":{"cached_tokens":900},"completion_tokens_details":{"reasoning_tokens":3}}`
					}
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(503)
					io.WriteString(w, `{"error":{"type":"server_error","message":"synthetic unavailable"},`+usage+`}`)
					return
				}
				if strings.HasPrefix(mode, "raw") {
					usage := `"usage":{"prompt_tokens":1000,"completion_tokens":7,"total_tokens":1007,"prompt_tokens_details":{"cached_tokens":900},"completion_tokens_details":{"reasoning_tokens":3}}`
					if mode == "raw_buffer" {
						w.Header().Set("Content-Type", "application/json")
						io.WriteString(w, `{"choices":[{"message":{"content":"synthetic"},"finish_reason":"stop"}],`+usage+`}`)
						return
					}
					w.Header().Set("Content-Type", "text/event-stream")
					io.WriteString(w, `data: {"choices":[{"delta":{"content":"synthetic"},"finish_reason":"stop"}]}`+"\n\ndata: {\"choices\":[],"+usage+"}\n\ndata: [DONE]\n\n")
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, independentHeaderCreated+independentHeaderDelta)
				status, event := "completed", "response.completed"
				if mode == "terminal_failed" {
					status, event = "failed", "response.failed"
				}
				terminal := `data: {"type":"` + event + `","response":{"id":"resp_synthetic","status":"` + status + `","model":"gemini-4-argon","service_tier":"priority","output":[{"type":"message","content":[{"type":"output_text","text":"synthetic"}]}],` + native
				if mode == "terminal_failed" {
					terminal += `,"error":{"type":"server_error","code":"synthetic_failure","message":"synthetic failure","status_code":503}`
				}
				io.WriteString(w, terminal+"}}\n\n")
			}))
			defer upstream.Close()
			repo := &chatReceiptCaptureRepo{}
			if mode == "save_failure" {
				repo.saveError = errors.New("synthetic persistence unavailable")
			}
			transport := &independentHeaderHTTP{client: upstream.Client()}
			svc := &OpenAIGatewayService{cfg: structuredInputRetryTestConfig(), httpUpstream: transport, usageLogRepo: repo}
			account := structuredInputRetryTestAccount()
			account.Credentials["base_url"] = upstream.URL
			if strings.HasPrefix(mode, "raw") {
				account.Extra["openai_responses_supported"] = false
			}
			done := make(chan error, 1)
			rid := uuid.NewString()
			billingID := uuid.NewString()
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				ctx := context.WithValue(r.Context(), ctxkey.ClientRequestID, billingID)
				ctx, err := svc.BeginChatUsageReceipt(ctx, 11, rid, "gemini-4-argon", body)
				if err != nil {
					http.Error(w, "synthetic claim rejected", 409)
					done <- err
					return
				}
				c, _ := gin.CreateTestContext(w)
				c.Request = r.WithContext(ctx)
				c.Header(ChatReceiptVersionHeader, "v2")
				c.Header(ChatReceiptIDHeader, rid)
				_, err = svc.ForwardAsChatCompletions(ctx, c, account, body, "", "")
				done <- err
			}))
			defer gateway.Close()
			stream := mode != "responses_buffer" && mode != "raw_buffer"
			body, _ := json.Marshal(map[string]any{"model": "gemini-4-argon", "messages": []map[string]any{{"role": "user", "content": "synthetic"}}, "stream": stream})
			client := gateway.Client()
			if mode == "cancel_before_headers" || mode == "raw_http503_cancel" {
				client = &http.Client{Timeout: 50 * time.Millisecond}
			}
			response, requestErr := client.Post(gateway.URL+ChatReceiptEndpoint, "application/json", strings.NewReader(string(body)))
			if response != nil {
				io.Copy(io.Discard, response.Body)
				response.Body.Close()
			}
			if mode == "cancel_before_headers" || mode == "raw_http503_cancel" {
				require.Error(t, requestErr)
			} else {
				require.NoError(t, requestErr)
			}
			select {
			case <-done:
			case <-time.After(4 * time.Second):
				t.Fatal("late metering did not finish")
			}
			require.Equal(t, int32(1), transport.calls.Load(), "receipt recovery never regenerates")
			repo.mu.Lock()
			got := repo.receipt
			gotBilling := repo.billingID
			repo.mu.Unlock()
			want := mode != "http400" && mode != "missing_usage" && mode != "partial_usage" && mode != "duplicate_usage"
			if !want {
				require.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			require.Equal(t, 2, got.Version)
			require.Equal(t, rid, got.RequestID)
			require.Equal(t, "client:"+billingID, gotBilling)
			if mode == "terminal_zero" {
				require.Zero(t, got.InputTokens)
				require.Zero(t, got.OutputTokens)
			} else {
				require.Equal(t, 100, got.InputTokens)
				require.Equal(t, 900, got.CacheReadTokens)
				require.Equal(t, 7, got.OutputTokens)
				if strings.Contains(mode, "http503") {
					require.Zero(t, got.ReasoningTokens, "existing HTTP error wire carries output totals without reasoning detail")
				} else {
					require.Equal(t, 3, got.ReasoningTokens)
				}
			}
			if mode == "terminal_failed" || strings.Contains(mode, "http503") {
				require.Equal(t, "failed", got.TerminalState)
				require.GreaterOrEqual(t, got.HTTPStatus, 400)
			} else {
				require.Equal(t, "completed", got.TerminalState)
			}
			if mode == "cancel_before_headers" || mode == "raw_http503_cancel" {
				t.Log("before-header client cancellation retained exact late final meter without replay")
			}
		})
	}
}
