//go:build unit

package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestGrokSearchMeterSurvivesConversion(t *testing.T) {
	final := `{"id":"resp_meter","object":"response","model":"grok-4.7","status":"completed","output":[{"type":"web_search_call","id":"ws","status":"completed"},{"type":"x_search_call","id":"xs","status":"completed"},{"type":"message","id":"msg","role":"assistant","status":"completed","content":[{"type":"output_text","text":"synthetic answer"}]}],"usage":{"input_tokens":100,"output_tokens":20,"total_tokens":120}}`
	sse := "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"web_search_call\",\"id\":\"ws\",\"status\":\"completed\"}}\n\n" + "data: {\"type\":\"response.completed\",\"response\":" + final + "}\n\n"
	for _, chat := range []bool{true, false} {
		for _, stream := range []bool{true, false} {
			t.Run(map[bool]string{true: "chat", false: "messages"}[chat]+map[bool]string{true: "-sse", false: "-json"}[stream], func(t *testing.T) {
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				capture := &chatReceiptCapture{claim: &ChatUsageReceiptClaim{RequestID: "2c4e888b-836b-4992-9ff1-dc1699488902"}}
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil).WithContext(context.WithValue(context.Background(), chatReceiptContextKey{}, capture))
				resp := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(sse))}
				a := healthyGrokOAuthGatewayTestAccount(9991, "synthetic-token")
				svc := &OpenAIGatewayService{}
				var result *OpenAIForwardResult
				var err error
				if chat {
					if stream {
						result, err = svc.handleChatStreamingResponse(resp, c, a, "grok-4.7", "grok-4.7", "grok-4.7", time.Now(), 0)
					} else {
						result, err = svc.handleChatBufferedStreamingResponse(resp, c, a, "grok-4.7", "grok-4.7", "grok-4.7", time.Now())
					}
				} else {
					if stream {
						result, err = svc.handleAnthropicStreamingResponse(resp, c, a, "grok-4.7", "grok-4.7", "grok-4.7", time.Now())
					} else {
						result, err = svc.handleAnthropicBufferedStreamingResponse(resp, c, a, "grok-4.7", "grok-4.7", "grok-4.7", time.Now())
					}
				}
				require.NoError(t, err)
				require.Equal(t, 2, result.SearchCount)
				got := int64(0)
				if stream {
					forEachOpenAISSEDataPayload(rec.Body.String(), func(data []byte) {
						if n := gjson.GetBytes(data, "usage.server_tool_use.web_search_requests").Int(); n > got {
							got = n
						}
					})
				} else {
					got = gjson.Get(rec.Body.String(), "usage.server_tool_use.web_search_requests").Int()
				}
				require.Equal(t, int64(2), got, "converted wire must preserve billed search count")
				if chat {
					require.NotNil(t, capture.meter)
					require.Equal(t, 2, capture.meter.WebSearchCalls)
				}
			})
		}
	}
}
func TestGrokSearchRejectsFailedAndClientFunctions(t *testing.T) {
	for _, item := range []string{`{"type":"web_search_call","status":"failed"}`, `{"type":"x_search_call","status":"in_progress"}`, `{"type":"function_call","name":"web_search","status":"completed"}`, `{"type":"custom_tool_call","name":"x_search","status":"completed"}`} {
		require.Zero(t, countGrokNativeSearchCallsFromJSONBytes([]byte(`{"output":[`+item+`]}`)), item)
	}
}
func TestChatReceiptSearchCounterStrict(t *testing.T) {
	for _, counter := range []string{`-1`, `1.5`, `null`, `"2"`, `9223372036854775808`} {
		require.Nil(t, parseChatReceiptMeter([]byte(`{"prompt_tokens":100,"completion_tokens":20,"server_tool_use":{"web_search_requests":`+counter+`}}`)))
	}
	require.Nil(t, parseChatReceiptMeter([]byte(`{"prompt_tokens":100,"completion_tokens":20,"server_tool_use":{"web_search_requests":1,"web_search_requests":0}}`)))
	r := parseChatReceiptMeter([]byte(`{"prompt_tokens":100,"completion_tokens":20,"server_tool_use":{"web_search_requests":2}}`))
	require.NotNil(t, r)
	require.Equal(t, 2, r.WebSearchCalls)
}

func TestChatReceiptCompletesSearchOnlyWithMatchingLedger(t *testing.T) {
	for _, tc := range []struct {
		name         string
		wire, ledger int
		failed, want bool
	}{
		{"completed", 2, 2, false, true}, {"failed_after_search", 2, 2, true, true}, {"lost_search_counter", 0, 2, false, false}, {"conflicting_counter", 2, 1, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &chatReceiptCaptureRepo{}
			svc := &OpenAIGatewayService{usageLogRepo: repo}
			claim := &ChatUsageReceiptClaim{APIKeyID: 99, RequestID: "2c4e888b-836b-4992-9ff1-dc1699488902", Model: "grok-4.7"}
			capture := &chatReceiptCapture{claim: claim, meter: &UsageReceipt{InputTokens: 100, OutputTokens: 20, WebSearchCalls: tc.wire}, terminal: "completed"}
			ctx := context.WithValue(context.Background(), chatReceiptContextKey{}, capture)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("POST", ChatReceiptEndpoint, nil).WithContext(ctx)
			var failure error
			if tc.failed {
				failure = errors.New("synthetic failure")
			}
			svc.completeChatUsageReceipt(ctx, c, healthyGrokOAuthGatewayTestAccount(9991, "synthetic"), &OpenAIForwardResult{SearchCount: tc.ledger}, failure)
			require.Equal(t, tc.want, repo.receipt != nil)
			if tc.want {
				require.Equal(t, 2, repo.receipt.WebSearchCalls)
				require.True(t, ValidateChatUsageReceipt(repo.receipt))
			}
		})
	}
}

func TestGrokFailedAfterSearchRetainsMeter(t *testing.T) {
	for _, chat := range []bool{true, false} {
		for _, stream := range []bool{true, false} {
			t.Run(fmt.Sprintf("chat_%t_stream_%t", chat, stream), func(t *testing.T) {
				final := `{"id":"r","model":"grok-4.7","status":"failed","error":{"type":"server_error","message":"synthetic terminal failure"},"output":[{"type":"web_search_call","id":"w","status":"completed"}],"usage":{"input_tokens":100,"output_tokens":20}}`
				sse := "data: {\"type\":\"response.failed\",\"response\":" + final + "}\n\n"
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest("POST", "/v1/chat/completions", nil)
				resp := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(sse))}
				svc := &OpenAIGatewayService{}
				a := healthyGrokOAuthGatewayTestAccount(9991, "synthetic")
				var r *OpenAIForwardResult
				var err error
				if chat {
					if stream {
						r, err = svc.handleChatStreamingResponse(resp, c, a, "grok-4.7", "grok-4.7", "grok-4.7", time.Now(), 0)
					} else {
						r, err = svc.handleChatBufferedStreamingResponse(resp, c, a, "grok-4.7", "grok-4.7", "grok-4.7", time.Now())
					}
				} else {
					if stream {
						r, err = svc.handleAnthropicStreamingResponse(resp, c, a, "grok-4.7", "grok-4.7", "grok-4.7", time.Now())
					} else {
						r, err = svc.handleAnthropicBufferedStreamingResponse(resp, c, a, "grok-4.7", "grok-4.7", "grok-4.7", time.Now())
					}
				}
				require.Error(t, err)
				require.NotNil(t, r)
				require.Equal(t, 1, r.SearchCount)
				body := rec.Body.String()
				if stream {
					forEachOpenAISSEDataPayload(body, func(data []byte) {
						if gjson.GetBytes(data, "usage.server_tool_use.web_search_requests").Int() == 1 {
							body = string(data)
						}
					})
				}
				require.Equal(t, int64(1), gjson.Get(body, "usage.server_tool_use.web_search_requests").Int())
			})
		}
	}
}
