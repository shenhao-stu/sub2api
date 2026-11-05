//go:build unit

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestCacheTTLWireAndUsage(t *testing.T) {
	const sse = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"synthetic\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4\",\"content\":[],\"usage\":{\"input_tokens\":3,\"cache_creation_input_tokens\":20,\"cache_creation\":{\"ephemeral_5m_input_tokens\":7,\"ephemeral_1h_input_tokens\":13}}}}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"OK\"}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	for _, protocol := range []string{"messages", "chat", "responses"} {
		for _, stream := range []bool{false, true} {
			for _, ttl := range []string{"", "5m", "1h"} {
				t.Run(fmt.Sprintf("%s/%v/%s", protocol, stream, ttl), func(t *testing.T) {
					cc := `{"type":"ephemeral"}`
					wantTTL := "1h"
					if ttl != "" {
						cc = `{"type":"ephemeral","ttl":"` + ttl + `"}`
						wantTTL = ttl
					}
					body := fmt.Sprintf(`{"model":"claude-sonnet-4","max_tokens":16,"stream":%v,"messages":[{"role":"user","content":[{"type":"text","text":"synthetic","cache_control":%s}]}]}`, stream, cc)
					if protocol == "responses" {
						body = fmt.Sprintf(`{"model":"claude-sonnet-4","max_output_tokens":16,"stream":%v,"input":[{"role":"user","content":[{"type":"input_text","text":"synthetic","cache_control":%s}]}]}`, stream, cc)
					}
					response := sse
					ct := "text/event-stream"
					if protocol == "messages" && !stream {
						response = `{"id":"synthetic","type":"message","content":[{"type":"text","text":"OK"}],"stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":2,"cache_creation_input_tokens":20,"cache_creation":{"ephemeral_5m_input_tokens":7,"ephemeral_1h_input_tokens":13}}}`
						ct = "application/json"
					}
					up := &anthropicHTTPUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {ct}}, Body: io.NopCloser(strings.NewReader(response))}}
					svc := newForwardPartialUsageServiceForTest(up)
					rec := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(rec)
					c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
					account := newAnthropicAPIKeyAccountForTest()
					account.Extra["cache_ttl_override_enabled"] = true
					account.Extra["cache_ttl_override_target"] = "5m"
					var result *ForwardResult
					var err error
					switch protocol {
					case "messages":
						var parsed *ParsedRequest
						parsed, err = ParseGatewayRequest(NewRequestBodyRef([]byte(body)), PlatformAnthropic)
						require.NoError(t, err)
						result, err = svc.Forward(context.Background(), c, account, parsed)
					case "chat":
						result, err = svc.ForwardAsChatCompletions(context.Background(), c, account, []byte(body), nil)
					case "responses":
						result, err = svc.ForwardAsResponses(context.Background(), c, account, []byte(body), nil)
					}
					require.NoError(t, err)
					require.NotNil(t, result)
					require.Equal(t, wantTTL, gjson.GetBytes(up.lastBody, "messages.0.content.0.cache_control.ttl").String())
					require.Equal(t, "synthetic", gjson.GetBytes(up.lastBody, "messages.0.content.0.text").String())
					require.Equal(t, "user", gjson.GetBytes(up.lastBody, "messages.0.role").String())
					require.Equal(t, 7, result.Usage.CacheCreation5mTokens)
					require.Equal(t, 13, result.Usage.CacheCreation1hTokens)
					require.Contains(t, rec.Body.String(), `"ephemeral_5m_input_tokens":7`)
					require.Contains(t, rec.Body.String(), `"ephemeral_1h_input_tokens":13`)
				})
			}
		}
	}
}

func TestCacheTTLConflictNeverDispatches(t *testing.T) {
	body := []byte(`{"model":"claude-sonnet-4","messages":[{"role":"user","content":[{"type":"text","text":"a","cache_control":{"type":"ephemeral","ttl":"5m"}},{"type":"text","text":"b","cache_control":{"type":"ephemeral"}}]}]}`)
	_, err := ParseGatewayRequest(NewRequestBodyRef(body), PlatformAnthropic)
	require.ErrorIs(t, err, ErrCacheTTLOrderConflict)
	up := &anthropicHTTPUpstreamRecorder{}
	svc := newForwardPartialUsageServiceForTest(up)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
	_, err = svc.Forward(context.Background(), c, newAnthropicAPIKeyAccountForTest(), &ParsedRequest{Body: NewRequestBodyRef(body), Model: "claude-sonnet-4"})
	require.ErrorIs(t, err, ErrCacheTTLOrderConflict)
	require.Nil(t, up.lastReq)
	require.Equal(t, 400, rec.Code)
	require.Equal(t, "cache_ttl_order_conflict", gjson.Get(rec.Body.String(), "error.code").String())
}

func TestCacheTTLRetiredAdminWriteRejectedBeforePersistence(t *testing.T) {
	svc := &adminServiceImpl{}
	extra := map[string]any{"cache_ttl_override_enabled": true}
	_, err := svc.CreateAccount(context.Background(), &CreateAccountInput{Extra: extra})
	require.ErrorContains(t, err, "retired")
	_, err = svc.UpdateAccount(context.Background(), 1, &UpdateAccountInput{Extra: extra})
	require.ErrorContains(t, err, "retired")
	err = svc.UpdateAccountExtra(context.Background(), 1, extra)
	require.ErrorContains(t, err, "retired")
	_, err = svc.BulkUpdateAccounts(context.Background(), &BulkUpdateAccountsInput{Extra: extra})
	require.ErrorContains(t, err, "retired")
	for _, value := range []map[string]any{nil, {}, {"cache_ttl_override_enabled": false}, {"cache_ttl_override_enabled": nil}} {
		require.NoError(t, rejectDeprecatedCacheTTLOverride(value))
	}
}

func TestCacheTTLRecordUsagePreservesActualBuckets(t *testing.T) {
	logs := &openAIRecordUsageLogRepoStub{inserted: true}
	bill := &openAIRecordUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: true}}
	svc := newGatewayRecordUsageServiceWithBillingRepoForTest(logs, bill, &openAIRecordUsageUserRepoStub{}, &openAIRecordUsageSubRepoStub{})
	result := &ForwardResult{Model: "claude-sonnet-4", Usage: ClaudeUsage{InputTokens: 3, OutputTokens: 2, CacheCreationInputTokens: 20, CacheCreation5mTokens: 7, CacheCreation1hTokens: 13}}
	err := svc.RecordUsage(context.Background(), &RecordUsageInput{Result: result, APIKey: &APIKey{ID: 501, Quota: 100}, User: &User{ID: 601}, Account: &Account{ID: 701, Platform: PlatformAnthropic, Type: AccountTypeOAuth, Extra: map[string]any{"cache_ttl_override_enabled": true, "cache_ttl_override_target": "5m"}}})
	require.NoError(t, err)
	require.NotNil(t, logs.lastLog)
	require.Equal(t, 7, logs.lastLog.CacheCreation5mTokens)
	require.Equal(t, 13, logs.lastLog.CacheCreation1hTokens)
	require.False(t, logs.lastLog.CacheTTLOverridden)
	require.Equal(t, 1, bill.calls)
}

func TestCacheTTLConvertedCumulativeUpdates(t *testing.T) {
	for _, tc := range []struct {
		name, delta         string
		want5, want1, total int
	}{
		{"partial_output", `{"output_tokens":8}`, 7, 13, 20},
		{"cumulative", `{"output_tokens":8,"cache_creation_input_tokens":22,"cache_creation":{"ephemeral_5m_input_tokens":5,"ephemeral_1h_input_tokens":17}}`, 5, 17, 22},
		{"explicit_zero", `{"output_tokens":8,"cache_creation_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0}}`, 0, 0, 0},
		{"unknown_new_total", `{"output_tokens":8,"cache_creation_input_tokens":22}`, 0, 0, 22},
		{"conflicting", `{"output_tokens":8,"cache_creation_input_tokens":22,"cache_creation":{"ephemeral_5m_input_tokens":5,"ephemeral_1h_input_tokens":18}}`, 0, 0, 22},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var usage ClaudeUsage
			frames := []string{`{"type":"message_start","message":{"usage":{"input_tokens":3,"cache_creation_input_tokens":20,"cache_creation":{"ephemeral_5m_input_tokens":7,"ephemeral_1h_input_tokens":13}}}}`, `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":` + tc.delta + `}`}
			for _, frame := range frames {
				observePolicyCacheEvent(&usage, []byte(frame))
				var event apicompat.AnthropicStreamEvent
				require.NoError(t, json.Unmarshal([]byte(frame), &event))
				if event.Message != nil {
					mergeAnthropicUsage(&usage, event.Message.Usage)
				}
				if event.Usage != nil {
					mergeAnthropicUsage(&usage, *event.Usage)
				}
			}
			require.Equal(t, tc.want5, usage.CacheCreation5mTokens)
			require.Equal(t, tc.want1, usage.CacheCreation1hTokens)
			require.Equal(t, tc.total, usage.CacheCreationInputTokens)
		})
	}
}

func TestCacheTTLOAuthMimicKeepsExplicitClientMarkers(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprint(explicit), func(t *testing.T) {
			cc := ""
			if explicit {
				cc = `,"cache_control":{"type":"ephemeral","ttl":"5m"}`
			}
			body := []byte(`{"model":"claude-sonnet-4","max_tokens":16,"system":"synthetic system","messages":[{"role":"user","content":[{"type":"text","text":"synthetic first"` + cc + `}]},{"role":"assistant","content":"OK"},{"role":"user","content":"synthetic last"}]}`)
			up := &anthropicHTTPUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"id":"synthetic","type":"message","content":[{"type":"text","text":"OK"}],"stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":2}}`))}}
			svc := newForwardPartialUsageServiceForTest(up)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
			parsed, err := ParseGatewayRequest(NewRequestBodyRef(body), PlatformAnthropic)
			require.NoError(t, err)
			_, err = svc.Forward(context.Background(), c, newAnthropicOAuthAccountForPartialUsageTest(), parsed)
			require.NoError(t, err)
			require.NotNil(t, up.lastReq)
			_, paths, _, _ := collectCacheControlPaths(up.lastBody)
			if explicit {
				found := false
				for _, p := range paths {
					if gjson.GetBytes(up.lastBody, p+".ttl").String() == "5m" {
						found = true
					}
				}
				require.True(t, found)
			}
			_, err = normalizeClaudeCacheTTL(up.lastBody, "claude-sonnet-4")
			require.NoError(t, err)
		})
	}
}

func TestCacheTTLMarkedEmptyTextIsNotStripped(t *testing.T) {
	for _, cc := range []string{`null`, `{"type":"ephemeral"}`, `{"type":"ephemeral","ttl":null}`} {
		body := []byte(`{"messages":[{"role":"user","content":[{"type":"text","text":"","cache_control":` + cc + `}]}]}`)
		require.JSONEq(t, string(body), string(StripEmptyTextBlocks(body)))
	}
}

func TestCacheTTLRetiredGlobalToggleRejectedBeforePersistence(t *testing.T) {
	_, err := (&SettingService{}).buildSystemSettingsUpdates(context.Background(), &SystemSettings{EnableAnthropicCacheTTL1hInjection: true})
	require.ErrorContains(t, err, "retired")
}
