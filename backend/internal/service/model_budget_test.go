package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func budgetTestBody(model string, metered bool) string {
	body := fmt.Sprintf(`{"type":"error","error":{"type":"budget_exceeded","message":"Insufficient API key budget: only 0 %s request(s) can run at once right now. This is not a platform outage or a concurrency setting. Fix: top up, or use a cheaper model.","help":"https://private.invalid/?key=secret"}`, model)
	if metered {
		body += `,"usage":{"input_tokens":12,"output_tokens":7,"cache_read_input_tokens":5,"cache_creation":{"ephemeral_5m_input_tokens":2,"ephemeral_1h_input_tokens":3}}`
	}
	return body + "}"
}

func TestModelBudgetForwardTerminal402(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		for _, metered := range []bool{false, true} {
			t.Run(fmt.Sprintf("passthrough=%t/metered=%t", passthrough, metered), func(t *testing.T) {
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
				up := &queuedHTTPUpstreamStub{responses: []*http.Response{{StatusCode: 402, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(budgetTestBody("claude-opus-5-5", metered)))}}}
				s := newForwardPartialUsageServiceForTest(nil)
				s.httpUpstream = up
				account := newAnthropicAPIKeyAccountForTest()
				account.Credentials["pool_mode"] = true
				account.Extra["anthropic_passthrough"] = passthrough
				parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(`{"model":"claude-opus-5-5","messages":[{"role":"user","content":"synthetic"}]}`)), PlatformAnthropic)
				require.NoError(t, err)
				result, err := s.Forward(context.Background(), c, account, parsed)
				require.Error(t, err)
				var failover *UpstreamFailoverError
				require.False(t, errors.As(err, &failover))
				require.Equal(t, 1, up.callCount)
				require.Equal(t, 402, c.Writer.Status())
				require.Contains(t, rec.Body.String(), `"code":"upstream_model_budget_exhausted"`)
				require.Contains(t, rec.Body.String(), `"status_code":402`)
				require.NotContains(t, rec.Body.String(), "private.invalid")
				require.NotContains(t, rec.Body.String(), "secret")
				require.True(t, account.Schedulable)
				require.Nil(t, account.TempUnschedulableUntil)
				if metered {
					require.NotNil(t, result)
					require.Equal(t, 12, result.Usage.InputTokens)
					require.Equal(t, 7, result.Usage.OutputTokens)
					require.Equal(t, 5, result.Usage.CacheReadInputTokens)
					require.Equal(t, 2, result.Usage.CacheCreation5mTokens)
					require.Equal(t, 3, result.Usage.CacheCreation1hTokens)
				} else {
					require.Nil(t, result)
				}
			})
		}
	}
}

func TestModelBudgetRequiresCompleteExact402Contract(t *testing.T) {
	const model = "claude-opus-5-5"
	body := budgetTestBody(model, false)
	require.True(t, isModelBudgetError(402, []byte(body), model))
	bilingual := strings.Replace(body, "Insufficient API key budget:", "API Key 剩余额度不足，当前只够同时进行 0 个 "+model+" 请求。这不是平台故障，也不是并发配置问题。解决：充值，或换用更便宜的模型。 | Insufficient API key budget:", 1)
	require.True(t, isModelBudgetError(402, []byte(bilingual), model))
	for _, status := range []int{200, 400, 401, 403, 429, 500, 503} {
		require.False(t, isModelBudgetError(status, []byte(body), model))
	}
	for _, requested := range []string{"", "claude-sonnet-5-5", model + " ", model + "\n"} {
		require.False(t, isModelBudgetError(402, []byte(body), requested))
	}
	for _, mutate := range []func(map[string]any){
		func(v map[string]any) { v["status_code"] = 503 },
		func(v map[string]any) { v["type"] = "success" },
		func(v map[string]any) { v["type"] = nil },
		func(v map[string]any) { v["status_code"] = nil },
		func(v map[string]any) { v["status_code"] = "402" },
		func(v map[string]any) { v["error"].(map[string]any)["status_code"] = 500 },
		func(v map[string]any) { v["error"].(map[string]any)["code"] = "insufficient_quota" },
		func(v map[string]any) { v["error"].(map[string]any)["code"] = nil },
		func(v map[string]any) { v["error"].(map[string]any)["model"] = "claude-sonnet-5-5" },
		func(v map[string]any) { v["error"].(map[string]any)["type"] = "rate_limit" },
		func(v map[string]any) { v["error"].(map[string]any)["message"] = "insufficient budget" },
		func(v map[string]any) {
			e := v["error"].(map[string]any)
			e["message"] = e["message"].(string) + " https://private.invalid"
		},
	} {
		var v map[string]any
		require.NoError(t, json.Unmarshal([]byte(body), &v))
		mutate(v)
		raw, err := json.Marshal(v)
		require.NoError(t, err)
		require.False(t, isModelBudgetError(402, raw, model), string(raw))
	}
	for _, bad := range []string{
		body + " trailing", body[:len(body)-1], "[" + body + "]",
		strings.Replace(body, `"type":"budget_exceeded"`, `"type":"budget_exceeded","type":"budget_exceeded"`, 1),
		strings.Replace(body, `"type":"budget_exceeded"`, `"type":"budget_exceeded","TYPE":"budget_exceeded"`, 1),
		strings.Replace(body, `"help":`, `"message":"different","help":`, 1),
		strings.Replace(body, `"help":`, `"status_code":402,"status_code":402,"help":`, 1),
		strings.Replace(body, `"help":`, `"padding":"`+strings.Repeat("x", modelBudgetBodyLimit)+`","help":`, 1),
	} {
		require.False(t, isModelBudgetError(402, []byte(bad), model))
	}
}

func TestModelBudgetUnmatchedBodyKeepsReadErrorAndClose(t *testing.T) {
	for _, readErr := range []error{nil, io.ErrUnexpectedEOF} {
		for _, body := range []string{budgetTestBody("claude-opus-5-5", false), `{"error":{"message":"wallet empty"}}`, strings.Repeat("x", modelBudgetBodyLimit+33)} {
			if readErr == nil && body == budgetTestBody("claude-opus-5-5", false) {
				continue
			}
			reader := io.Reader(strings.NewReader(body))
			if readErr != nil {
				reader = io.MultiReader(reader, &streamReadCloser{sent: true, err: readErr})
			}
			source := &modelPermissionReadCloser{Reader: reader}
			resp := &http.Response{StatusCode: 402, Body: source}
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			s := &GatewayService{}
			result, err, handled := s.handleModelBudgetResponse(c, newAnthropicAPIKeyAccountForTest(), resp, "claude-opus-5-5", "claude-opus-5-5", false, time.Now())
			require.False(t, handled)
			require.Nil(t, result)
			require.NoError(t, err)
			require.False(t, source.closed)
			actual, err := io.ReadAll(resp.Body)
			require.ErrorIs(t, err, readErr)
			require.Equal(t, body, string(actual))
			require.NoError(t, resp.Body.Close())
			require.True(t, source.closed)
		}
	}
}

func TestModelBudgetCommittedStreamPreservesStatusAndObservedBuckets(t *testing.T) {
	for _, path := range []string{"/v1/messages", "/v1/chat/completions", "/v1/responses"} {
		t.Run(path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest("POST", path, nil)
			c.Header("Content-Type", "text/event-stream")
			_, _ = c.Writer.WriteString(": ping\n\n")
			c.Writer.Flush()
			s := newForwardPartialUsageServiceForTest(nil)
			body := budgetTestBody("claude-opus-5-5", true)
			resp := &http.Response{StatusCode: 402, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}
			result, err, handled := s.handleModelBudgetResponse(c, newAnthropicAPIKeyAccountForTest(), resp, "claude-opus-5-5", "claude-opus-5-5", true, time.Now())
			require.True(t, handled)
			require.Error(t, err)
			require.NotNil(t, result)
			require.Equal(t, 12, result.Usage.InputTokens)
			require.Equal(t, 7, result.Usage.OutputTokens)
			require.Equal(t, 5, result.Usage.CacheReadInputTokens)
			require.Equal(t, 2, result.Usage.CacheCreation5mTokens)
			require.Equal(t, 3, result.Usage.CacheCreation1hTokens)
			require.Equal(t, 200, rec.Code)
			require.Contains(t, rec.Body.String(), `"status_code":402`)
			require.Equal(t, 1, strings.Count(rec.Body.String(), `"code":"upstream_model_budget_exhausted"`))
			require.NotContains(t, rec.Body.String(), "private.invalid")
			require.NotContains(t, rec.Body.String(), "secret")
		})
	}
}

func TestModelBudgetDoesNotPauseOAuthOrGeneric402(t *testing.T) {
	for _, oauth := range []bool{false, true} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
		a := newAnthropicAPIKeyAccountForTest()
		body := `{"error":{"type":"billing_error","message":"wallet depleted"}}`
		if oauth {
			a.Type = AccountTypeOAuth
			body = budgetTestBody("claude-opus-5-5", false)
		}
		resp := &http.Response{StatusCode: 402, Body: io.NopCloser(strings.NewReader(body))}
		s := &GatewayService{}
		_, err, handled := s.handleModelBudgetResponse(c, a, resp, "claude-opus-5-5", "claude-opus-5-5", false, time.Now())
		require.False(t, handled)
		require.NoError(t, err)
		require.False(t, c.Writer.Written())
		got, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, body, string(got))
		require.NoError(t, resp.Body.Close())
	}
}
