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

	"github.com/Wei-Shaw/sub2api/internal/config"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestGrokSemanticOutputClassifier(t *testing.T) {
	for _, tc := range []struct {
		name, payload string
		want          bool
	}{
		{"created", `{"type":"response.created","response":{"id":"r"}}`, false},
		{"progress", `{"type":"response.in_progress"}`, false},
		{"ping", `{"type":"ping","delta":"not model output"}`, false},
		{"unknown delta", `{"type":"response.queue.delta","delta":"waiting"}`, false},
		{"empty text", `{"type":"response.output_text.delta","delta":""}`, false},
		{"wrong text type", `{"type":"response.output_text.delta","delta":42}`, false},
		{"empty reasoning item", `{"type":"response.output_item.added","item":{"type":"reasoning","summary":[]}}`, false},
		{"empty tool", `{"type":"response.output_item.added","item":{"type":"function_call","name":"search","arguments":""}}`, false},
		{"usage only", `{"type":"response.in_progress","usage":{"input_tokens":7}}`, false},
		{"text", `{"type":"response.output_text.delta","delta":"hello"}`, true},
		{"reasoning", `{"type":"response.reasoning_text.delta","delta":"thinking"}`, true},
		{"summary", `{"type":"response.reasoning_summary_text.delta","delta":"thinking"}`, true},
		{"tool args", `{"type":"response.function_call_arguments.delta","delta":"{"}`, true},
		{"custom tool", `{"type":"response.custom_tool_call_input.delta","delta":"search"}`, true},
		{"encrypted reasoning", `{"type":"response.output_item.added","item":{"type":"reasoning","encrypted_content":"opaque"}}`, true},
		{"item result", `{"type":"response.output_item.done","item":{"type":"message","content":[{"type":"output_text","text":"done"}]}}`, true},
		{"chat role", `{"choices":[{"delta":{"role":"assistant"}}]}`, false},
		{"chat text", `{"choices":[{"delta":{"content":"yes"}}]}`, true},
		{"chat reasoning", `{"choices":[{"delta":{"reasoning_content":"thinking"}}]}`, true},
		{"chat tool", `{"choices":[{"delta":{"tool_calls":[{"function":{"arguments":"{}"}}]}}]}`, true},
		{"chat identity", `{"choices":[{"delta":{"tool_calls":[{"id":"call_1","function":{"name":"search"}}]}}]}`, false},
		{"chat usage", `{"choices":[],"usage":{"prompt_tokens":7,"completion_tokens":0}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frame := []byte("data: " + tc.payload + "\n\n")
			require.Equal(t, tc.want, grokSSEFrameHasSemanticOutput(frame))
			var observer grokSemanticSSEObserver
			for i, b := range frame {
				got := observer.feed([]byte{b})
				require.Equal(t, tc.want && i == len(frame)-1, got)
			}
		})
	}
	require.False(t, grokSSEFrameHasSemanticOutput([]byte(": ping\n\n")))
	require.True(t, grokSSEFrameHasSemanticOutput([]byte("event: response.reasoning_text.delta\r\ndata: {\"delta\":\"yes\"}\r\n\r\n")))
}

func TestGrokSemanticObserverBoundsPreamble(t *testing.T) {
	var o grokSemanticSSEObserver
	require.False(t, o.feed([]byte("data: "+strings.Repeat("x", 2*openAIFirstOutputStageMaxBytes)+"\n\n")))
	require.LessOrEqual(t, len(o.frame), int(openAIFirstOutputStageMaxBytes))
	require.True(t, o.feed([]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"yes\"}\n\n")))
}

func TestGrokSemanticBudgetSharedAcrossAccounts(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	req := httptest.NewRequest("POST", "/v1/responses", nil)
	cfg := &config.Config{Gateway: config.GatewayConfig{GrokFirstSemanticTimeoutSeconds: 1200}}
	a := &Account{ID: 1, Platform: PlatformGrok}
	first := withGrokFirstSemanticBudget(req, c, a, cfg, true)
	budget := first.Context().Value(grokFirstSemanticContextKey{}).(*grokFirstSemanticBudget)
	budget.deadline = time.Now().Add(-time.Second)
	second := withGrokFirstSemanticBudget(req, c, &Account{ID: 2, Platform: PlatformGrok}, cfg, true)
	require.Same(t, budget, second.Context().Value(grokFirstSemanticContextKey{}))
	_, err := doGrokFirstSemanticUpstream(second, func(*http.Request) (*http.Response, error) {
		t.Fatal("exhausted request must not try another account")
		return nil, nil
	})
	var timeout *grokFirstSemanticTimeoutError
	require.ErrorAs(t, err, &timeout)
	for _, tt := range []struct {
		name    string
		cfg     *config.Config
		account *Account
		stream  bool
	}{
		{"disabled", &config.Config{}, a, true}, {"nonstream", cfg, a, false}, {"other platform", cfg, &Account{Platform: PlatformOpenAI}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Same(t, req, withGrokFirstSemanticBudget(req, c, tt.account, tt.cfg, tt.stream))
		})
	}
}

func semanticTestResponse(t *testing.T, budget time.Duration, prefix string) (*http.Response, <-chan struct{}) {
	t.Helper()
	reader, writer := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer writer.Close()
		if _, err := io.WriteString(writer, prefix); err != nil {
			return
		}
		for {
			if _, err := io.WriteString(writer, ": heartbeat\n\n"); err != nil {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	t.Cleanup(func() {
		_ = reader.Close()
		_ = writer.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("synthetic source did not stop")
		}
	})
	req := httptest.NewRequest("POST", "/v1/responses", nil)
	b := &grokFirstSemanticBudget{deadline: time.Now().Add(budget), timeout: budget}
	req = req.WithContext(context.WithValue(req.Context(), grokFirstSemanticContextKey{}, b))
	resp, err := doGrokFirstSemanticUpstream(req, func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: reader}, nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp, done
}

func TestGrokSemanticTimeoutStopsHeartbeatAndHeaderWait(t *testing.T) {
	t.Run("heartbeats", func(t *testing.T) {
		resp, done := semanticTestResponse(t, 40*time.Millisecond, "data: {\"type\":\"response.in_progress\"}\n\n")
		_, err := io.ReadAll(resp.Body)
		var timeout *grokFirstSemanticTimeoutError
		require.ErrorAs(t, err, &timeout)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("timeout must close upstream")
		}
	})
	t.Run("headers", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/v1/responses", nil)
		b := &grokFirstSemanticBudget{deadline: time.Now().Add(20 * time.Millisecond), timeout: 20 * time.Millisecond}
		req = req.WithContext(context.WithValue(req.Context(), grokFirstSemanticContextKey{}, b))
		_, err := doGrokFirstSemanticUpstream(req, func(r *http.Request) (*http.Response, error) { <-r.Context().Done(); return nil, r.Context().Err() })
		var timeout *grokFirstSemanticTimeoutError
		require.ErrorAs(t, err, &timeout)
	})
}

func TestGrokSemanticProgressDoesNotLimitGeneration(t *testing.T) {
	for _, payload := range []string{
		`{"type":"response.output_text.delta","delta":"hello"}`,
		`{"type":"response.reasoning_summary_text.delta","delta":"thinking"}`,
		`{"type":"response.function_call_arguments.delta","delta":"{"}`,
		`{"choices":[{"delta":{"reasoning_content":"thinking"}}]}`,
	} {
		t.Run(payload, func(t *testing.T) {
			resp, _ := semanticTestResponse(t, 60*time.Millisecond, "data: "+payload+"\n\n")
			first := make([]byte, 4096)
			_, err := resp.Body.Read(first)
			require.NoError(t, err)
			time.Sleep(100 * time.Millisecond)
			_, err = resp.Body.Read(first)
			require.NoError(t, err, "valid long generation must outlive first-output budget")
		})
	}
}

func TestGrokSemanticTimeoutAllResponseAdapters(t *testing.T) {
	for _, mode := range []string{"responses", "chat", "messages", "raw-chat", "buffered-chat", "buffered-messages"} {
		t.Run(mode, func(t *testing.T) {
			prefix := "data: {\"type\":\"response.in_progress\",\"usage\":{\"input_tokens\":7,\"output_tokens\":0}}\n\n"
			path := "/v1/responses"
			if strings.Contains(mode, "chat") {
				path = "/v1/chat/completions"
			}
			if strings.Contains(mode, "messages") {
				path = "/v1/messages"
			}
			if mode == "raw-chat" {
				prefix = "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":0}}\n\n"
			}
			resp, _ := semanticTestResponse(t, 40*time.Millisecond, prefix)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest("POST", path, nil)
			svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}}
			account := &Account{ID: 7, Platform: PlatformGrok, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true}
			var usage *OpenAIUsage
			var err error
			switch mode {
			case "responses":
				var result *openaiStreamingResult
				result, err = svc.handleStreamingResponse(context.Background(), resp, c, account, time.Now(), "grok-4.6", "grok-4.6")
				require.NotNil(t, result)
				usage = result.usage
			default:
				var result *OpenAIForwardResult
				switch mode {
				case "chat":
					result, err = svc.handleChatStreamingResponse(resp, c, account, "grok-4.6", "grok-4.6", "grok-4.6", time.Now(), 0)
				case "messages":
					result, err = svc.handleAnthropicStreamingResponse(resp, c, account, "grok-4.6", "grok-4.6", "grok-4.6", time.Now())
				case "raw-chat":
					result, err = svc.streamRawChatCompletions(c, resp, account, "grok-4.6", "grok-4.6", "grok-4.6", nil, nil, time.Now(), 0)
				case "buffered-chat":
					result, err = svc.handleChatBufferedStreamingResponse(resp, c, account, "grok-4.6", "grok-4.6", "grok-4.6", time.Now())
				case "buffered-messages":
					result, err = svc.handleAnthropicBufferedStreamingResponse(resp, c, account, "grok-4.6", "grok-4.6", "grok-4.6", time.Now())
				}
				require.NotNil(t, result)
				usage = &result.Usage
			}
			var timeout *grokFirstSemanticTimeoutError
			require.ErrorAs(t, err, &timeout)
			var failover *UpstreamFailoverError
			require.False(t, errors.As(err, &failover))
			require.Equal(t, 7, usage.InputTokens)
			require.Zero(t, usage.OutputTokens)
			require.Contains(t, recorder.Body.String(), grokFirstSemanticTimeoutMessage)
			require.Nil(t, account.TempUnschedulableUntil)
			require.True(t, account.Schedulable)
			raw, ok := c.Get(OpsUpstreamErrorsKey)
			require.True(t, ok)
			events := raw.([]*OpsUpstreamErrorEvent)
			require.Len(t, events, 1)
			require.Equal(t, grokFirstSemanticTimeoutCode, events[0].Kind)
		})
	}
}

func TestGrokSemanticBudgetRequestBuilders(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
	account := grokChatBridgeTestAccount(71)
	cfg := &config.Config{Gateway: config.GatewayConfig{GrokFirstSemanticTimeoutSeconds: 1200}}
	req, err := buildGrokResponsesRequest(context.Background(), c, account, []byte(`{"model":"grok-4.6","stream":true}`), "synthetic", "", cfg)
	require.NoError(t, err)
	require.NotNil(t, req.Context().Value(grokFirstSemanticContextKey{}))
}

func TestGrokSemanticTimeoutWithoutUsageAndTransportDoNotRetry(t *testing.T) {
	account := &Account{ID: 7, Platform: PlatformGrok, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true}
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
	resp, _ := semanticTestResponse(t, 30*time.Millisecond, "data: {\"type\":\"response.in_progress\"}\n\n")
	result, err := svc.handleStreamingResponse(context.Background(), resp, c, account, time.Now(), "grok-4.6", "grok-4.6")
	require.Error(t, err)
	require.NotNil(t, result)
	require.Zero(t, *result.usage)
	require.Equal(t, http.StatusGatewayTimeout, recorder.Code)
	var timeout *grokFirstSemanticTimeoutError
	require.ErrorAs(t, err, &timeout)
	recorder2 := httptest.NewRecorder()
	c2, _ := gin.CreateTestContext(recorder2)
	c2.Request = httptest.NewRequest("POST", "/v1/chat/completions", nil)
	got := svc.handleOpenAIUpstreamTransportError(context.Background(), c2, account, err, false)
	require.Same(t, err, got)
	require.Equal(t, 504, recorder2.Code)
	var retry *UpstreamFailoverError
	require.False(t, errors.As(got, &retry))
	require.Nil(t, account.TempUnschedulableUntil)
}

func TestGrokSemanticHTTPErrorAndOtherPlatformUntouched(t *testing.T) {
	for _, status := range []int{400, 429} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			req := httptest.NewRequest("POST", "/v1/responses", nil)
			b := &grokFirstSemanticBudget{deadline: time.Now().Add(15 * time.Millisecond), timeout: 15 * time.Millisecond}
			req = req.WithContext(context.WithValue(req.Context(), grokFirstSemanticContextKey{}, b))
			var upstreamCtx context.Context
			resp, err := doGrokFirstSemanticUpstream(req, func(r *http.Request) (*http.Response, error) {
				upstreamCtx = r.Context()
				return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"unchanged":true}`))}, nil
			})
			require.NoError(t, err)
			time.Sleep(25 * time.Millisecond)
			require.NoError(t, upstreamCtx.Err())
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, `{"unchanged":true}`, string(body))
			require.NoError(t, resp.Body.Close())
			require.ErrorIs(t, upstreamCtx.Err(), context.Canceled)
		})
	}
	req := httptest.NewRequest("POST", "/v1/responses", nil)
	original := &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(": ping\n\n"))}
	resp, err := doGrokFirstSemanticUpstream(req, func(got *http.Request) (*http.Response, error) { require.Same(t, req, got); return original, nil })
	require.NoError(t, err)
	require.Same(t, original, resp)
}

func TestGrokSemanticRawChatBuilderUsesSharedBudget(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/chat/completions", nil)
	account := grokChatBridgeTestAccount(71)
	cfg := &config.Config{Gateway: config.GatewayConfig{GrokFirstSemanticTimeoutSeconds: 1200}}
	upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("data: [DONE]\n\n"))}}
	svc := &OpenAIGatewayService{cfg: cfg, httpUpstream: upstream}
	resp, err := svc.sendCCUpstreamRequest(context.Background(), c, account, "https://example.test/v1/chat/completions", []byte(`{"model":"grok-4.6","stream":true}`), true, "synthetic", "", "")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.NotNil(t, upstream.lastReq.Context().Value(grokFirstSemanticContextKey{}))
	_, ok := resp.Body.(*grokFirstSemanticGuard)
	require.True(t, ok)
}

func TestGrokSemanticBudgetIsScopedToWebSocketTurn(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	req := httptest.NewRequest("GET", "/v1/responses", nil)
	account := &Account{Platform: PlatformGrok}
	cfg := &config.Config{Gateway: config.GatewayConfig{GrokFirstSemanticTimeoutSeconds: 1200}}
	first := withGrokFirstSemanticBudget(req, c, account, cfg, true).Context().Value(grokFirstSemanticContextKey{}).(*grokFirstSemanticBudget)
	first.observed.Store(true)
	first.deadline = time.Now().Add(-time.Second)
	c.Set(grokFirstSemanticTimeoutCode, true)
	// Same turn retries cannot buy another deadline, even after budget exhaustion.
	require.Same(t, first, withGrokFirstSemanticBudget(req, c, account, cfg, true).Context().Value(grokFirstSemanticContextKey{}))
	resetGrokFirstSemanticBudget(c)
	second := withGrokFirstSemanticBudget(req, c, account, cfg, true).Context().Value(grokFirstSemanticContextKey{}).(*grokFirstSemanticBudget)
	require.NotSame(t, first, second)
	require.False(t, second.observed.Load())
	require.True(t, second.deadline.After(time.Now()))
	handled, _ := c.Get(grokFirstSemanticTimeoutCode)
	require.Equal(t, false, handled)
	require.Same(t, second, withGrokFirstSemanticBudget(req, c, account, cfg, true).Context().Value(grokFirstSemanticContextKey{}))
}

func TestGrokSemanticWebSocketTimeoutDoesNotWriteHTTPOrRetry(t *testing.T) {
	for _, turn := range []int{1, 2} {
		t.Run(string(rune('0'+turn)), func(t *testing.T) {
			resp, _ := semanticTestResponse(t, 40*time.Millisecond, "data: {\"type\":\"response.in_progress\"}\n\n")
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest("GET", "/v1/responses", nil)
			account := grokChatBridgeTestAccount(71)
			upstream := &httpUpstreamRecorder{resp: resp}
			svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{GrokFirstSemanticTimeoutSeconds: 1200}}, httpUpstream: upstream}
			payload := []byte(`{"type":"response.create","model":"grok-4.6","input":"test"}`)
			var messages []string
			result, err := svc.proxyOpenAIWSHTTPBridgeTurn(context.Background(), c, account, "synthetic", payload, len(payload), "grok-4.6", "", "", "", "", turn, func(p []byte) error { messages = append(messages, string(p)); return nil })
			require.NotNil(t, result)
			require.Zero(t, result.Usage)
			var timeout *grokFirstSemanticTimeoutError
			require.ErrorAs(t, err, &timeout)
			var failover *UpstreamFailoverError
			require.False(t, errors.As(err, &failover))
			require.Empty(t, recorder.Body.String(), "hijacked WebSocket must not receive HTTP writes")
			require.Contains(t, strings.Join(messages, "\n"), grokFirstSemanticTimeoutCode)
			require.Contains(t, strings.Join(messages, "\n"), `"status":504`)
		})
	}
}

func TestGrokSemanticWrongContentTypeCannotBypassDeadline(t *testing.T) {
	for _, contentType := range []string{"", "application/json"} {
		t.Run(contentType, func(t *testing.T) {
			reader, writer := io.Pipe()
			defer reader.Close()
			defer writer.Close()
			req := httptest.NewRequest("POST", "/v1/responses", nil)
			budget := &grokFirstSemanticBudget{deadline: time.Now().Add(30 * time.Millisecond), timeout: 30 * time.Millisecond}
			req = req.WithContext(context.WithValue(req.Context(), grokFirstSemanticContextKey{}, budget))
			resp, err := doGrokFirstSemanticUpstream(req, func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{contentType}}, Body: reader}, nil
			})
			require.NoError(t, err)
			defer resp.Body.Close()
			_, err = io.ReadAll(resp.Body)
			var timeout *grokFirstSemanticTimeoutError
			require.ErrorAs(t, err, &timeout)
		})
	}
}

func TestGrokSemanticParentCancelAndClosePreserveCancellation(t *testing.T) {
	for _, alreadyCanceled := range []bool{false, true} {
		t.Run(map[bool]string{true: "before-send", false: "during-headers"}[alreadyCanceled], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			req := httptest.NewRequest("POST", "/v1/responses", nil).WithContext(ctx)
			b := &grokFirstSemanticBudget{deadline: time.Now().Add(time.Second), timeout: time.Second}
			req = req.WithContext(context.WithValue(req.Context(), grokFirstSemanticContextKey{}, b))
			if alreadyCanceled {
				cancel()
			}
			_, err := doGrokFirstSemanticUpstream(req, func(r *http.Request) (*http.Response, error) {
				if alreadyCanceled {
					t.Fatal("canceled request must not send")
				}
				cancel()
				<-r.Context().Done()
				return nil, r.Context().Err()
			})
			require.ErrorIs(t, err, context.Canceled)
			var timeout *grokFirstSemanticTimeoutError
			require.False(t, errors.As(err, &timeout))
		})
	}
	resp, _ := semanticTestResponse(t, 30*time.Millisecond, ": heartbeat\n\n")
	require.NoError(t, resp.Body.Close())
	time.Sleep(50 * time.Millisecond)
	_, err := resp.Body.Read(make([]byte, 128))
	var timeout *grokFirstSemanticTimeoutError
	require.False(t, errors.As(err, &timeout), "closed source must not turn into timeout")
}

func TestGrokSemanticMessagesTimeoutCacheAccounting(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
	usage := &OpenAIUsage{InputTokens: 10, CacheReadInputTokens: 4, CacheCreationInputTokens: 2}
	err := &grokFirstSemanticTimeoutError{&grokFirstSemanticBudget{deadline: time.Now(), timeout: time.Second}}
	require.True(t, handleGrokFirstSemanticTimeout(c, &Account{Platform: PlatformGrok}, err, usage))
	require.Equal(t, 504, recorder.Code)
	require.Equal(t, "error", gjson.Get(recorder.Body.String(), "type").String())
	require.EqualValues(t, 4, gjson.Get(recorder.Body.String(), "usage.input_tokens").Int())
	require.EqualValues(t, 4, gjson.Get(recorder.Body.String(), "usage.cache_read_input_tokens").Int())
	require.EqualValues(t, 2, gjson.Get(recorder.Body.String(), "usage.cache_creation_input_tokens").Int())
	require.Equal(t, 10, usage.InputTokens, "accounting source must stay Responses-inclusive")
}

func TestGrokSemanticTimeoutUsageBillingEligibility(t *testing.T) {
	account := &Account{Platform: PlatformGrok}
	timeout := &grokFirstSemanticTimeoutError{&grokFirstSemanticBudget{deadline: time.Now()}}
	for _, usage := range []OpenAIUsage{{InputTokens: 7}, {OutputTokens: 2}, {CacheReadInputTokens: 3}, {CacheCreationInputTokens: 4}, {ImageInputTokens: 2}, {ImageOutputTokens: 2}} {
		result := &OpenAIForwardResult{Usage: usage}
		require.True(t, ShouldRecordGrokFirstSemanticTimeoutUsage(account, result, timeout))
		require.False(t, ShouldRecordGrokFirstSemanticTimeoutUsage(&Account{Platform: PlatformOpenAI}, result, timeout))
		require.False(t, ShouldRecordGrokFirstSemanticTimeoutUsage(account, result, context.DeadlineExceeded))
	}
	require.False(t, ShouldRecordGrokFirstSemanticTimeoutUsage(account, &OpenAIForwardResult{}, timeout))
	require.False(t, ShouldRecordGrokFirstSemanticTimeoutUsage(account, nil, timeout))
}

func TestGrokSemanticWebSocketOuterRetryRetainsLogicalTurnBudget(t *testing.T) {
	firstBody := `data: {"type":"response.output_text.delta","delta":"ok"}` + "\n\n" + `data: {"type":"response.completed","response":{"id":"resp_first","status":"completed","usage":{"input_tokens":4,"output_tokens":1}}}` + "\n\n"
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(firstBody))},
		{StatusCode: 429, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"error":{"message":"Concurrency limit exceeded for account, please retry later"}}`))},
	}}
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize, GrokFirstSemanticTimeoutSeconds: 1200}}, httpUpstream: upstream}
	account := grokChatBridgeTestAccount(71)
	type outcome struct {
		first, failed, retried *grokFirstSemanticBudget
		firstErr, retryErr     error
		calls                  int
		retrySafe              bool
	}
	done := make(chan outcome, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			done <- outcome{firstErr: err}
			return
		}
		defer conn.CloseNow()
		_, initial, err := conn.Read(r.Context())
		if err != nil {
			done <- outcome{firstErr: err}
			return
		}
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = r
		c.Set("api_key", &APIKey{ID: 7101})
		out := outcome{}
		hooks := &OpenAIWSIngressHooks{AfterTurn: func(turn int, _ *OpenAIForwardResult, _ error) {
			value, _ := c.Get(grokFirstSemanticBudgetKey)
			if turn == 1 {
				out.first, _ = value.(*grokFirstSemanticBudget)
			}
		}}
		out.firstErr = svc.ProxyResponsesWebSocketFromClient(r.Context(), c, conn, account, "synthetic", initial, hooks)
		value, _ := c.Get(grokFirstSemanticBudgetKey)
		out.failed, _ = value.(*grokFirstSemanticBudget)
		retryPayload, safe := OpenAIWSCurrentTurnRetryPayload(out.firstErr)
		out.retrySafe = safe
		if out.failed != nil && safe {
			// Previous attempt is closed: expire the existing logical turn without waiting twenty minutes.
			out.failed.deadline = time.Now().Add(-time.Second)
			out.retryErr = svc.ProxyResponsesWebSocketFromClient(r.Context(), c, conn, grokChatBridgeTestAccount(72), "synthetic", retryPayload, nil)
			value, _ = c.Get(grokFirstSemanticBudgetKey)
			out.retried, _ = value.(*grokFirstSemanticBudget)
		}
		out.calls = len(upstream.requests)
		done <- out
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	require.NoError(t, err)
	defer client.CloseNow()
	require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"grok-4.6","input":"first"}`)))
	for {
		_, data, err := client.Read(ctx)
		require.NoError(t, err)
		if gjson.GetBytes(data, "type").String() == "response.completed" {
			break
		}
	}
	require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"grok-4.6","input":"second"}`)))
	_, message, err := client.Read(ctx)
	require.NoError(t, err)
	require.Contains(t, string(message), grokFirstSemanticTimeoutCode)
	select {
	case out := <-done:
		var failover *UpstreamFailoverError
		require.ErrorAs(t, out.firstErr, &failover)
		require.True(t, out.retrySafe)
		require.NotNil(t, out.first)
		require.NotNil(t, out.failed)
		require.NotSame(t, out.first, out.failed, "new client frame starts new budget")
		require.True(t, out.first.observed.Load())
		require.False(t, out.failed.observed.Load())
		require.Same(t, out.failed, out.retried, "outer retry must keep second turn budget when turn number restarts at one")
		var timeout *grokFirstSemanticTimeoutError
		require.ErrorAs(t, out.retryErr, &timeout)
		require.Equal(t, 2, out.calls, "expired logical turn must never send another account request")
	case <-ctx.Done():
		t.Fatal("proxy did not finish")
	}
}

func TestGrokSemanticRawChatDefaultMessageEvent(t *testing.T) {
	require.True(t, grokSSEFrameHasSemanticOutput([]byte("event: message\ndata: {\"choices\":[{\"delta\":{\"reasoning_content\":\"thinking\"}}]}\n\n")))
	require.False(t, grokSSEFrameHasSemanticOutput([]byte("event: metadata\ndata: {\"choices\":[{\"delta\":{\"content\":\"metadata\"}}]}\n\n")))
}

func TestGrokSemanticEnabledCanceledClientStillSettlesSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/responses", nil).WithContext(ctx)
	body := `data: {"type":"response.output_text.delta","delta":"visible"}` + "\n\n" + `data: {"type":"response.completed","response":{"id":"resp_usage","status":"completed","usage":{"input_tokens":12,"output_tokens":5}}}` + "\n\n"
	upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(body))}}
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{GrokFirstSemanticTimeoutSeconds: 1200}}, httpUpstream: upstream}
	account := &Account{ID: 71, Platform: PlatformGrok, Type: AccountTypeAPIKey, Credentials: map[string]any{"base_url": "https://api.x.ai/v1", "api_key": "synthetic"}}
	result, err := svc.forwardGrokResponses(ctx, c, account, []byte(`{"model":"grok-4.6","stream":true,"input":"test"}`), "grok-4.6", true, time.Now())
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 12, result.Usage.InputTokens)
	require.Equal(t, 5, result.Usage.OutputTokens)
	require.Len(t, upstream.requests, 1)
	budget := upstream.lastReq.Context().Value(grokFirstSemanticContextKey{}).(*grokFirstSemanticBudget)
	require.True(t, budget.observed.Load(), "successful semantic output disarms the active configured budget despite downstream cancellation")
}
