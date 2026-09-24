//go:build unit

package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// An attempt that has already reported usage must retain its error and billing
// result, even when no model output has yet been committed to the client.
func TestOpenAIMeteredTerminalFailureDoesNotReplay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, platform := range []string{PlatformOpenAI, PlatformGrok} {
		for _, path := range []string{"responses_stream", "passthrough_stream", "responses_buffered", "passthrough_buffered", "chat_stream", "chat_buffered", "messages_stream", "messages_buffered"} {
			for _, metering := range []string{"none", "terminal", "prior_then_zero"} {
				t.Run(platform+"/"+path+"/"+metering, func(t *testing.T) {
					prefix, usage := "", `{"input_tokens":0,"output_tokens":0}`
					if metering == "terminal" {
						usage = `{"input_tokens":37,"output_tokens":2,"input_tokens_details":{"cached_tokens":11}}`
					} else if metering == "prior_then_zero" {
						prefix = "data: {\"type\":\"response.in_progress\",\"response\":{\"id\":\"resp_cost\",\"usage\":{\"input_tokens\":37,\"output_tokens\":2,\"input_tokens_details\":{\"cached_tokens\":11}}}}\n\n"
					}
					body := prefix + fmt.Sprintf("event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_cost\",\"model\":\"model\",\"status\":\"failed\",\"error\":{\"code\":\"rate_limit_exceeded\",\"message\":\"Rate limit reached\"},\"usage\":%s}}\n\n", usage)
					c, rec, resp, svc, account := meteredOpenAITestContext(platform, body)
					got, err := runMeteredOpenAIReader(svc, c, resp, account, path)
					require.Error(t, err)
					var failover *UpstreamFailoverError
					if metering == "none" {
						require.ErrorAs(t, err, &failover)
						require.Empty(t, rec.Body.String())
						return
					}
					require.NotErrorAs(t, err, &failover)
					require.NotNil(t, got)
					require.Equal(t, 37, got.InputTokens)
					require.Equal(t, 2, got.OutputTokens)
					require.Equal(t, 11, got.CacheReadInputTokens)
					require.Contains(t, strings.ToLower(rec.Body.String()), "rate limit")
					require.NotContains(t, rec.Body.String(), "response.completed")
					wireUsage, ok := extractOpenAIUsageFromJSONBytes(rec.Body.Bytes())
					if strings.Contains(rec.Header().Get("Content-Type"), "text/event-stream") {
						wireUsage = *svc.parseSSEUsageFromBody(rec.Body.String())
						ok = true
					}
					require.True(t, ok, "failure must expose measured usage to downstream billing")
					wantInput := 37
					if strings.HasPrefix(path, "messages_") {
						wantInput = 26 // Messages excludes the 11 cached input tokens.
					}
					require.Equal(t, wantInput, wireUsage.InputTokens)
					require.Equal(t, 2, wireUsage.OutputTokens)
					require.Equal(t, 11, wireUsage.CacheReadInputTokens)
				})
			}
		}
	}
}

func TestOpenAIMeteredGrokForwardRetainsUsage(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			body := `data: {"type":"response.failed","response":{"id":"resp_metered","status":"failed","error":{"code":"rate_limit_exceeded","message":"Rate limit reached"},"usage":{"input_tokens":37,"output_tokens":2}}}` + "\n\n"
			c, rec, resp, _, account := meteredOpenAITestContext(PlatformGrok, body)
			account.Type = AccountTypeAPIKey
			account.Credentials = map[string]any{"api_key": "test-only-token", "base_url": "https://api.x.ai/v1"}
			upstream := &httpUpstreamRecorder{responses: []*http.Response{resp}}
			svc := &OpenAIGatewayService{httpUpstream: upstream}
			request := []byte(fmt.Sprintf(`{"model":"grok-4.6","input":"test","stream":%v}`, stream))
			result, err := svc.forwardGrokResponses(c.Request.Context(), c, account, request, "grok-4.6", stream, time.Now())
			require.Error(t, err)
			var failover *UpstreamFailoverError
			require.NotErrorAs(t, err, &failover)
			require.NotNil(t, result)
			require.Equal(t, 37, result.Usage.InputTokens)
			require.Equal(t, "metered-attempt", result.RequestID)
			require.Len(t, upstream.requests, 1)
			require.Contains(t, rec.Body.String(), "Rate limit")
		})
	}
}

func TestOpenAIMeteredPassthroughForwardRetainsUsage(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			body := `data: {"type":"response.failed","response":{"id":"resp_metered","status":"failed","error":{"code":"rate_limit_exceeded","message":"Rate limit reached"},"usage":{"input_tokens":37,"output_tokens":2}}}` + "\n\n"
			c, _, resp, _, account := meteredOpenAITestContext(PlatformOpenAI, body)
			account.Type = AccountTypeAPIKey
			account.Credentials = map[string]any{"api_key": "test-only-token"}
			upstream := &httpUpstreamRecorder{responses: []*http.Response{resp}}
			svc := openAIClientToolsTestService(upstream)
			request := []byte(fmt.Sprintf(`{"model":"gpt-5.4","input":"test","stream":%v}`, stream))
			result, err := svc.forwardOpenAIPassthrough(c.Request.Context(), c, account, request, request, "gpt-5.4", false, nil, stream, time.Now())
			require.Error(t, err)
			var failover *UpstreamFailoverError
			require.NotErrorAs(t, err, &failover)
			require.NotNil(t, result)
			require.Equal(t, 37, result.Usage.InputTokens)
			require.Equal(t, "metered-attempt", result.RequestID)
			require.Len(t, upstream.requests, 1)
		})
	}
}

func TestOpenAIMeteredErrorMetadataAndCacheConversion(t *testing.T) {
	for _, path := range []string{"responses_buffered", "passthrough_buffered", "chat_buffered", "messages_buffered"} {
		t.Run(path, func(t *testing.T) {
			body := `data: {"type":"response.failed","response":{"id":"resp_metered","model":"actual-model","service_tier":"priority","status":"failed","error":{"code":"rate_limit_exceeded","message":"Rate limit reached"},"usage":{"input_tokens":37,"output_tokens":2,"cache_creation_input_tokens":3,"input_tokens_details":{"cached_tokens":11}}}}` + "\n\n"
			c, rec, resp, svc, account := meteredOpenAITestContext(PlatformOpenAI, body)
			got, err := runMeteredOpenAIReader(svc, c, resp, account, path)
			require.Error(t, err)
			require.NotNil(t, got)
			require.Equal(t, 37, got.InputTokens)
			require.Contains(t, rec.Body.String(), `"service_tier":"priority"`)
			require.Contains(t, rec.Body.String(), `"model":"actual-model"`)
			wire, ok := extractOpenAIUsageFromJSONBytes(rec.Body.Bytes())
			require.True(t, ok)
			wantInput := 37
			if path == "messages_buffered" {
				wantInput = 23
			}
			require.Equal(t, wantInput, wire.InputTokens)
			require.Equal(t, 11, wire.CacheReadInputTokens)
			require.Equal(t, 3, wire.CacheCreationInputTokens)
			require.Equal(t, http.StatusBadGateway, rec.Code)
		})
	}
}

func TestOpenAIMeteredReadFailureRetainsUsage(t *testing.T) {
	for _, path := range []string{"responses_stream", "passthrough_stream", "chat_buffered", "messages_buffered", "messages_stream", "chat_stream"} {
		for _, readError := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/read_error_%v", path, readError), func(t *testing.T) {
				body := "data: {\"type\":\"response.in_progress\",\"response\":{\"id\":\"resp_cost\",\"usage\":{\"input_tokens\":37,\"output_tokens\":2}}}\n\n"
				c, rec, resp, svc, account := meteredOpenAITestContext(PlatformOpenAI, body)
				if readError {
					resp.Body = io.NopCloser(io.MultiReader(strings.NewReader(body), &openAICompatBufferedReadErrorCloser{err: io.ErrUnexpectedEOF}))
				}
				got, err := runMeteredOpenAIReader(svc, c, resp, account, path)
				require.Error(t, err)
				var failover *UpstreamFailoverError
				require.NotErrorAs(t, err, &failover)
				require.NotNil(t, got)
				require.Equal(t, 37, got.InputTokens)
				require.Equal(t, 2, got.OutputTokens)
				wire, ok := extractOpenAIUsageFromJSONBytes(rec.Body.Bytes())
				if strings.Contains(rec.Header().Get("Content-Type"), "text/event-stream") {
					wire = *svc.parseSSEUsageFromBody(rec.Body.String())
					ok = true
				}
				require.True(t, ok)
				require.Equal(t, 37, wire.InputTokens)
				require.Equal(t, 2, wire.OutputTokens)
			})
		}
	}
}

func TestOpenAIMeteredCyberRetainsUsage(t *testing.T) {
	for _, path := range []string{"responses_buffered", "passthrough_buffered", "chat_buffered", "messages_buffered", "responses_stream", "passthrough_stream", "chat_stream", "messages_stream"} {
		t.Run(path, func(t *testing.T) {
			body := `data: {"type":"response.failed","response":{"status":"failed","error":{"code":"cyber_policy","message":"Request blocked by cyber policy"},"usage":{"input_tokens":37,"output_tokens":2}}}` + "\n\n"
			c, rec, resp, svc, account := meteredOpenAITestContext(PlatformOpenAI, body)
			got, err := runMeteredOpenAIReader(svc, c, resp, account, path)
			require.Error(t, err)
			require.NotNil(t, got)
			require.Equal(t, 37, got.InputTokens)
			require.NotNil(t, GetOpsCyberPolicy(c))
			require.Contains(t, rec.Body.String(), `"usage"`)
			var failover *UpstreamFailoverError
			require.NotErrorAs(t, err, &failover)
		})
	}
}

func TestOpenAIMeteredStageOverflowReturnsBoundedFailure(t *testing.T) {
	prefix := "data: {\"type\":\"response.in_progress\",\"response\":{\"usage\":{\"input_tokens\":37,\"output_tokens\":2}}}\n\n"
	line := `data: {"type":"response.output_text.delta","delta":"` + strings.Repeat("x", 1024*1024-256) + `"}`
	c, rec, resp, svc, account := meteredOpenAITestContext(PlatformOpenAI, prefix+strings.Repeat(line+"\n", 9))
	svc.cfg.Gateway.MaxLineSize = 2 * 1024 * 1024
	got, err := runMeteredOpenAIReader(svc, c, resp, account, "responses_stream")
	require.Error(t, err)
	var failover *UpstreamFailoverError
	require.NotErrorAs(t, err, &failover)
	require.NotNil(t, got)
	require.Equal(t, 37, got.InputTokens)
	require.Contains(t, rec.Body.String(), "response_too_large")
	require.Less(t, rec.Body.Len(), 1024, "do not release the unfinished oversized event")
	wire := svc.parseSSEUsageFromBody(rec.Body.String())
	require.Equal(t, 37, wire.InputTokens)
	require.Equal(t, 2, wire.OutputTokens)
}

func meteredOpenAITestContext(platform, body string) (*gin.Context, *httptest.ResponseRecorder, *http.Response, *OpenAIGatewayService, *Account) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}, "X-Request-Id": {"metered-attempt"}}, Body: io.NopCloser(strings.NewReader(body))}
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}}
	return c, rec, resp, svc, &Account{ID: 7, Platform: platform, Type: AccountTypeOAuth}
}

func runMeteredOpenAIReader(svc *OpenAIGatewayService, c *gin.Context, resp *http.Response, account *Account, path string) (*OpenAIUsage, error) {
	var got *OpenAIUsage
	var err error
	switch path {
	case "responses_stream":
		var r *openaiStreamingResult
		r, err = svc.handleStreamingResponse(context.Background(), resp, c, account, time.Now(), "model", "model")
		if r != nil {
			got = r.usage
		}
	case "passthrough_stream":
		var r *openaiStreamingResultPassthrough
		r, err = svc.handleStreamingResponsePassthrough(context.Background(), resp, c, account, time.Now(), "model", "model")
		if r != nil {
			got = r.usage
		}
	case "responses_buffered":
		var r *openaiNonStreamingResult
		r, err = svc.handleNonStreamingResponse(context.Background(), resp, c, account, "model", "model")
		if r != nil {
			got = r.usage
		}
	case "passthrough_buffered":
		var r *openaiNonStreamingResultPassthrough
		r, err = svc.handleNonStreamingResponsePassthrough(context.Background(), resp, c, account, "model", "model")
		if r != nil {
			got = r.usage
		}
	default:
		var r *OpenAIForwardResult
		switch path {
		case "chat_stream":
			r, err = svc.handleChatStreamingResponse(resp, c, account, "model", "model", "model", time.Now(), 0)
		case "chat_buffered":
			r, err = svc.handleChatBufferedStreamingResponse(resp, c, account, "model", "model", "model", time.Now())
		case "messages_stream":
			r, err = svc.handleAnthropicStreamingResponse(resp, c, account, "model", "model", "model", time.Now())
		case "messages_buffered":
			r, err = svc.handleAnthropicBufferedStreamingResponse(resp, c, account, "model", "model", "model", time.Now())
		}
		if r != nil {
			got = &r.Usage
		}
	}
	return got, err
}
