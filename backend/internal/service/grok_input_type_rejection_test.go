//go:build unit

package service

import (
	"bytes"
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
	"github.com/tidwall/gjson"
)

const grokUnknownInputTypeTestBody = `{"error":"Failed to deserialize the JSON body into the target type: input[0]: unknown item type \"agent_message\"; expected one of: message, reasoning, function_call, function_call_output, shell_call, shell_call_output, web_search_call, file_search_call, code_interpreter_call, mcp_call, custom_tool_call, image_generation_call, compaction"}`

func TestGrokUnknownInputItemTypeClassificationIsNarrow(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name     string
		status   int
		body     string
		terminal bool
		failover bool
	}{
		{"explicit unsupported item", 422, grokUnknownInputTypeTestBody, true, false},
		{"nested error and dotted input", 422, `{"error":{"message":"could not decode input.3: unknown item type future_item"}}`, true, false},
		{"other status", 500, grokUnknownInputTypeTestBody, false, true},
		{"not a decoder rejection", 422, `{"error":"input[0]: unknown item type"}`, false, false},
		{"not an input path", 422, `{"error":"could not decode tools[0]: unknown item type"}`, false, false},
		{"generic ModelInput remains retryable", 422, `{"error":"Failed to deserialize input[0]: data did not match any variant of untagged enum ModelInput"}`, false, true},
		{"generic Content remains retryable", 422, `{"error":"Failed to deserialize messages[1].content: data did not match any variant of untagged enum Content"}`, false, true},
		{"compaction remains retryable", 422, `{"error":{"message":"Could not decode the compaction blob"}}`, false, true},
		{"rate limit remains retryable", 429, `{"error":{"message":"too many requests"}}`, false, true},
		{"server error remains retryable", 503, `{"error":{"message":"temporarily unavailable"}}`, false, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.terminal, isGrokUnknownInputItemTypeError(tt.status, []byte(tt.body)))
			require.Equal(t, tt.failover, (&OpenAIGatewayService{}).shouldFailoverGrokUpstreamError(tt.status, []byte(tt.body)))
		})
	}
}

func TestForwardGrokResponsesUnknownInputTypeStopsWithoutCoolingAccount(t *testing.T) {
	gin.SetMode(gin.TestMode)
	requestBody := []byte(`{"model":"grok-4.6","input":[{"type":"future_item","content":"visible context"},{"type":"reasoning","id":"rs_native","summary":[{"type":"summary_text","text":"keep visible summary"}],"encrypted_content":"keep-native-opaque-state"}],"stream":true}`)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(requestBody))
	upstream := &httpUpstreamRecorder{responses: []*http.Response{{
		StatusCode: http.StatusUnprocessableEntity,
		Header:     http.Header{"Xai-Request-Id": []string{"unsupported-input"}},
		Body:       io.NopCloser(strings.NewReader(grokUnknownInputTypeTestBody)),
	}}}
	repo := &grokQuotaAccountRepo{}
	svc := &OpenAIGatewayService{httpUpstream: upstream, accountRepo: repo}
	account := &Account{
		ID: 227901, Platform: PlatformGrok, Type: AccountTypeAPIKey, Status: StatusActive, Concurrency: 1,
		Credentials: map[string]any{"api_key": "test-only-token", "base_url": "https://api.x.ai/v1"},
	}
	result, err := svc.forwardGrokResponses(context.Background(), c, account, requestBody, "grok-4.6", true, time.Now())
	require.Nil(t, result)
	require.Error(t, err)
	var failoverErr *UpstreamFailoverError
	require.False(t, errors.As(err, &failoverErr), "the handler must not select another account for this request")
	require.Len(t, upstream.requests, 1)
	require.Equal(t, "keep-native-opaque-state", gjson.GetBytes(upstream.bodies[0], "input.1.encrypted_content").String())
	require.True(t, IsResponseCommitted(c))
	require.Equal(t, http.StatusUnprocessableEntity, recorder.Code)
	require.Equal(t, "invalid_request_error", gjson.Get(recorder.Body.String(), "error.type").String())
	require.Equal(t, "input", gjson.Get(recorder.Body.String(), "error.param").String())
	require.Equal(t, gjson.Get(grokUnknownInputTypeTestBody, "error").String(), gjson.Get(recorder.Body.String(), "error.message").String())
	require.Zero(t, repo.tempUnschedCalls)
	require.Zero(t, repo.rateLimitedCalls)
	require.Zero(t, repo.updateCalls)
	require.Equal(t, StatusActive, account.Status)
	require.False(t, isGrokModelQuotaBlocked(account.ID, "grok-4.6", time.Now()))
	require.Equal(t, http.StatusUnprocessableEntity, c.GetInt(OpsUpstreamStatusCodeKey))
	rawEvents, exists := c.Get(OpsUpstreamErrorsKey)
	require.True(t, exists)
	events, ok := rawEvents.([]*OpsUpstreamErrorEvent)
	require.True(t, ok)
	require.Len(t, events, 1)
	require.Equal(t, "http_error", events[0].Kind)
	require.Empty(t, events[0].Detail)
	require.Contains(t, events[0].Message, `unknown item type "agent_message"`)
}

func TestForwardGrokResponsesRequestErrorsAfterHeartbeatRemainFailedSSE(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tt := range []struct {
		name          string
		requestBody   string
		status        int
		upstreamCalls int
	}{
		{"upstream unknown input", `{"model":"grok-4.6","input":"hi","stream":true}`, http.StatusUnprocessableEntity, 1},
		{"local input validation", `{"model":"grok-4.6","stream":true,"tools":[{"type":"tool_search"}],"input":[{"type":"tool_search_output","status":"completed"}]}`, http.StatusBadRequest, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(tt.requestBody))
			c.Header("Content-Type", "text/event-stream")
			_, err := c.Writer.WriteString(": ping\n\n")
			require.NoError(t, err)
			c.Writer.Flush()
			upstream := &httpUpstreamRecorder{responses: []*http.Response{{
				StatusCode: http.StatusUnprocessableEntity,
				Header:     http.Header{},
				Body:       io.NopCloser(strings.NewReader(grokUnknownInputTypeTestBody)),
			}}}
			repo := &grokQuotaAccountRepo{}
			svc := &OpenAIGatewayService{httpUpstream: upstream, accountRepo: repo}
			account := grokProtocolAPIKeyAccount(227902)
			result, err := svc.forwardGrokResponses(context.Background(), c, account, []byte(tt.requestBody), "grok-4.6", true, time.Now())
			require.Nil(t, result)
			require.Error(t, err)
			var failoverErr *UpstreamFailoverError
			require.False(t, errors.As(err, &failoverErr))
			require.Len(t, upstream.requests, tt.upstreamCalls)
			require.True(t, IsResponseCommitted(c))
			require.Zero(t, repo.tempUnschedCalls)
			require.Zero(t, repo.rateLimitedCalls)
			require.Equal(t, http.StatusOK, recorder.Code, "the heartbeat has already committed the wire status")
			require.Equal(t, "text/event-stream", recorder.Header().Get("Content-Type"))
			frames := strings.Split(strings.TrimSuffix(recorder.Body.String(), "\n\n"), "\n\n")
			require.Len(t, frames, 2)
			require.Equal(t, ": ping", frames[0])
			const prefix = "event: response.failed\ndata: "
			require.True(t, strings.HasPrefix(frames[1], prefix))
			payload := strings.TrimPrefix(frames[1], prefix)
			require.True(t, gjson.Valid(payload))
			require.Equal(t, "failed", gjson.Get(payload, "response.status").String())
			require.Equal(t, "invalid_request_error", gjson.Get(payload, "response.error.code").String())
			require.NotEmpty(t, gjson.Get(payload, "response.error.message").String())
			require.NotContains(t, recorder.Body.String(), "response.completed")
			streamErr, exists := GetOpsStreamError(c)
			require.True(t, exists, "HTTP 200 must still be observed as a failed request")
			require.Equal(t, tt.status, streamErr.IntendedStatus)
			require.Equal(t, "invalid_request_error", streamErr.ErrType)
		})
	}
}
