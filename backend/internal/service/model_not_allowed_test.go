package service

import (
	"context"
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

func TestModelNotAllowedForwardStopsWithoutLosingUsage(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		for _, metered := range []bool{false, true} {
			for _, committed := range []bool{false, true} {
				t.Run(fmt.Sprintf("passthrough=%t/metered=%t/committed=%t", passthrough, metered, committed), func(t *testing.T) {
					rec := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(rec)
					c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
					if committed {
						c.Header("Content-Type", "text/event-stream")
						_, _ = c.Writer.WriteString(": ping\n\n")
						c.Writer.Flush()
					}
					body := `{"type":"error","error":{"type":"upstream_error","message":"This model is prohibited from being used.","request_id":"private-provider-id","help":"https://private.example/?token=secret"}`
					if metered {
						body += `,"usage":{"input_tokens":12,"output_tokens":7,"cache_read_input_tokens":5,"cache_creation":{"ephemeral_5m_input_tokens":2,"ephemeral_1h_input_tokens":3}}`
					}
					body += "}"
					up := &queuedHTTPUpstreamStub{responses: []*http.Response{{StatusCode: 429, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}}}
					s := newForwardPartialUsageServiceForTest(nil)
					s.httpUpstream = up
					s.cfg.Gateway.LogUpstreamErrorBody = true
					s.cfg.Gateway.LogUpstreamErrorBodyMaxBytes = 2048
					account := newAnthropicAPIKeyAccountForTest()
					account.Credentials["pool_mode"] = true
					account.Extra["anthropic_passthrough"] = passthrough
					parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(fmt.Sprintf(`{"model":"claude-sonnet-5-5","stream":%t,"messages":[{"role":"user","content":"synthetic"}]}`, committed))), PlatformAnthropic)
					require.NoError(t, err)
					result, err := s.Forward(context.Background(), c, account, parsed)
					require.Error(t, err)
					var failover *UpstreamFailoverError
					require.False(t, errors.As(err, &failover), "explicit model prohibition must not select another account")
					require.Equal(t, 1, up.callCount)
					require.True(t, account.Schedulable)
					require.Nil(t, account.RateLimitResetAt)
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
					want := 403
					if committed {
						want = 200
						require.Equal(t, 1, strings.Count(rec.Body.String(), "event: error"))
					}
					require.Equal(t, want, rec.Code)
					require.Contains(t, rec.Body.String(), `"code":"model_not_allowed"`)
					require.Contains(t, rec.Body.String(), `"type":"permission_error"`)
					require.Contains(t, rec.Body.String(), `"status_code":403`)
					require.Contains(t, rec.Body.String(), "This model is prohibited from being used.")
					require.NotContains(t, rec.Body.String(), "private-provider-id")
					require.NotContains(t, rec.Body.String(), "private.example")
					require.NotContains(t, rec.Body.String(), "token=secret")
					raw, ok := c.Get(OpsUpstreamErrorsKey)
					require.True(t, ok)
					events := raw.([]*OpsUpstreamErrorEvent)
					require.Len(t, events, 1)
					require.Equal(t, "request_rejection", events[0].Kind)
					require.Equal(t, 429, events[0].UpstreamStatusCode)
					require.Equal(t, "private-provider-id", events[0].UpstreamRequestID)
				})
			}
		}
	}
}

func TestModelNotAllowedRequiresUnambiguous429Contract(t *testing.T) {
	for _, body := range []string{
		`{"error":{"message":"This model is prohibited from being used."}}`,
		`{"error":{"code":"model_not_allowed"}}`,
		`{"error":{"code":"","message":" THIS MODEL IS PROHIBITED FROM BEING USED. "}}`,
		`{"status_code":429,"error":{"code":"model_not_allowed","status_code":429}}`,
	} {
		require.True(t, isModelNotAllowedError(429, []byte(body)), body)
		for _, status := range []int{200, 400, 401, 403, 500, 503} {
			require.False(t, isModelNotAllowedError(status, []byte(body)))
		}
	}
	for _, body := range []string{
		`{"error":{"message":"Rate limit exceeded"}}`,
		`{"error":{"message":"This model is prohibited from being used. Try again later."}}`,
		`{"message":"This model is prohibited from being used."}`,
		`{"request":{"message":"This model is prohibited from being used."}}`,
		`{"error":{"code":"insufficient_quota","message":"This model is prohibited from being used."}}`,
		`{"error":{"code":null,"message":"This model is prohibited from being used."}}`,
		`{"error":{"code":429,"message":"This model is prohibited from being used."}}`,
		`{"error":{"code":"model_not_allowed","code":"rate_limit"}}`,
		`{"error":{"code":"model_not_allowed","CODE":"rate_limit"}}`,
		`{"error":{"message":"This model is prohibited from being used.","message":"capacity"}}`,
		`{"error":{"code":"model_not_allowed"},"error":{"code":"rate_limit"}}`,
		`{"error":{"code":"model_not_allowed","status_code":503}}`,
		`{"status_code":503,"error":{"code":"model_not_allowed"}}`,
		`{"status_code":"429","error":{"code":"model_not_allowed"}}`,
		`{"status_code":null,"error":{"code":"model_not_allowed"}}`,
		`{"error":{"code":"model_not_allowed","status_code":429,"status_code":429}}`,
		`{"error":{"code":"model_not_allowed"}} trailing`,
		`{"error":{"code":"model_not_allowed"}`,
		`[{"error":{"code":"model_not_allowed"}}]`,
		`{"error":{"code":"model_not_allowed"},"padding":"` + strings.Repeat("x", modelNotAllowedBodyLimit) + `"}`,
	} {
		require.False(t, isModelNotAllowedError(429, []byte(body)), body[:min(len(body), 200)])
	}
}

type modelPermissionReadCloser struct {
	io.Reader
	closed bool
}

func (r *modelPermissionReadCloser) Close() error { r.closed = true; return nil }

func TestModelNotAllowedUnmatchedBodyPreservesBytesErrorsAndClose(t *testing.T) {
	for _, readErr := range []error{nil, io.ErrUnexpectedEOF} {
		for _, body := range []string{`{"error":{"message":"capacity busy"}}`, strings.Repeat("x", modelNotAllowedBodyLimit+33)} {
			reader := io.Reader(strings.NewReader(body))
			if readErr != nil {
				reader = io.MultiReader(strings.NewReader(body), &streamReadCloser{sent: true, err: readErr})
			}
			source := &modelPermissionReadCloser{Reader: reader}
			resp := &http.Response{StatusCode: 429, Body: source}
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			s := &GatewayService{}
			result, err, handled := s.handleModelNotAllowedResponse(c, newAnthropicAPIKeyAccountForTest(), resp, "m", "m", false, time.Now())
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

func TestModelNotAllowedCompatPreservesMeteredError(t *testing.T) {
	for _, path := range []string{"/v1/chat/completions", "/v1/responses"} {
		t.Run(path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest("POST", path, nil)
			body := `{"error":{"code":"model_not_allowed","message":"private https://upstream.invalid?key=secret"},"usage":{"input_tokens":12,"output_tokens":7,"cache_read_input_tokens":5,"cache_creation":{"ephemeral_5m_input_tokens":2,"ephemeral_1h_input_tokens":3}}}`
			up := &queuedHTTPUpstreamStub{responses: []*http.Response{{StatusCode: 429, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}}}
			s := newForwardPartialUsageServiceForTest(nil)
			s.httpUpstream = up
			account := newAnthropicAPIKeyAccountForTest()
			request := []byte(`{"model":"claude-sonnet-5-5","messages":[{"role":"user","content":"synthetic"}]}`)
			var result *ForwardResult
			var err error
			if path == "/v1/responses" {
				request = []byte(`{"model":"claude-sonnet-5-5","input":"synthetic"}`)
				result, err = s.ForwardAsResponses(context.Background(), c, account, request, nil)
			} else {
				result, err = s.ForwardAsChatCompletions(context.Background(), c, account, request, nil)
			}
			require.Error(t, err)
			var failover *UpstreamFailoverError
			require.False(t, errors.As(err, &failover))
			require.Equal(t, 1, up.callCount)
			require.NotNil(t, result)
			require.Equal(t, 12, result.Usage.InputTokens)
			require.Equal(t, 7, result.Usage.OutputTokens)
			require.Equal(t, 5, result.Usage.CacheReadInputTokens)
			require.Equal(t, 2, result.Usage.CacheCreation5mTokens)
			require.Equal(t, 3, result.Usage.CacheCreation1hTokens)
			require.Equal(t, 403, rec.Code)
			require.Contains(t, rec.Body.String(), `"code":"model_not_allowed"`)
			require.NotContains(t, rec.Body.String(), "upstream.invalid")
			require.NotContains(t, rec.Body.String(), "secret")
		})
	}
}

func TestOrdinary429KeepsExistingPoolFailover(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		t.Run(fmt.Sprint(passthrough), func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
			up := &queuedHTTPUpstreamStub{responses: []*http.Response{newOpenAIUpstreamErrorResponse(429, `{"error":{"message":"capacity busy"}}`)}}
			s := newForwardPartialUsageServiceForTest(nil)
			s.httpUpstream = up
			account := newAnthropicAPIKeyAccountForTest()
			account.Credentials["pool_mode"] = true
			account.Extra["anthropic_passthrough"] = passthrough
			parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(`{"model":"claude-sonnet-5-5","messages":[{"role":"user","content":"synthetic"}]}`)), PlatformAnthropic)
			require.NoError(t, err)
			result, err := s.Forward(context.Background(), c, account, parsed)
			var failover *UpstreamFailoverError
			require.Nil(t, result)
			require.ErrorAs(t, err, &failover)
			require.True(t, failover.ShouldRetryNextAccount())
			require.True(t, failover.RetryableOnSameAccount)
			require.False(t, c.Writer.Written())
			require.Equal(t, 1, up.callCount)
		})
	}
}

func TestModelNotAllowedAfterSignatureRectificationIsTerminal(t *testing.T) {
	up := &queuedHTTPUpstreamStub{responses: []*http.Response{
		newOpenAIUpstreamErrorResponse(400, `{"error":{"message":"synthetic signature"}}`),
		newOpenAIUpstreamErrorResponse(429, `{"error":{"message":"This model is prohibited from being used."}}`),
	}}
	svc := newForwardPartialUsageServiceForTest(nil)
	svc.httpUpstream = up
	svc.settingService = &SettingService{settingRepo: &consumedSettingsRepo{value: `{"enabled":true,"apikey_signature_enabled":true,"apikey_signature_patterns":["synthetic signature"]}`}}
	account := newAnthropicAPIKeyAccountForTest()
	account.Extra["anthropic_passthrough"] = false
	account.Credentials["custom_error_codes_enabled"] = true
	account.Credentials["custom_error_codes"] = []any{float64(429)}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
	parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(`{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"synthetic"}]}`)), PlatformAnthropic)
	require.NoError(t, err)
	result, err := svc.Forward(context.Background(), c, account, parsed)
	require.Error(t, err)
	require.Nil(t, result)
	var failover *UpstreamFailoverError
	require.NotErrorAs(t, err, &failover)
	require.Equal(t, 2, up.callCount)
	require.Equal(t, 403, rec.Code)
	require.Contains(t, rec.Body.String(), `"code":"model_not_allowed"`)
}
