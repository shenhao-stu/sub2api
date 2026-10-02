//go:build unit

package service

import (
	"bytes"
	"context"
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

func TestExtractGrokUpstreamErrorMessage(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		body string
		want string
	}{
		{"nested message", `{"error":{"message":"could not decode ModelInput"}}`, "could not decode ModelInput"},
		{"nested error", `{"error":{"error":"could not decode ModelInput"}}`, "could not decode ModelInput"},
		{"string error", `{"error":"input[2]: data did not match any variant of untagged enum ModelInput"}`, "input[2]: data did not match any variant of untagged enum ModelInput"},
		{"top level message", `{"message":"input.3 is invalid"}`, "input.3 is invalid"},
		{"detail", `{"detail":"input.3 is invalid"}`, "input.3 is invalid"},
		{"plaintext", "  could not deserialize ModelInput  ", "could not deserialize ModelInput"},
		{"precedence", `{"error":{"message":"first"},"message":"second"}`, "first"},
		{"unknown shape", `{"code":"invalid-argument"}`, ""},
		{"empty", "", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, extractGrokUpstreamErrorMessage([]byte(tt.body)))
		})
	}
}

func TestGrokUpstreamDiagnosticsRedactsCredentialsBeforeTruncation(t *testing.T) {
	t.Parallel()
	body := []byte(`{"error":"could not decode ModelInput; Bearer bearer-secret xai-secret access_token=access-secret api_key=key-secret https://api.x.ai?refresh_token=query-secret"}`)
	message := extractGrokUpstreamErrorMessage(body)
	require.Contains(t, message, "could not decode ModelInput")
	for _, secret := range []string{"bearer-secret", "xai-secret", "access-secret", "key-secret", "query-secret"} {
		require.NotContains(t, message, secret)
	}
	require.LessOrEqual(t, len(extractGrokUpstreamErrorMessage([]byte(strings.Repeat("x", 4096)))), 2048)
}

func TestGrokUpstreamErrorDetailHonorsLoggingConfiguration(t *testing.T) {
	t.Parallel()
	body := []byte(strings.Repeat("diagnostic ", 1024) + "access_token=secret")
	var absent *OpenAIGatewayService
	require.Empty(t, absent.grokUpstreamErrorDetail(body))
	require.Empty(t, (&OpenAIGatewayService{}).grokUpstreamErrorDetail(body))
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{LogUpstreamErrorBody: true}}}
	detail := svc.grokUpstreamErrorDetail(body)
	require.NotEmpty(t, detail)
	require.LessOrEqual(t, len(detail), 2048)
	require.NotContains(t, detail, "secret")
}

func TestGrokTerminalClientErrorRetainsSanitizedDiagnostic(t *testing.T) {
	for _, logBody := range []bool{false, true} {
		repo := &grokQuotaAccountRepo{}
		svc := &OpenAIGatewayService{accountRepo: repo, cfg: &config.Config{
			Gateway: config.GatewayConfig{LogUpstreamErrorBody: logBody},
		}}
		account := &Account{ID: 227, Platform: PlatformGrok, Type: AccountTypeOAuth}
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		response := &http.Response{
			StatusCode: http.StatusBadRequest,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"error":"invalid tool definition; Bearer bearer-secret", "access_token":"body-secret"}`)),
		}
		_, err := svc.handleErrorResponse(context.Background(), response, c, account, nil, "grok-4.7-fast")
		require.Error(t, err)
		require.Equal(t, http.StatusBadRequest, recorder.Code)
		require.Contains(t, c.GetString(OpsUpstreamErrorMessageKey), "invalid tool definition")
		require.NotContains(t, c.GetString(OpsUpstreamErrorMessageKey), "bearer-secret")
		require.NotContains(t, err.Error(), "bearer-secret")
		detail := c.GetString(OpsUpstreamErrorDetailKey)
		require.NotContains(t, detail, "body-secret")
		require.NotContains(t, detail, "bearer-secret")
		if logBody {
			require.Contains(t, detail, "invalid tool definition")
		} else {
			require.Empty(t, detail)
		}
		require.Zero(t, repo.tempUnschedCalls)
		require.Zero(t, repo.rateLimitedCalls)
		require.Zero(t, repo.updateCalls)
	}
}

func TestForwardGrokResponsesRetainsDiagnosticWithoutChangingFailover(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, logBody := range []bool{false, true} {
		name := "body disabled"
		if logBody {
			name = "body enabled"
		}
		t.Run(name, func(t *testing.T) {
			requestBody := []byte(`{"model":"grok-4.6","input":"hi","stream":false}`)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(requestBody))
			responseBody := `{"error":"input[2]: could not decode ModelInput; Bearer bearer-secret", "access_token":"body-secret"}`
			upstream := &httpUpstreamRecorder{responses: []*http.Response{{
				StatusCode: http.StatusUnprocessableEntity,
				Header:     http.Header{"Xai-Request-Id": []string{"diag-request-id"}},
				Body:       io.NopCloser(strings.NewReader(responseBody)),
			}}}
			svc := &OpenAIGatewayService{
				httpUpstream: upstream,
				cfg: &config.Config{Gateway: config.GatewayConfig{
					LogUpstreamErrorBody: logBody, LogUpstreamErrorBodyMaxBytes: 256,
				}},
			}
			account := &Account{
				ID: 227, Platform: PlatformGrok, Type: AccountTypeAPIKey, Concurrency: 1,
				Credentials: map[string]any{"api_key": "test-only-token", "base_url": "https://api.x.ai/v1"},
			}
			result, err := svc.forwardGrokResponses(context.Background(), c, account, requestBody, "grok-4.6", false, time.Now())
			require.Nil(t, result)
			var failoverErr *UpstreamFailoverError
			require.ErrorAs(t, err, &failoverErr)
			require.Equal(t, http.StatusUnprocessableEntity, failoverErr.StatusCode)
			require.Equal(t, responseBody, string(failoverErr.ResponseBody))
			require.True(t, failoverErr.ShouldRetryNextAccount())
			require.False(t, recorder.Flushed)
			require.False(t, c.Writer.Written())
			require.Len(t, upstream.requests, 1)
			require.Contains(t, c.GetString(OpsUpstreamErrorMessageKey), "could not decode ModelInput")
			require.NotContains(t, c.GetString(OpsUpstreamErrorMessageKey), "bearer-secret")
			// The terminal handler's generic extractor must not erase the Grok diagnostic.
			setOpsUpstreamError(c, failoverErr.StatusCode, extractUpstreamErrorMessage(failoverErr.ResponseBody), "")
			require.Contains(t, c.GetString(OpsUpstreamErrorMessageKey), "could not decode ModelInput")
			rawEvents, exists := c.Get(OpsUpstreamErrorsKey)
			require.True(t, exists)
			events, ok := rawEvents.([]*OpsUpstreamErrorEvent)
			require.True(t, ok)
			require.Len(t, events, 1)
			require.Equal(t, "diag-request-id", events[0].UpstreamRequestID)
			require.Equal(t, "failover", events[0].Kind)
			require.Equal(t, c.GetString(OpsUpstreamErrorMessageKey), events[0].Message)
			require.Equal(t, events[0].Detail, events[0].UpstreamResponseBody)
			if logBody {
				require.Contains(t, events[0].Detail, "could not decode ModelInput")
				require.NotContains(t, events[0].Detail, "body-secret")
				require.NotContains(t, events[0].Detail, "bearer-secret")
				require.LessOrEqual(t, len(events[0].Detail), 256)
			} else {
				require.Empty(t, events[0].Detail)
				require.Empty(t, c.GetString(OpsUpstreamErrorDetailKey))
			}
		})
	}
}
