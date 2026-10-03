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
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

const grokImageModerationBody = `{"code":"imagine:content-moderated","error":"Generated image rejected by content moderation.","usage":{"cost_in_usd_ticks":900000000}}`

func TestIsGrokImageContentPolicyRejection(t *testing.T) {
	for _, tt := range []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{"confirmed top level code", http.StatusBadRequest, grokImageModerationBody, true},
		{"wrong status", http.StatusForbidden, grokImageModerationBody, false},
		{"payment status", http.StatusPaymentRequired, grokImageModerationBody, false},
		{"ordinary input error", http.StatusBadRequest, `{"error":"invalid image format"}`, false},
		{"phrase alone", http.StatusBadRequest, `{"error":"Generated image rejected by content moderation."}`, false},
		{"permission denied", http.StatusBadRequest, `{"code":"permission-denied","error":"Access denied"}`, false},
		{"nested quoted code", http.StatusBadRequest, `{"input":{"code":"imagine:content-moderated"}}`, false},
		{"similar code", http.StatusBadRequest, `{"code":"imagine:content-moderated-other"}`, false},
		{"account message takes precedence", http.StatusBadRequest, `{"code":"imagine:content-moderated","error":"subscription required"}`, false},
		{"account marker takes precedence", http.StatusBadRequest, `{"code":"imagine:content-moderated","error":{"type":"account_suspended"}}`, false},
		{"malformed JSON", http.StatusBadRequest, `{"code":"imagine:content-moderated"`, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isGrokImageContentPolicyRejection(tt.status, []byte(tt.body)))
		})
	}
}

func TestGrokImageModerationPreservesFailureWithoutInventingUsage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, path := range []string{"/v1/images/generations", "/v1/images/edits"} {
		for _, heartbeat := range []bool{false, true} {
			name := path + "/uncommitted"
			if heartbeat {
				name = path + "/json-heartbeat"
			}
			t.Run(name, func(t *testing.T) {
				repo := &grokQuotaAccountRepo{}
				svc := &OpenAIGatewayService{accountRepo: repo}
				account := &Account{ID: 28001, Platform: PlatformGrok, Type: AccountTypeOAuth,
					Credentials: map[string]any{
						"custom_error_codes_enabled": true,
						"custom_error_codes":         []any{float64(http.StatusTooManyRequests)},
					}}
				recorder := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(recorder)
				c.Request = httptest.NewRequest(http.MethodPost, path, nil)
				if heartbeat {
					stop := StartOpenAIJSONKeepalive(c, time.Hour)
					defer stop()
					require.True(t, openAIImagesJSONKeepaliveFromContext(c).beat())
				}
				resp := &http.Response{StatusCode: http.StatusBadRequest,
					Header: http.Header{"Content-Type": []string{"application/json"}},
					Body:   io.NopCloser(strings.NewReader(grokImageModerationBody))}

				result, err := svc.handleGrokMediaErrorResponse(context.Background(), resp, c, account, "offline-request", "grok-imagine-image-2.0")

				require.Error(t, err)
				require.Nil(t, result, "unverified cost ticks must not create billable usage")
				var failover *UpstreamFailoverError
				require.False(t, errors.As(err, &failover))
				require.Zero(t, repo.tempUnschedCalls)
				require.Zero(t, repo.rateLimitedCalls)
				require.Zero(t, repo.updateCalls)
				require.False(t, svc.isOpenAIAccountRuntimeBlocked(account))
				wantStatus := http.StatusBadRequest
				if heartbeat {
					wantStatus = http.StatusOK
				}
				require.Equal(t, wantStatus, recorder.Code)
				var payload map[string]any
				require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &payload), "heartbeat must end with one complete JSON error")
				require.Equal(t, map[string]any{"error": map[string]any{
					"type": "invalid_request_error", "code": "content_policy_violation",
					"message": "Generated image rejected by upstream content policy",
				}}, payload)
				require.Equal(t, "Generated image rejected by content moderation.", c.GetString(OpsUpstreamErrorMessageKey))
			})
		}
	}
}

func TestGrokMediaOrdinary400RetainsSanitizedDiagnostic(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &OpenAIGatewayService{accountRepo: &grokQuotaAccountRepo{}, cfg: &config.Config{
		Gateway: config.GatewayConfig{LogUpstreamErrorBody: true},
	}}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/edits", nil)
	resp := &http.Response{StatusCode: http.StatusBadRequest, Header: make(http.Header),
		Body: io.NopCloser(strings.NewReader(`{"error":"invalid image format; Bearer offline-secret","access_token":"body-secret"}`))}
	account := &Account{ID: 28002, Platform: PlatformGrok, Type: AccountTypeOAuth}
	result, err := svc.handleGrokMediaErrorResponse(context.Background(), resp, c, account, "", "grok-imagine-image-2.0")
	require.Nil(t, result)
	require.Error(t, err)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), "invalid image format")
	require.NotContains(t, recorder.Body.String(), "content_policy_violation")
	for _, value := range []string{recorder.Body.String(), err.Error(), c.GetString(OpsUpstreamErrorMessageKey), c.GetString(OpsUpstreamErrorDetailKey)} {
		require.NotContains(t, value, "offline-secret")
		require.NotContains(t, value, "body-secret")
	}
}
