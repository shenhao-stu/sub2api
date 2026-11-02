//go:build unit

package admin

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/xai"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type classifiedSSOImportClient struct {
	grokImportOAuthClientStub
	mu    sync.Mutex
	calls []string
}

func TestGrokSSOBatchExtractsJWTRecordWithoutSendingLabelOrPassword(t *testing.T) {
	gin.SetMode(gin.TestMode)
	jwt := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256"}`)) + "." + base64.RawURLEncoding.EncodeToString([]byte(`{"session_id":"synthetic-session"}`)) + "." + base64.RawURLEncoding.EncodeToString([]byte("signature"))
	client := &classifiedSSOImportClient{}
	svc := service.NewGrokOAuthService(nil, client)
	defer svc.Stop()
	h := NewGrokOAuthHandler(svc, newGrokImportAdminService(), nil, nil)
	r := gin.New()
	r.POST("/import", h.CreateAccountsFromSSO)
	payload, _ := json.Marshal(map[string]any{"sso_token": "1. untrusted-label@example.com---secret!,private-middle,with---delimiters---" + jwt + "\n2. label@example.com---pw; sso=secret,private-middle---" + jwt + "\nCookie: sso=cookie-token; other=ignored\n" + jwt, "sso_tokens": []string{"raw-one,raw-two"}})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/import", strings.NewReader(string(payload)))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.ElementsMatch(t, []string{jwt, "cookie-token", "raw-one", "raw-two"}, client.calls)
	for _, secret := range []string{"untrusted-label@example.com", "secret!", "private-middle", "with---delimiters"} {
		require.NotContains(t, w.Body.String(), secret)
	}
	var response struct {
		Data GrokSSOToOAuthResponse `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	require.Equal(t, 4, response.Data.Total)
	require.Equal(t, 4, response.Data.CreatedCount)
}

func (c *classifiedSSOImportClient) ConvertSSOToBuild(_ context.Context, token, _ string) (*xai.TokenResponse, error) {
	c.mu.Lock()
	c.calls = append(c.calls, token)
	c.mu.Unlock()
	switch token {
	case "browser-sso":
		return nil, infraerrors.New(http.StatusForbidden, "GROK_SSO_BROWSER_VERIFICATION_REQUIRED", "Complete Device OAuth authorization in a browser.")
	case "expired-sso":
		return nil, infraerrors.New(http.StatusUnauthorized, "GROK_SSO_UNAUTHORIZED", "Grok Web SSO cookie is invalid or expired")
	}
	return &xai.TokenResponse{AccessToken: "synthetic-access", RefreshToken: "synthetic-refresh", ExpiresIn: 3600}, nil
}

func TestGrokSSOBatchReportsBrowserRequirementWithoutRetryOrLosingSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	client := &classifiedSSOImportClient{}
	svc := service.NewGrokOAuthService(nil, client)
	defer svc.Stop()
	h := NewGrokOAuthHandler(svc, newGrokImportAdminService(), nil, nil)
	r := gin.New()
	r.POST("/import", h.CreateAccountsFromSSO)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/import", strings.NewReader(`{"sso_tokens":["browser-sso","sso=browser-sso","expired-sso","good-sso"]}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var response struct {
		Data GrokSSOToOAuthResponse `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	result := response.Data
	require.Equal(t, 3, result.Total)
	require.Equal(t, 1, result.CreatedCount)
	require.Equal(t, 2, result.FailedCount)
	require.Equal(t, 1, result.NeedsBrowserAuthorizationCount)
	require.Len(t, result.Created, 1)
	require.Len(t, result.Failed, 2)
	require.Equal(t, 1, result.Failed[0].Index)
	require.Equal(t, "GROK_SSO_BROWSER_VERIFICATION_REQUIRED", result.Failed[0].Code)
	require.True(t, result.Failed[0].NeedsBrowserAuthorization)
	require.Equal(t, "GROK_SSO_UNAUTHORIZED", result.Failed[1].Code)
	require.False(t, result.Failed[1].NeedsBrowserAuthorization)
	require.Len(t, client.calls, 3)
	for _, secret := range []string{"browser-sso", "expired-sso", "good-sso"} {
		require.NotContains(t, w.Body.String(), secret)
	}
}

func TestGrokSSOBatchRejectsAmbiguousRecordsBeforeCallingConverter(t *testing.T) {
	gin.SetMode(gin.TestMode)
	segment := func(value string) string { return base64.RawURLEncoding.EncodeToString([]byte(value)) }
	jwt := segment(`{"alg":"HS256"}`) + "." + segment(`{"session_id":"synthetic"}`) + "." + segment("signature")
	nullHeader := segment(`null`) + "." + segment(`{"session_id":"synthetic"}`) + "." + segment("signature")
	nullPayload := segment(`{"alg":"HS256"}`) + "." + segment(`null`) + "." + segment("signature")
	prefix := "2. label@example.com---"
	for _, tc := range []struct{ name, record string }{
		{"suffix garbage", prefix + "private-password---" + jwt + " !garbage"},
		{"password JWT is not credential", prefix + "private," + jwt + ",middle---not-a-jwt"},
		{"null header", prefix + "password---" + nullHeader},
		{"null payload", prefix + "password---" + nullPayload},
		{"oversize credential", prefix + "password---" + jwt + strings.Repeat("A", 16<<10)},
		{"missing email", "not-an-email---password---" + jwt},
		{"password cookie text", prefix + "private,one;sso=not-a-credential,private-middle---not-a-jwt"},
		{"malformed label cookie text", "not-an-email---password;sso=not-a-credential,private-middle---" + jwt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &classifiedSSOImportClient{}
			svc := service.NewGrokOAuthService(nil, client)
			defer svc.Stop()
			h := NewGrokOAuthHandler(svc, newGrokImportAdminService(), nil, nil)
			r := gin.New()
			r.POST("/import", h.CreateAccountsFromSSO)
			payload, _ := json.Marshal(map[string]any{"sso_tokens": []string{tc.record, "safe-control"}})
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/import", strings.NewReader(string(payload)))
			req.Header.Set("Content-Type", "application/json")
			r.ServeHTTP(w, req)
			require.Equal(t, http.StatusOK, w.Code)
			require.Equal(t, []string{"safe-control"}, client.calls)
		})
	}
}

func TestGrokSSOBatchPreservesExplicitCookieTokenWithRecordDelimiters(t *testing.T) {
	gin.SetMode(gin.TestMode)
	client := &classifiedSSOImportClient{}
	svc := service.NewGrokOAuthService(nil, client)
	defer svc.Stop()
	h := NewGrokOAuthHandler(svc, newGrokImportAdminService(), nil, nil)
	r := gin.New()
	r.POST("/import", h.CreateAccountsFromSSO)
	payload, _ := json.Marshal(map[string]any{"sso_token": "Cookie: foo=bar; sso=opaque---cookie; other=ignored\r\nsso-rw=opaque---other\npure-one,pure-two"})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/import", strings.NewReader(string(payload)))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.ElementsMatch(t, []string{"opaque---cookie", "opaque---other", "pure-one", "pure-two"}, client.calls)
}
