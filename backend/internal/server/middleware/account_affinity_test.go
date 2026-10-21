package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type stubAffinitySettings struct {
	settings service.AccountAffinitySettings
	calls    int
}

func (s *stubAffinitySettings) GetAccountAffinitySettingsCached(context.Context) service.AccountAffinitySettings {
	s.calls++
	return s.settings
}

func runAccountAffinity(t *testing.T, settings AccountAffinitySettingsReader, apiKey *service.APIKey, header string) ([]string, int) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		if apiKey != nil {
			c.Set(string(ContextKeyAPIKey), apiKey)
		}
		c.Next()
	})
	router.Use(AccountAffinity(settings))
	var seen []string
	router.POST("/v1/messages", func(c *gin.Context) {
		seen = service.AccountAffinityTagsFromContext(c.Request.Context())
		c.Status(http.StatusOK)
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	if header != "" {
		req.Header.Set(service.AccountAffinityHeader, header)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return seen, w.Code
}

func TestAccountAffinity_TrustedKeyWithHeaderSetsTag(t *testing.T) {
	stub := &stubAffinitySettings{settings: service.AccountAffinitySettings{Enabled: true, TrustedAPIKeyIDs: []int64{214}, TagPrefix: "getoken:"}}
	tags, code := runAccountAffinity(t, stub, &service.APIKey{ID: 214}, "123")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, []string{"getoken:123"}, tags)
	require.Equal(t, 1, stub.calls)
}

func TestAccountAffinity_NoHeaderIsZeroCost(t *testing.T) {
	stub := &stubAffinitySettings{settings: service.AccountAffinitySettings{Enabled: true, TrustedAPIKeyIDs: []int64{214}}}
	tags, code := runAccountAffinity(t, stub, &service.APIKey{ID: 214}, "")
	require.Equal(t, http.StatusOK, code)
	require.Nil(t, tags)
	require.Zero(t, stub.calls, "无请求头时不应读取配置")
}

func TestAccountAffinity_UntrustedKeyHeaderIgnored(t *testing.T) {
	stub := &stubAffinitySettings{settings: service.AccountAffinitySettings{Enabled: true, TrustedAPIKeyIDs: []int64{214}}}
	tags, code := runAccountAffinity(t, stub, &service.APIKey{ID: 999}, "123")
	require.Equal(t, http.StatusOK, code, "永不拒绝请求")
	require.Nil(t, tags)
}

func TestAccountAffinity_DisabledIgnoresEveryone(t *testing.T) {
	stub := &stubAffinitySettings{settings: service.AccountAffinitySettings{Enabled: false, TrustedAPIKeyIDs: []int64{214}}}
	tags, _ := runAccountAffinity(t, stub, &service.APIKey{ID: 214}, "123")
	require.Nil(t, tags)
}

func TestAccountAffinity_InvalidUserIDIgnored(t *testing.T) {
	stub := &stubAffinitySettings{settings: service.AccountAffinitySettings{Enabled: true, TrustedAPIKeyIDs: []int64{214}}}
	for _, raw := range []string{"abc", "12a", "-1", "1;drop", "123456789012345678901"} {
		tags, code := runAccountAffinity(t, stub, &service.APIKey{ID: 214}, raw)
		require.Equal(t, http.StatusOK, code)
		require.Nil(t, tags, "raw=%q", raw)
	}
}

func TestAccountAffinity_NoAPIKeyInContextIgnored(t *testing.T) {
	stub := &stubAffinitySettings{settings: service.AccountAffinitySettings{Enabled: true, TrustedAPIKeyIDs: []int64{214}}}
	tags, code := runAccountAffinity(t, stub, nil, "123")
	require.Equal(t, http.StatusOK, code)
	require.Nil(t, tags)
	require.Zero(t, stub.calls)
}

func TestAccountAffinity_NilSettingServicePointerIsSafe(t *testing.T) {
	var nilSvc *service.SettingService
	tags, code := runAccountAffinity(t, nilSvc, &service.APIKey{ID: 214}, "123")
	require.Equal(t, http.StatusOK, code)
	require.Nil(t, tags)
}
