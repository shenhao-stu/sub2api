//go:build unit

package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/adminaccess"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type quotaCooldownHandlerRepo struct {
	service.AccountRepository
	account *service.Account
	writes  int
}

func (r *quotaCooldownHandlerRepo) GetByID(context.Context, int64) (*service.Account, error) {
	copy := *r.account
	return &copy, nil
}
func (r *quotaCooldownHandlerRepo) SetRateLimitedIfLater(_ context.Context, _ int64, until time.Time) error {
	r.writes++
	r.account.RateLimitResetAt = &until
	return nil
}

func quotaCooldownTestHandler() (*GrokOAuthHandler, *quotaCooldownHandlerRepo) {
	r := &quotaCooldownHandlerRepo{account: &service.Account{ID: 900001, Platform: service.PlatformGrok, Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: false}}
	s := service.NewGrokQuotaService(r, nil, nil, nil, nil)
	return NewGrokOAuthHandler(nil, nil, s, nil), r
}

func TestGrokQuotaCooldownHandlerRejectsUntypedAndArbitraryBoundaryInputs(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, body := range []string{`null`, `[]`, `{"until":"2099-01-01T00:00:00Z"}`, `{"observed_error_code":"not-an-observation"}`, `{"observed_error_code":"usage_limit_reached"} {}`, strings.Repeat(" ", 4097) + `{}`} {
		h, repo := quotaCooldownTestHandler()
		router := gin.New()
		router.POST("/api/v1/admin/grok/accounts/:id/quota-cooldown", h.SetQuotaCooldown)
		request := httptest.NewRequest(http.MethodPost, "/api/v1/admin/grok/accounts/900001/quota-cooldown", strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, request)
		require.Equal(t, http.StatusBadRequest, w.Code, body)
		require.Zero(t, repo.writes)
	}
}

func TestGrokQuotaCooldownHandlerPreservesFullAdminGateAndManualSwitch(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h, repo := quotaCooldownTestHandler()
	router := gin.New()
	// The real registration inherits AdminAuthMiddleware, audit, and compliance
	// from the existing /admin group. Exercise the inherited group boundary here.
	admin := router.Group("/api/v1/admin", func(c *gin.Context) {
		if c.GetHeader("X-Synthetic-Admin") != "allowed" {
			c.AbortWithStatus(http.StatusUnauthorized)
		}
	})
	admin.POST("/grok/accounts/:id/quota-cooldown", h.SetQuotaCooldown)
	for _, authorized := range []bool{false, true} {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/admin/grok/accounts/900001/quota-cooldown", strings.NewReader(`{"observed_error_code":"usage_limit_reached"}`))
		request.Header.Set("Content-Type", "application/json")
		if authorized {
			request.Header.Set("X-Synthetic-Admin", "allowed")
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, request)
		if !authorized {
			require.Equal(t, http.StatusUnauthorized, w.Code)
			require.Zero(t, repo.writes)
			continue
		}
		require.Equal(t, http.StatusOK, w.Code)
		require.False(t, repo.account.Schedulable)
		require.Equal(t, 1, repo.writes)
		var result struct {
			Data struct {
				Source               string     `json:"source"`
				OfficialResetAt      *time.Time `json:"official_reset_at"`
				ManualSchedulable    bool       `json:"manual_schedulable"`
				EffectiveSchedulable bool       `json:"effective_schedulable"`
			} `json:"data"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		require.Equal(t, "retry_probe", result.Data.Source)
		require.Nil(t, result.Data.OfficialResetAt)
		require.False(t, result.Data.ManualSchedulable)
		require.False(t, result.Data.EffectiveSchedulable)
	}
}

func TestGrokQuotaCooldownHandlerDoesNotExpandScopedAdminKeyPermissions(t *testing.T) {
	h, repo := quotaCooldownTestHandler()
	router := gin.New()
	router.POST("/api/v1/admin/grok/accounts/:id/quota-cooldown", h.SetQuotaCooldown)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/admin/grok/accounts/900001/quota-cooldown", strings.NewReader(`{"observed_error_code":"usage_limit_reached"}`))
	request = request.WithContext(adminaccess.WithPrincipal(request.Context(), &adminaccess.Principal{}))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, request)
	require.Equal(t, http.StatusForbidden, w.Code)
	require.Zero(t, repo.writes)
}
