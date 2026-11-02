package handler

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestReceiptLookupDoesNotCountAsFailedGeneration(t *testing.T) {
	const id = "2c4e888b-836b-4992-9ff1-dc1699488902"
	for _, tc := range []struct {
		name     string
		key      *service.APIKey
		id       string
		err      error
		result   *service.UsageReceipt
		status   int
		failures int64
	}{
		{"missing_owned", &service.APIKey{ID: 11}, id, service.ErrUsageLogNotFound, nil, 404, 0},
		{"missing_other_key", &service.APIKey{ID: 22}, id, service.ErrUsageLogNotFound, nil, 404, 0},
		{"pending", &service.APIKey{ID: 11}, id, service.ErrUsageReceiptPending, nil, 202, 0},
		{"final", &service.APIKey{ID: 11}, id, nil, &service.UsageReceipt{Model: "model", InputTokens: 12, OutputTokens: 3, RecordedAt: time.Now()}, 200, 0},
		{"unauthenticated", nil, id, nil, nil, 401, 1},
		{"invalid_id", &service.APIKey{ID: 11}, "invalid", nil, nil, 400, 1},
		{"storage_error", &service.APIKey{ID: 11}, id, errors.New("private database failure"), nil, 503, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setupOpsErrorLogTestQueue(t, 3)
			gin.SetMode(gin.TestMode)
			repo := &receiptRepository{err: tc.err, result: tc.result}
			h := &GatewayHandler{usageService: service.NewUsageService(repo, nil, nil, nil)}
			r := gin.New()
			r.Use(OpsErrorLoggerMiddleware(service.NewOpsService(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)))
			r.GET("/v1/sub2api/usage/:request_id", func(c *gin.Context) {
				if tc.key != nil {
					c.Set(string(middleware2.ContextKeyAPIKey), tc.key)
				}
				h.KeyUsageReceipt(c)
			})
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/v1/sub2api/usage/"+tc.id+"?api_key_id=999", nil)
			r.ServeHTTP(w, req)
			require.Equal(t, tc.status, w.Code)
			require.Equal(t, tc.failures, OpsErrorLogQueueLength())
			if tc.status == 404 {
				require.Equal(t, tc.key.ID, repo.key)
				require.Contains(t, w.Body.String(), `"code":"usage_receipt_not_found"`)
				require.NotContains(t, w.Body.String(), "output_tokens")
				require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
			}
		})
	}
}

func TestGenerationCannotImpersonateReceiptLookupMiss(t *testing.T) {
	setupOpsErrorLogTestQueue(t, 3)
	r := gin.New()
	r.Use(OpsErrorLoggerMiddleware(service.NewOpsService(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)))
	r.POST("/v1/messages", func(c *gin.Context) {
		c.JSON(404, gin.H{"type": "error", "error": gin.H{"type": "not_found_error", "code": "usage_receipt_not_found", "message": "No recorded final usage receipt for this request"}})
	})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	req.Header.Set("X-Sub2API-Usage-Receipt", "v1")
	req.Header.Set("X-Ops-Receipt-Lookup-Miss", "true")
	r.ServeHTTP(w, req)
	require.Equal(t, 404, w.Code)
	require.Equal(t, int64(1), OpsErrorLogQueueLength())
}
