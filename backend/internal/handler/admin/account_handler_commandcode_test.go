package admin

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func commandCodeModelsRouter(account service.Account, upstream service.HTTPUpstream) *gin.Engine {
	admin := &availableModelsAdminService{stubAdminService: newStubAdminService(), account: account}
	tester := service.NewAccountTestService(nil, nil, nil, nil, nil, upstream, nil, nil)
	handler := NewAccountHandler(admin, nil, nil, nil, nil, nil, nil, nil, tester, nil, nil, nil, nil, nil)
	router := gin.New()
	router.GET("/api/v1/admin/accounts/:id/models", handler.GetAvailableModels)
	router.POST("/api/v1/admin/accounts/models/sync-upstream-preview", handler.SyncUpstreamModelsPreview)
	return router
}

func TestAccountHandlerGetAvailableModels_CommandCodeDoesNotFallBack(t *testing.T) {
	account := service.Account{ID: 87, Type: service.AccountTypeAPIKey, Platform: service.PlatformOpenAI,
		Credentials: map[string]any{"base_url": service.CommandCodeGoBaseURL, "api_key": "private-test-key"},
		Extra:       map[string]any{"provider": service.CommandCodeGoProvider}}
	upstream := &syncUpstreamHTTPUpstream{resp: &http.Response{StatusCode: 503, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("private-upstream-details"))}}
	router := commandCodeModelsRouter(account, upstream)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/admin/accounts/87/models", nil))
	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.NotContains(t, rec.Body.String(), "gpt-")
	require.NotContains(t, rec.Body.String(), "private-")
}

func TestAccountHandlerCommandCodePreviewNeedsNoKeyAndFiltersProtocol(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		status      int
	}{
		{"public", `{"platform":"anthropic","type":"apikey","base_url":"https://api.commandcode.ai/provider","extra":{"provider":"commandcode"}}`, 200},
		{"ordinary_requires_key", `{"platform":"anthropic","type":"apikey","base_url":"https://api.anthropic.com"}`, 400},
		{"foreign_origin_rejected", `{"platform":"anthropic","type":"apikey","base_url":"https://attacker.invalid","extra":{"provider":"commandcode"}}`, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := `{"data":[{"id":"vendor/messages","context_length":12345,"supported_endpoints":["/messages"]},{"id":"vendor/chat","supported_endpoints":["/chat/completions"]}]}`
			upstream := &syncUpstreamHTTPUpstream{resp: &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(catalog))}}
			router := commandCodeModelsRouter(service.Account{}, upstream)
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/accounts/models/sync-upstream-preview", strings.NewReader(tc.input))
			req.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(rec, req)
			require.Equal(t, tc.status, rec.Code)
			if tc.status == http.StatusOK {
				var response struct {
					Data service.UpstreamModelCatalog `json:"data"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
				require.Equal(t, []string{"vendor/messages"}, response.Data.Models)
				require.EqualValues(t, 12345, response.Data.Metadata["vendor/messages"].ContextWindow)
			}
		})
	}
}
