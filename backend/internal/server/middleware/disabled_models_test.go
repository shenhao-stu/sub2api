package middleware

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestDisabledModelsAdmissionAllHTTPForms(t *testing.T) {
	cfg := &config.Config{Gateway: config.GatewayConfig{DisabledModels: []string{"gpt-5.5", "gpt-5.5-*"}}}
	for _, grouped := range []bool{false, true} {
		for _, tc := range []struct{ method, path, body string }{
			{"POST", "/v1/responses", `{"model":"gpt-5.5"}`},
			{"POST", "/v1/responses/compact", `{"model":"gpt-5.5[1m]"}`},
			{"POST", "/v1/messages", `{"model":"gpt-6.1-sol","Model":"gpt-5.5"}`},
			{"POST", "/v1/chat/completions", `{"model":"gpt-5.5","model":"gpt-6.1-sol"}`},
			{"POST", "/v1/embeddings", `{"model":"gpt-5.5"}`},
			{"GET", "/v1/models/gpt-5.5", ""},
			{"GET", "/v1/realtime?model=gpt-5.5", ""},
			{"POST", "/v1beta/models/gpt-5.5:generateContent", `{}`},
		} {
			t.Run(tc.path+tc.body+map[bool]string{true: "group", false: "ungrouped"}[grouped], func(t *testing.T) {
				called := false
				r := gin.New()
				r.Use(func(c *gin.Context) {
					if grouped {
						c.Set(string(ContextKeyAPIKey), &service.APIKey{Group: &service.Group{}})
					}
				})
				r.Use(GroupModelAllowlist(cfg))
				for _, path := range []string{"/v1/responses", "/v1/responses/compact", "/v1/messages", "/v1/chat/completions", "/v1/embeddings", "/v1/models/:model", "/v1/realtime", "/v1beta/models/*modelAction"} {
					r.Any(path, func(c *gin.Context) { called = true })
				}
				req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
				req.Header.Set("Content-Type", "application/json")
				w := httptest.NewRecorder()
				r.ServeHTTP(w, req)
				require.Equal(t, 404, w.Code)
				require.False(t, called)
			})
		}
	}
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	require.NoError(t, mw.WriteField("model", "gpt-6.1-sol"))
	require.NoError(t, mw.WriteField("model", "gpt-5.5"))
	require.NoError(t, mw.Close())
	called := false
	r := gin.New()
	r.Use(GroupModelAllowlist(cfg))
	r.POST("/v1/images/edits", func(c *gin.Context) { called = true })
	req := httptest.NewRequest("POST", "/v1/images/edits", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusNotFound, w.Code)
	require.False(t, called)
}

func TestDisabledModelsAdmissionPreservesSupportedBody(t *testing.T) {
	cfg := &config.Config{Gateway: config.GatewayConfig{DisabledModels: []string{"gpt-5.5", "gpt-5.5-*"}}}
	for _, model := range []string{"gpt-6.1-sol", "gpt-5.50", "gpt-image-2.5-flare", "gpt-image-2.5-sunburst"} {
		r := gin.New()
		r.Use(GroupModelAllowlist(cfg))
		r.POST("/v1/responses", func(c *gin.Context) { c.Status(204) })
		w := doJSON(t, r, "POST", "/v1/responses", `{"model":"`+model+`"}`)
		require.Equal(t, 204, w.Code)
	}
}
