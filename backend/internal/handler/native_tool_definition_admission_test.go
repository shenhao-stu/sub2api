package handler

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestNativeToolDefinitionsRejectedBeforeRouting(t *testing.T) {
	for name, tools := range map[string]string{
		"duplicate":       `[{"name":"Read","input_schema":{}},{"name":"Read","input_schema":{}}]`,
		"invalid name":    `[{"name":"private.tool","input_schema":{}}]`,
		"non string name": `[{"name":17,"input_schema":{}}]`,
		"missing schema":  `[{"name":"Read"}]`,
		"null tools":      `null`,
	} {
		for _, path := range []string{"/v1/messages", "/v1/messages/count_tokens"} {
			t.Run(name+path, func(t *testing.T) {
				setupOpsErrorLogTestQueue(t, 2)
				gin.SetMode(gin.TestMode)
				groupID := int64(25)
				ops := service.NewOpsService(&ingressRejectOpsRepo{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
				h := &GatewayHandler{cfg: &config.Config{Gateway: config.GatewayConfig{AnthropicNativeToolPreflightGroupIDs: []int64{groupID}}}}
				r := gin.New()
				r.Use(OpsErrorLoggerMiddleware(ops))
				r.POST(path, func(c *gin.Context) {
					c.Set(string(middleware.ContextKeyAPIKey), &service.APIKey{ID: 1, GroupID: &groupID, Group: &service.Group{ID: groupID, Platform: service.PlatformAnthropic, ClaudeCodeOnly: true}})
					c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 1, Concurrency: 1})
					if path == "/v1/messages" {
						h.Messages(c)
					} else {
						h.CountTokens(c)
					}
				})
				body := `{"model":"claude-opus-5-5","max_tokens":64,"tools":` + tools + `,"messages":[{"role":"user","content":"fixture"}]}`
				rec := httptest.NewRecorder()
				// Nil routing, billing and concurrency dependencies prove no upstream path is reached.
				require.NotPanics(t, func() { r.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))) })
				require.Equal(t, http.StatusBadRequest, rec.Code)
				require.Contains(t, rec.Body.String(), `"code":"invalid_tool_definition"`)
				require.NotContains(t, rec.Body.String(), "private.tool")
				if path == "/v1/messages/count_tokens" {
					require.Zero(t, OpsErrorLogQueueLength()) // count_tokens is intentionally excluded from Ops.
					return
				}
				require.Equal(t, int64(1), OpsErrorLogQueueLength())
				entry := (<-opsErrorLogQueue).entry
				require.Nil(t, entry.AccountID)
				require.Nil(t, entry.UpstreamStatusCode)
			})
		}
	}
}

func TestNativeToolDefinitionPreflightPreservesValidToolsAndScope(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := &GatewayHandler{cfg: &config.Config{Gateway: config.GatewayConfig{AnthropicNativeToolPreflightGroupIDs: []int64{25}}}}
	for _, tools := range []string{`[]`, `[{"name":"Read","input_schema":{}},{"name":"read","input_schema":{},"description":"fixture"}]`} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		body := `{"model":"claude-opus-5-5","tools":` + tools + `,"messages":[{"role":"user","content":"fixture"}]}`
		parsed, err := service.ParseGatewayRequest(service.NewRequestBodyRef([]byte(body)), service.PlatformAnthropic)
		require.NoError(t, err)
		require.False(t, h.rejectInvalidNativeToolResult(c, &service.APIKey{Group: &service.Group{ID: 25, Platform: service.PlatformAnthropic, ClaudeCodeOnly: true}}, parsed))
	}
	fallback := int64(26)
	for _, group := range []*service.Group{nil, {ID: 26, Platform: service.PlatformAnthropic, ClaudeCodeOnly: true}, {ID: 25, Platform: service.PlatformAnthropic}, {ID: 25, Platform: service.PlatformAnthropic, ClaudeCodeOnly: true, FallbackGroupID: &fallback}} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		parsed, err := service.ParseGatewayRequest(service.NewRequestBodyRef([]byte(`{"model":"claude-opus-5-5","tools":[{"name":"bad.name"}],"messages":[{"role":"user","content":"fixture"}]}`)), service.PlatformAnthropic)
		require.NoError(t, err)
		require.False(t, h.rejectInvalidNativeToolResult(c, &service.APIKey{Group: group}, parsed))
		require.False(t, c.Writer.Written())
	}
}
