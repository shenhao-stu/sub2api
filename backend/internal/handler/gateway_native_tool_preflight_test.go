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

func TestMessagesRejectsInvalidNativeToolResultsBeforeRouting(t *testing.T) {
	groupID := int64(25)
	for name, tc := range map[string]struct {
		content string
		field   string
	}{
		"bad reference name":       {`[{"type":"tool_reference","tool_name":"sensitive.value"}]`, "content[0].tool_name"},
		"unsupported result block": {`[{"type":"tool_use","id":"fixture","name":"Read","input":{}}]`, "content[0].type"},
		"null content":             {`null`, "content"},
	} {
		t.Run(name, func(t *testing.T) {
			setupOpsErrorLogTestQueue(t, 2)
			gin.SetMode(gin.TestMode)
			router := gin.New()
			ops := service.NewOpsService(&ingressRejectOpsRepo{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
			router.Use(OpsErrorLoggerMiddleware(ops))
			// The remaining dependencies are nil. Reaching routing, billing, or
			// concurrency would panic and fail this test.
			h := &GatewayHandler{cfg: &config.Config{Gateway: config.GatewayConfig{
				AnthropicNativeToolPreflightGroupIDs: []int64{groupID},
			}}}
			router.POST("/v1/messages", func(c *gin.Context) {
				c.Set(string(middleware.ContextKeyAPIKey), &service.APIKey{
					ID: 1, GroupID: &groupID,
					Group: &service.Group{ID: groupID, Platform: service.PlatformAnthropic, ClaudeCodeOnly: true},
				})
				c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 1, Concurrency: 1})
				h.Messages(c)
			})
			body := `{"model":"claude-opus-5-5","max_tokens":64,"stream":true,"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"fixture","content":` + tc.content + `}]}]}`
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(body)))
			require.Equal(t, http.StatusBadRequest, rec.Code)
			require.Contains(t, rec.Body.String(), `"code":"invalid_tool_result"`)
			require.Contains(t, rec.Body.String(), "messages[0].content[0]."+tc.field)
			require.NotContains(t, rec.Body.String(), "sensitive.value")
			require.Equal(t, int64(1), OpsErrorLogQueueLength())
			entry := (<-opsErrorLogQueue).entry
			require.Equal(t, "client", entry.ErrorOwner)
			require.Nil(t, entry.AccountID)
			require.Nil(t, entry.UpstreamStatusCode)
		})
	}
}

func TestNativeToolPreflightOnlyAppliesToExplicitNativeGroups(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := &GatewayHandler{cfg: &config.Config{Gateway: config.GatewayConfig{
		AnthropicNativeToolPreflightGroupIDs: []int64{25},
	}}}
	fallback := int64(26)
	body := `{"model":"claude-opus-5-5","messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"fixture","content":[{"type":"tool_reference","tool_name":"invalid.name"}]}]}]}`
	for _, group := range []*service.Group{
		nil,
		{ID: 26, Platform: service.PlatformAnthropic, ClaudeCodeOnly: true},
		{ID: 25, Platform: service.PlatformAnthropic, ClaudeCodeOnly: false},
		{ID: 25, Platform: service.PlatformAnthropic, ClaudeCodeOnly: true, FallbackGroupID: &fallback},
	} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
		parsed, err := service.ParseGatewayRequest(service.NewRequestBodyRef([]byte(body)), service.PlatformAnthropic)
		require.NoError(t, err)
		require.False(t, h.rejectInvalidNativeToolResult(c, &service.APIKey{Group: group}, parsed))
		require.False(t, c.Writer.Written())
	}
}
