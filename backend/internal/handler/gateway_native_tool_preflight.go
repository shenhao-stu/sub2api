package handler

import (
	"net/http"
	"slices"

	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func (h *GatewayHandler) rejectInvalidNativeToolResult(c *gin.Context, key *service.APIKey, parsed *service.ParsedRequest) bool {
	if h.cfg == nil || key.Group == nil || key.Group.Platform != service.PlatformAnthropic ||
		!key.Group.ClaudeCodeOnly || key.Group.FallbackGroupID != nil || parsed.Model == "" ||
		!slices.Contains(h.cfg.Gateway.AnthropicNativeToolPreflightGroupIDs, key.Group.ID) {
		return false
	}
	if platform, forced := middleware.GetForcePlatformFromContext(c); forced && platform != service.PlatformAnthropic {
		return false
	}
	err := service.ValidateNativeClaudeToolDefinitions(parsed.Body.Bytes())
	code := "invalid_tool_definition"
	if err == nil {
		err = service.ValidateNativeClaudeToolResults(parsed.MessagesRaw())
		code = "invalid_tool_result"
	}
	if err != nil {
		setOpsRequestContext(c, parsed.Model, parsed.Stream)
		setOpsEndpointContext(c, "", int16(service.RequestTypeFromLegacy(parsed.Stream, false)))
		h.errorResponseWithCode(c, http.StatusBadRequest, "invalid_request_error", code, err.Error())
		return true
	}
	return false
}
