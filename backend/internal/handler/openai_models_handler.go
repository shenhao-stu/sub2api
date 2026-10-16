package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/Wei-Shaw/sub2api/internal/config"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func (h *GatewayHandler) pinnedOpenAIModels(c *gin.Context, group *service.Group) {
	if c.Request.Context().Err() != nil {
		return
	}
	if h.openAIGatewayService == nil {
		writeOpenAIModelsError(c, http.StatusInternalServerError, "api_error", "OpenAI model discovery is not configured")
		return
	}
	etag := c.GetHeader("If-None-Match")
	if c.Param("model") != "" {
		etag = "" // A collection ETag cannot validate a single-model representation.
	}
	response, account, err := h.openAIGatewayService.FetchPinnedOpenAIModelsList(
		c.Request.Context(), group, h.maxAccountSwitches, etag,
	)
	if c.Request.Context().Err() != nil {
		return
	}
	if err != nil {
		if errors.Is(err, service.ErrNoPinnedCodexModelsAccounts) {
			writeOpenAIModelsError(c, http.StatusServiceUnavailable, "upstream_error", "No available OpenAI model discovery accounts")
			return
		}
		writeOpenAIModelsError(c, infraerrors.Code(err), "upstream_error", infraerrors.Message(err))
		return
	}
	setOpsSelectedAccount(c, account.ID, account.Platform)
	writeOpenAIModelsResponse(c, response)
}

func writeOpenAIModelsError(c *gin.Context, status int, errorType, message string) {
	c.JSON(status, gin.H{"error": gin.H{"type": errorType, "message": message}})
}

func writeOpenAIModelsResponse(c *gin.Context, manifest *service.OpenAIModelsResponse) {
	if c.Param("model") != "" {
		writeRetrievedModel(c, manifest.Body)
		return
	}
	if manifest.ETag != "" {
		c.Header("ETag", manifest.ETag)
	}
	if manifest.NotModified {
		c.Status(http.StatusNotModified)
		c.Writer.WriteHeaderNow()
		return
	}
	c.Data(http.StatusOK, "application/json", manifest.Body)
}

// Both discovery endpoints consume the same final catalogue, after group/platform
// selection and allowlist filtering. Preserve every field on the selected entry.
func writeModelsListResponse(c *gin.Context, models any, configs ...*config.Config) {
	response := gin.H{"object": "list", "data": models}
	var cfg *config.Config
	if len(configs) > 0 {
		cfg = configs[0]
	}
	if c.Param("model") == "" && (cfg == nil || len(cfg.Gateway.DisabledModels) == 0) {
		c.JSON(http.StatusOK, response)
		return
	}
	body, err := json.Marshal(response)
	if err != nil {
		writeOpenAIModelsError(c, http.StatusInternalServerError, "api_error", "Failed to encode model catalogue")
		return
	}
	body, err = service.FilterDisabledModelsBody(cfg, body, nil, false)
	if err != nil {
		writeOpenAIModelsError(c, http.StatusInternalServerError, "api_error", "Failed to filter model catalogue")
		return
	}
	if c.Param("model") == "" {
		c.Data(http.StatusOK, "application/json", body)
		return
	}
	writeRetrievedModel(c, body)
}

func writeRetrievedModel(c *gin.Context, body []byte) {
	var catalog struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &catalog); err != nil {
		writeOpenAIModelsError(c, http.StatusBadGateway, "upstream_error", "Invalid model catalogue")
		return
	}
	modelID := c.Param("model")
	for _, raw := range catalog.Data {
		var model map[string]json.RawMessage
		if err := json.Unmarshal(raw, &model); err != nil {
			writeOpenAIModelsError(c, http.StatusBadGateway, "upstream_error", "Invalid model catalogue entry")
			return
		}
		var id string
		if err := json.Unmarshal(model["id"], &id); err != nil {
			writeOpenAIModelsError(c, http.StatusBadGateway, "upstream_error", "Invalid model catalogue ID")
			return
		}
		if id == modelID {
			c.Data(http.StatusOK, "application/json", raw)
			return
		}
	}
	c.JSON(http.StatusNotFound, gin.H{"error": gin.H{
		"type": "invalid_request_error", "code": "model_not_found", "param": "model",
		"message": fmt.Sprintf("Model %q does not exist or is not available for this group", modelID),
	}})
}

func (h *GatewayHandler) writeFallbackModelsList(c *gin.Context, groupID *int64, models any) {
	if h.cfg == nil || len(h.cfg.Gateway.DisabledModels) == 0 {
		writeModelsListResponse(c, models, h.cfg)
		return
	}
	raw, err := json.Marshal(models)
	if err != nil {
		writeOpenAIModelsError(c, 500, "api_error", "Failed to encode model catalogue")
		return
	}
	var entries []json.RawMessage
	if json.Unmarshal(raw, &entries) != nil {
		writeOpenAIModelsError(c, 500, "api_error", "Failed to encode model catalogue")
		return
	}
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		var model struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(entry, &model)
		ids = append(ids, model.ID)
	}
	allowed := h.gatewayService.FilterDisabledModelIDs(c.Request.Context(), groupID, ids)
	keep := make(map[string]bool, len(allowed))
	for _, id := range allowed {
		keep[id] = true
	}
	filtered := make([]json.RawMessage, 0, len(entries))
	for i, entry := range entries {
		if keep[ids[i]] {
			filtered = append(filtered, entry)
		}
	}
	writeModelsListResponse(c, filtered, h.cfg)
}
