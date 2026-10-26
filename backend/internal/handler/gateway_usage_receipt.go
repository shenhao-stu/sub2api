package handler

import (
	"errors"
	"net/http"

	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// KeyUsageReceipt reads one already-recorded request. It never starts generation,
// changes a balance, or accepts a caller-supplied owner/key ID.
func (h *GatewayHandler) KeyUsageReceipt(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	key, ok := middleware2.GetAPIKeyFromContext(c)
	if !ok || key == nil {
		h.errorResponse(c, http.StatusUnauthorized, "authentication_error", "Invalid API key")
		return
	}
	id := c.Param("request_id")
	if !service.ValidUsageReceiptID(id) {
		h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", "Invalid receipt request ID")
		return
	}
	if h.usageService == nil {
		h.errorResponse(c, http.StatusServiceUnavailable, "api_error", "Usage receipt is unavailable")
		return
	}
	receipt, err := h.usageService.GetAPIKeyUsageReceipt(c.Request.Context(), key.ID, id)
	if errors.Is(err, service.ErrUsageReceiptPending) {
		c.Header("Retry-After", "60")
		c.JSON(http.StatusAccepted, service.PendingUsageReceipt{
			Object:    "sub2api.usage_receipt",
			Version:   2,
			RequestID: id,
			State:     "pending",
		})
		return
	}
	if errors.Is(err, service.ErrUsageLogNotFound) {
		h.errorResponse(c, http.StatusNotFound, "not_found_error", "Usage receipt is not available for this API key")
		return
	}
	if err != nil {
		h.errorResponse(c, http.StatusServiceUnavailable, "api_error", "Usage receipt is unavailable")
		return
	}
	c.JSON(http.StatusOK, receipt)
}
