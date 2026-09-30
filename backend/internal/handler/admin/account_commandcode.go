package admin

import (
	"net/http"
	"strconv"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/gin-gonic/gin"
)

func (h *AccountHandler) GetCommandCodeQuota(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		response.BadRequest(c, "Invalid account ID")
		return
	}
	account, err := h.adminService.GetAccount(c.Request.Context(), id)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	if !account.IsCommandCodeGo() {
		response.BadRequest(c, "Command Code quota is available for Go accounts only")
		return
	}
	quota, err := h.accountTestService.GetCommandCodeQuota(c.Request.Context(), account)
	if err != nil {
		response.Error(c, http.StatusBadGateway, "Command Code quota is unavailable; verify this account's API key and subscription")
		return
	}
	c.Header("Cache-Control", "no-store")
	response.Success(c, quota)
}
