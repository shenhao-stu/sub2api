package admin

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/Wei-Shaw/sub2api/internal/pkg/adminaccess"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/gin-gonic/gin"
)

// SetQuotaCooldown is registered only under the existing authenticated admin
// router. Scoped keys do not gain a new write capability from this endpoint.
func (h *GrokOAuthHandler) SetQuotaCooldown(c *gin.Context) {
	if adminaccess.FromContext(c.Request.Context()) != nil {
		response.Error(c, http.StatusForbidden, "Quota cooldown requires full administrator access")
		return
	}
	accountID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || accountID <= 0 {
		response.BadRequest(c, "Invalid account ID")
		return
	}
	var body *struct {
		ObservedErrorCode string `json:"observed_error_code"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(c.Writer, c.Request.Body, 4096))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&body); err != nil || body == nil {
		response.BadRequest(c, "Invalid quota cooldown request")
		return
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		response.BadRequest(c, "Invalid quota cooldown request")
		return
	}
	if h.quotaService == nil {
		response.Error(c, http.StatusServiceUnavailable, "Grok quota service is unavailable")
		return
	}
	result, err := h.quotaService.SetQuotaCooldown(c.Request.Context(), accountID, body.ObservedErrorCode)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}
