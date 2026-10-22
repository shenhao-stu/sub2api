package middleware

import (
	"net/http"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// Exact read-only route matching prevents a crafted generation path or method
// from borrowing the completed-request quota exemption.
func isUsageReceiptRequest(method, path string) bool {
	const prefix = "/v1/sub2api/usage/"
	return method == http.MethodGet && strings.HasPrefix(path, prefix) && service.ValidUsageReceiptID(strings.TrimPrefix(path, prefix))
}

// The response capability identifies the receipt protocol, not a promise that
// every provider will return a final meter. Missing receipts remain unknown.
func UsageReceiptCapability() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.Method == http.MethodPost && c.Request.URL.Path == "/v1/messages" {
			c.Header("X-Sub2API-Usage-Receipt", "v1")
		}
		c.Next()
	}
}
