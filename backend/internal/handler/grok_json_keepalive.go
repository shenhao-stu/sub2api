package handler

import (
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

var grokJSONKeepaliveInterval = 30 * time.Second

func startGrokJSONKeepalive(c *gin.Context, platform string, stream bool) func() {
	if platform != service.PlatformGrok || stream {
		return func() {}
	}
	return service.StartOpenAIJSONKeepalive(c, grokJSONKeepaliveInterval)
}

func openAIForwardWrittenSize(c *gin.Context) int {
	if service.OpenAIImagesJSONKeepalivePresent(c) {
		return service.OpenAIImagesJSONKeepaliveAdjustedWrittenSize(c)
	}
	return service.OpenAICompactKeepaliveAdjustedWrittenSize(c)
}
