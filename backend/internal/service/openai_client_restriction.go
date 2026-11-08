package service

import (
	"net/http"
	"regexp"
	"strings"

	"github.com/gin-gonic/gin"
)

var codexClientRestrictionMessage = regexp.MustCompile(`^(?:Access forbidden \(403\): )?client '[A-Za-z0-9_.-]{1,64}' is not allowed on the Codex hop$`)

const codexClientRestrictionAdvice = "This channel requires a supported Codex client. Use a Codex client or a channel that supports your current client."

// This explicit hop-level rejection describes the client, not account health.
// Keep opaque 403s and credential failures on the existing account policy.
func isOpenAIClientRestriction(status int, body []byte) bool {
	if status != http.StatusForbidden || len(body) > 64<<10 {
		return false
	}
	return codexClientRestrictionMessage.MatchString(strings.TrimSpace(extractUpstreamErrorMessage(body)))
}

func recordOpenAIClientRestriction(c *gin.Context, account *Account, headers http.Header) {
	setOpsUpstreamError(c, http.StatusForbidden, codexClientRestrictionAdvice, "")
	appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
		AccountID: account.ID, Platform: account.Platform, UpstreamStatusCode: http.StatusForbidden,
		UpstreamRequestID: headers.Get("x-request-id"), Kind: "request_rejection", Message: codexClientRestrictionAdvice,
	})
	MarkResponseCommitted(c)
}

func writeOpenAIClientRestriction(c *gin.Context) {
	writeOpenAIErrorFields(c, openAIClientErrorFields{StatusCode: http.StatusForbidden, Type: "invalid_request_error", Code: "client_not_allowed", Message: codexClientRestrictionAdvice}, false, nil)
}
