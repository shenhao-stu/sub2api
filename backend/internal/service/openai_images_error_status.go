package service

import (
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/util/logredact"
	"github.com/tidwall/gjson"
)

var imageErrorURL = regexp.MustCompile(`(?i)\b(?:https?|socks5h?)://[^\s<>"']+`)
var imageErrorBearer = regexp.MustCompile(`(?i)\bBearer\s+[A-Za-z0-9._~+/-]+=*`)

func sanitizeImageErrorMessage(message string) string {
	message = logredact.RedactText(message, "api_key", "apikey", "authorization", "cookie")
	message = imageErrorURL.ReplaceAllString(message, "<upstream>")
	message = imageErrorBearer.ReplaceAllString(message, "Bearer ***")
	return truncateString(strings.TrimSpace(message), 2048)
}

// Explicit terminal status survives a prior HTTP 200 heartbeat. Accept only
// consistent integer errors; an ambiguous status never becomes a success.
func openAIImagesExplicitErrorStatus(payload []byte, fallback int) int {
	status, invalid := 0, false
	for _, object := range []gjson.Result{gjson.ParseBytes(payload), gjson.GetBytes(payload, "error"), gjson.GetBytes(payload, "response.error")} {
		seen := false
		object.ForEach(func(key, value gjson.Result) bool {
			if key.String() != "status_code" {
				return true
			}
			code, err := strconv.Atoi(value.Raw)
			if seen || value.Type != gjson.Number || err != nil || code < 400 || code > 599 || (status != 0 && status != code) {
				invalid = true
			}
			seen, status = true, code
			return true
		})
	}
	if invalid {
		return http.StatusBadGateway
	}
	if status != 0 {
		return status
	}
	return fallback
}
