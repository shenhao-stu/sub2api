package handler

import (
	"strconv"
	"strings"
)

// One accepted turn keeps one billing identity across retries. An upstream that
// omits response.id must not make every turn reuse the connection's request id.
func openAIWSTurnBillingRequestID(sessionID string, turn int, upstreamID string) string {
	if id := strings.TrimSpace(upstreamID); id != "" {
		return id
	}
	return "ws-turn:" + sessionID + ":" + strconv.Itoa(turn)
}
