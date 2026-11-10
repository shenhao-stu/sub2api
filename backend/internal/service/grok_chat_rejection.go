package service

import (
	"encoding/json"
	"net/http"

	"github.com/tidwall/gjson"
)

// Recognize explicit Grok Chat capacity/throttle contracts, not arbitrary
// text or model-generated content. Unknown/resource-quota errors keep their path.
func grokRawChatRejectionPayload(account *Account, payload []byte) []byte {
	if account == nil || !account.IsGrok() || !gjson.ValidBytes(payload) || !gjson.GetBytes(payload, "error").IsObject() {
		return nil
	}
	code := gjson.GetBytes(payload, "error.code").String()
	capacity := code == "resource-exhausted" && classifyGrokUpstreamFailure(http.StatusBadGateway, payload, "").Class == GrokFailureModelCapacity
	throttled := gjson.GetBytes(payload, "error.type").String() == "rate_limit_error"
	if !capacity && !throttled {
		return nil
	}
	for _, path := range openAIStreamErrorStatusPaths {
		if status := gjson.GetBytes(payload, path).Int(); status >= 400 && status <= 599 && status != http.StatusTooManyRequests {
			return nil
		}
	}
	// This is a semantic 429 carried in HTTP 200. Include only bounded error
	// fields, retaining the provider code while redacting origin URLs/secrets.
	fields := boundedOpenAIClientError(payload, http.StatusTooManyRequests)
	body, _ := json.Marshal(map[string]any{"error": fields})
	return body
}
