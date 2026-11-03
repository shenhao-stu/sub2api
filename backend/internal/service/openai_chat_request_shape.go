package service

import "github.com/tidwall/gjson"

// IsResponsesShapedChatRequest keeps admission auditing and forwarding on the
// same input format. Empty messages retained by serializers do not hide input;
// meaningful or malformed messages retain Chat validation and conversion.
func IsResponsesShapedChatRequest(body []byte) bool {
	messages := gjson.GetBytes(body, "messages")
	return gjson.GetBytes(body, "input").Exists() &&
		(messages.Type == gjson.Null || (messages.IsArray() && len(messages.Array()) == 0))
}
