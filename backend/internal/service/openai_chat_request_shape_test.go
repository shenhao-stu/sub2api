package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResponsesShapedChatKeepsAuditAndForwardingInputAligned(t *testing.T) {
	for _, tc := range []struct {
		name, body, wantText string
		responses            bool
	}{
		{"absent", `{"input":"input marker"}`, "input marker", true},
		{"null", `{"messages":null,"input":"input marker"}`, "input marker", true},
		{"empty", `{"messages":[],"input":[{"role":"user","content":"input marker"}]}`, "input marker", true},
		{"chat_takes_precedence", `{"messages":[{"role":"user","content":"chat marker"}],"input":"unused"}`, "chat marker", false},
		{"malformed_messages", `{"messages":"invalid","input":"unused"}`, "", false},
		{"object_messages", `{"messages":{},"input":"unused"}`, "", false},
		{"no_input", `{"messages":[]}`, "", false},
		{"null_input", `{"messages":[],"input":null}`, "", true},
		{"empty_input", `{"messages":[],"input":[]}`, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(tc.body)
			require.Equal(t, tc.responses, IsResponsesShapedChatRequest(body))
			protocol := ContentModerationProtocolOpenAIChat
			if IsResponsesShapedChatRequest(body) {
				protocol = ContentModerationProtocolOpenAIResponses
			}
			require.Equal(t, tc.wantText, ExtractContentModerationText(protocol, body))
		})
	}
}
