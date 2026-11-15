//go:build unit

package service

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestDuplicateToolArgumentKeys(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		duplicate bool
	}{
		{"conflicting", `{"x":1,"x":2}`, true},
		{"equal", `{"x":1,"x":1}`, true},
		{"escaped", `{"x":1,"\u0078":2}`, true},
		{"nested_array", `{"items":[{"x":1,"x":2}]}`, true},
		{"nested_object", `{"a":{"b":{"x":1,"x":2}}}`, true},
		{"independent_objects", `{"items":[{"x":1},{"x":2}]}`, false},
		{"case_sensitive", `{"x":1,"X":2}`, false},
		{"large_number", `{"x":9007199254740993}`, false},
		{"string_content", `{"text":"{\"x\":1,\"x\":2}"}`, false},
		{"empty", `{}`, false},
		{"invalid_json", `{"x":1,"x":2`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.duplicate, duplicateToolArgumentKeys(tc.raw))
		})
	}
}

func TestGeminiToolArgumentAdmissionScope(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name, model, target, role, args string
		reject                          bool
	}{
		{"gemini", "gemini-4-argon", "", "assistant", `{"x":1,"x":2}`, true},
		{"mapped_alias", "alias", "ultra/gemini-4-argon-high", "assistant", `{"x":1,"x":2}`, true},
		{"mapped_away", "gemini-4-argon", "gpt-6-sol", "assistant", `{"x":1,"x":2}`, false},
		{"other_model", "gpt-6-sol", "", "assistant", `{"x":1,"x":2}`, false},
		{"not_tool_history", "gemini-4-argon", "", "user", `{"x":1,"x":2}`, false},
		{"valid_unchanged", "gemini-4-argon", "", "assistant", `{"x":9007199254740993}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, err := json.Marshal(map[string]any{"model": tc.model, "messages": []any{map[string]any{
				"role": tc.role, "tool_calls": []any{map[string]any{"type": "function", "function": map[string]any{"arguments": tc.args}}},
			}}})
			require.NoError(t, err)
			before := string(body)
			account := &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{}}
			if tc.target != "" {
				account.Credentials["model_mapping"] = map[string]any{tc.model: tc.target}
			}
			writer := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(writer)
			ctx.Request = httptest.NewRequest("POST", "/v1/chat/completions", nil)
			err = (&OpenAIGatewayService{}).ValidateGeminiChatToolArguments(ctx, account, body)
			require.Equal(t, tc.reject, err != nil)
			require.Equal(t, before, string(body))
			if tc.reject {
				require.Equal(t, 400, writer.Code)
				require.Contains(t, writer.Body.String(), "duplicate_tool_argument")
				require.NotContains(t, writer.Body.String(), tc.args)
			} else {
				require.False(t, ctx.Writer.Written())
			}
		})
	}
}
