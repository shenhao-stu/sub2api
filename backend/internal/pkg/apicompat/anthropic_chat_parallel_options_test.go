package apicompat

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAnthropicChatParallelOptionsRequireConvertedTools(t *testing.T) {
	for _, tc := range []struct{ name, tools string }{
		{"absent", `{}`},
		{"empty", `{"tools":[]}`},
		{"only unsupported server tool", `{"tools":[{"type":"web_search_20250305","name":"web_search"}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var req AnthropicRequest
			require.NoError(t, json.Unmarshal([]byte(tc.tools), &req))
			req.Model = "kimi-k3"
			out, err := AnthropicToChatCompletionsRequest(&req)
			require.NoError(t, err)
			payload, err := json.Marshal(out)
			require.NoError(t, err)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(payload, &fields))
			require.NotContains(t, fields, "parallel_tool_calls", "tool-only options must not make a text request invalid")
		})
	}
}

func TestAnthropicChatParallelOptionsPreserveDisableParallelToolUse(t *testing.T) {
	for _, disable := range []bool{false, true} {
		var req AnthropicRequest
		require.NoError(t, json.Unmarshal([]byte(`{"model":"kimi-k3","tools":[{"name":"lookup","input_schema":{"type":"object"}}]}`), &req))
		req.ToolChoice, _ = json.Marshal(map[string]any{"type": "auto", "disable_parallel_tool_use": disable})
		out, err := AnthropicToChatCompletionsRequest(&req)
		require.NoError(t, err)
		require.Len(t, out.Tools, 1)
		require.NotNil(t, out.ParallelToolCalls)
		require.Equal(t, !disable, *out.ParallelToolCalls)
	}
}
