package apicompat

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"testing"
)

func TestClaudeCacheControlCompatibilityRoundTrip(t *testing.T) {
	for _, protocol := range []string{"chat", "responses"} {
		for _, cc := range []string{`{"type":"ephemeral"}`, `{"type":"ephemeral","ttl":"5m"}`, `{"type":"ephemeral","ttl":"1h"}`, `{"type":"ephemeral","ttl":null}`, `{"type":"ephemeral","ttl":"invalid"}`, `null`, `{"type":"ephemeral","extension":3}`} {
			t.Run(protocol+cc, func(t *testing.T) {
				body := `{"model":"claude-sonnet-4","cache_control":` + cc + `,"tools":[{"type":"function","function":{"name":"probe","parameters":{"type":"object"}},"cache_control":` + cc + `}],"messages":[{"role":"system","content":[{"type":"text","text":"","cache_control":` + cc + `}]},{"role":"user","content":[{"type":"text","text":"","cache_control":` + cc + `}]}]}`
				var r *ResponsesRequest
				if protocol == "chat" {
					var in ChatCompletionsRequest
					require.NoError(t, json.Unmarshal([]byte(body), &in))
					var err error
					r, err = ChatCompletionsToResponses(&in)
					require.NoError(t, err)
				} else {
					body = `{"model":"claude-sonnet-4","cache_control":` + cc + `,"tools":[{"type":"function","name":"probe","parameters":{"type":"object"},"cache_control":` + cc + `}],"input":[{"role":"system","content":[{"type":"input_text","text":"","cache_control":` + cc + `}]},{"role":"user","content":[{"type":"input_text","text":"","cache_control":` + cc + `}]}]}`
					r = &ResponsesRequest{}
					require.NoError(t, json.Unmarshal([]byte(body), r))
				}
				a, err := ResponsesToAnthropicRequest(r)
				require.NoError(t, err)
				wire, err := json.Marshal(a)
				require.NoError(t, err)
				for _, path := range []string{"cache_control", "tools.0.cache_control", "system.0.cache_control", "messages.0.content.0.cache_control"} {
					require.JSONEq(t, cc, gjson.GetBytes(wire, path).Raw, path)
				}
			})
		}
	}
}

func TestClaudeCacheControlAssistantBoundaries(t *testing.T) {
	var req ChatCompletionsRequest
	require.NoError(t, json.Unmarshal([]byte(`{"model":"claude-sonnet-4","messages":[{"role":"user","content":"hi"},{"role":"assistant","reasoning_content":"synthetic reason","content":[{"type":"text","text":"one","cache_control":{"type":"ephemeral","ttl":"1h"}},{"type":"text","text":"","cache_control":null}]}]}`), &req))
	response, err := ChatCompletionsToResponses(&req)
	require.NoError(t, err)
	a, err := ResponsesToAnthropicRequest(response)
	require.NoError(t, err)
	wire, err := json.Marshal(a)
	require.NoError(t, err)
	require.Equal(t, "1h", gjson.GetBytes(wire, "messages.1.content.1.cache_control.ttl").String())
	require.Equal(t, "null", gjson.GetBytes(wire, "messages.1.content.2.cache_control").Raw)
	require.Equal(t, "one", gjson.GetBytes(wire, "messages.1.content.1.text").String())
}
