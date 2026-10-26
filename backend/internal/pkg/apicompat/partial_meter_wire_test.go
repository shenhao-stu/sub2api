package apicompat

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestPartialResponsesMeterCannotBecomeCompleteChatMeter(t *testing.T) {
	for _, tc := range []struct{ raw, known, absent string }{
		{`{"input_tokens":100}`, "prompt_tokens", "completion_tokens"},
		{`{"output_tokens":3}`, "completion_tokens", "prompt_tokens"},
		{`{"input_tokens_details":{"cached_tokens":90}}`, "prompt_tokens_details.cached_tokens", "prompt_tokens"},
	} {
		var source ResponsesUsage
		require.NoError(t, json.Unmarshal([]byte(tc.raw), &source))
		wire, err := json.Marshal(chatUsageFromResponsesUsage(&source))
		require.NoError(t, err)
		require.True(t, gjson.GetBytes(wire, tc.known).Exists(), string(wire))
		require.False(t, gjson.GetBytes(wire, tc.absent).Exists(), string(wire))
		require.False(t, gjson.GetBytes(wire, "total_tokens").Exists(), string(wire))
	}
}

func TestPartialMeterSurvivesBufferedRemarshalAndFinalReplacement(t *testing.T) {
	for _, raw := range []string{`{"input_tokens":100}`, `{"input_tokens":100,"output_tokens":null}`, `{"input_tokens":100,"output_tokens":0,"output_tokens":2}`} {
		var source ResponsesUsage
		require.NoError(t, json.Unmarshal([]byte(raw), &source))
		encoded, err := json.Marshal(source)
		require.NoError(t, err)
		require.False(t, gjson.GetBytes(encoded, "output_tokens").Exists(), string(encoded))
		partial := chatUsageFromResponsesUsage(&source)
		var final ResponsesUsage
		require.NoError(t, json.Unmarshal([]byte(`{"input_tokens":100,"output_tokens":15}`), &final))
		encoded, err = json.Marshal(retainChatUsage(partial, chatUsageFromResponsesUsage(&final)))
		require.NoError(t, err)
		require.Equal(t, int64(15), gjson.GetBytes(encoded, "completion_tokens").Int())
		require.Equal(t, int64(115), gjson.GetBytes(encoded, "total_tokens").Int())
	}
}
