package apicompat

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAnthropicResponsesDoesNotInventZeroUsage(t *testing.T) {
	t.Run("buffered", func(t *testing.T) {
		var response AnthropicResponse
		require.NoError(t, json.Unmarshal([]byte(`{"id":"msg_fixture","type":"message","role":"assistant","content":[{"type":"text","text":"fixture answer"}],"stop_reason":"end_turn"}`), &response))
		converted := AnthropicToResponsesResponse(&response)
		require.Nil(t, converted.Usage)
	})
	t.Run("stream", func(t *testing.T) {
		state := NewAnthropicEventToResponsesState()
		AnthropicEventToResponsesEvents(&AnthropicStreamEvent{Type: "message_start", Message: &AnthropicResponse{ID: "msg_fixture"}}, state)
		events := AnthropicEventToResponsesEvents(&AnthropicStreamEvent{Type: "message_stop"}, state)
		require.NotEmpty(t, events)
		require.Nil(t, events[len(events)-1].Response.Usage)
	})
}

func TestAnthropicBufferedUsageWireEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, usage, stop string
		want              bool
	}{
		{"explicit", `,"usage":{"input_tokens":0,"output_tokens":0}`, `,"stop_reason":"end_turn"`, true},
		{"nonterminal", `,"usage":{"input_tokens":0,"output_tokens":0}`, "", false},
		{"partial", `,"usage":{"input_tokens":0}`, `,"stop_reason":"end_turn"`, false},
		{"null", `,"usage":{"input_tokens":0,"output_tokens":null}`, `,"stop_reason":"end_turn"`, false},
		{"duplicate_usage", `,"usage":{"input_tokens":0,"output_tokens":0},"usage":{"input_tokens":0,"output_tokens":0}`, `,"stop_reason":"end_turn"`, false},
		{"duplicate_count", `,"usage":{"input_tokens":0,"output_tokens":0,"OUTPUT_TOKENS":0}`, `,"stop_reason":"end_turn"`, false},
		{"unknown_positive_cache", `,"usage":{"input_tokens":0,"output_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":1}}`, `,"stop_reason":"end_turn"`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var response AnthropicResponse
			require.NoError(t, json.Unmarshal([]byte(`{"id":"msg_fixture","type":"message","role":"assistant","content":[{"type":"thinking","signature":"fixture signature"}]`+tc.usage+tc.stop+`}`), &response))
			converted := AnthropicToResponsesResponse(&response)
			if !tc.want {
				require.Nil(t, converted.Usage)
				return
			}
			require.NotNil(t, converted.Usage)
			require.Equal(t, "explicit_zero", converted.Usage.MeteringState())
		})
	}
}

func TestAnthropicStandaloneLateZeroRetainsObservedOutput(t *testing.T) {
	state := NewAnthropicEventToResponsesState()
	for _, raw := range []string{
		`{"type":"message_start","message":{"id":"msg_fixture","usage":{"input_tokens":10}}}`,
		`{"type":"message_delta","delta":{},"usage":{"output_tokens":4}}`,
		`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":0,"output_tokens":0}}`,
	} {
		state.UsageEvidence = state.UsageEvidence.ObserveEventJSON([]byte(raw))
		var event AnthropicStreamEvent
		require.NoError(t, json.Unmarshal([]byte(raw), &event))
		AnthropicEventToResponsesEvents(&event, state)
	}
	events := AnthropicEventToResponsesEvents(&AnthropicStreamEvent{Type: "message_stop"}, state)
	require.NotEmpty(t, events)
	require.NotNil(t, events[len(events)-1].Response.Usage)
	require.Equal(t, 4, events[len(events)-1].Response.Usage.OutputTokens)
}
