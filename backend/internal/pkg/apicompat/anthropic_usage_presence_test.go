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

func TestAnthropicInternalJSONRoundtripDoesNotCertifyZero(t *testing.T) {
	reason := "end_turn"
	response := AnthropicResponse{StopReason: &reason}
	raw, err := json.Marshal(response)
	require.NoError(t, err)
	var decoded AnthropicResponse
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Nil(t, AnthropicToResponsesResponse(&decoded).Usage)
}

func TestAnthropicStandaloneLateZeroRetainsObservedOutput(t *testing.T) {
	for _, tc := range []struct {
		name, start, delta string
		output             int
	}{
		{"delta", `{"input_tokens":10}`, `{"type":"message_delta","delta":{},"usage":{"output_tokens":4}}`, 4},
		{"initial", `{"input_tokens":10,"output_tokens":1}`, "", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := NewAnthropicEventToResponsesState()
			for _, raw := range []string{
				`{"type":"message_start","message":{"id":"msg_fixture","usage":` + tc.start + `}}`,
				tc.delta,
				`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":0,"output_tokens":0}}`,
			} {
				if raw == "" {
					continue
				}
				state.UsageEvidence = state.UsageEvidence.ObserveEventJSON([]byte(raw))
				var event AnthropicStreamEvent
				require.NoError(t, json.Unmarshal([]byte(raw), &event))
				AnthropicEventToResponsesEvents(&event, state)
			}
			events := AnthropicEventToResponsesEvents(&AnthropicStreamEvent{Type: "message_stop"}, state)
			require.NotEmpty(t, events)
			require.NotNil(t, events[len(events)-1].Response.Usage)
			require.Equal(t, tc.output, events[len(events)-1].Response.Usage.OutputTokens)
		})
	}
}

func TestAnthropicUsageEvidenceStartsNewReceipt(t *testing.T) {
	prefix := []string{
		`{"type":"message_start","message":{"usage":{"input_tokens":0}}}`,
		`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":0}}`,
	}
	for _, tc := range []struct {
		name     string
		frames   []string
		wantZero bool
	}{
		{"missing_new_terminal", []string{`{"type":"message_start","message":{"usage":{"input_tokens":0}}}`}, false},
		{"missing_new_input", []string{`{"type":"message_start","message":{"usage":{}}}`, `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":0}}`}, false},
		{"complete_new_receipt", []string{`{"type":"message_start","message":{"usage":{"input_tokens":0}}}`, `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":0}}`}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var evidence AnthropicUsageEvidence
			for _, raw := range append(append([]string{}, prefix...), tc.frames...) {
				evidence = evidence.ObserveEventJSON([]byte(raw))
			}
			usage := evidence.ResponsesUsage(AnthropicUsage{})
			if tc.wantZero {
				require.NotNil(t, usage)
				require.Equal(t, "explicit_zero", usage.MeteringState())
			} else {
				require.Nil(t, usage)
			}
		})
	}
}
