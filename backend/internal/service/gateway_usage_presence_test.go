package service

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAnthropicSSEUsagePresence(t *testing.T) {
	parsers := map[string]func(string, *ClaudeUsage){
		"forward":     (&GatewayService{}).parseSSEUsage,
		"passthrough": parseSSEUsagePassthrough,
	}
	for name, parse := range parsers {
		t.Run(name, func(t *testing.T) {
			t.Run("observed_completed_kiro_usage", func(t *testing.T) {
				usage := &ClaudeUsage{}
				parse(`{"type":"message_start","message":{"usage":{"input_tokens":8,"output_tokens":1,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}`, usage)
				parse(`{"type":"message_delta","usage":{"input_tokens":0,"output_tokens":2,"cache_read_input_tokens":7553,"cache_creation_input_tokens":1644}}`, usage)
				require.Equal(t, &ClaudeUsage{
					InputTokens: 0, OutputTokens: 2,
					CacheReadInputTokens: 7553, CacheCreationInputTokens: 1644,
				}, usage)
			})
			t.Run("omitted_fields_keep_observed_usage", func(t *testing.T) {
				usage := &ClaudeUsage{InputTokens: 8, CacheReadInputTokens: 7, OutputTokens: 1}
				parse(`{"type":"message_delta","usage":{"output_tokens":2}}`, usage)
				require.Equal(t, &ClaudeUsage{InputTokens: 8, CacheReadInputTokens: 7, OutputTokens: 2}, usage)
				parse(`{"type":"error","error":{"type":"upstream_error","message":"stopped"}}`, usage)
				require.Equal(t, &ClaudeUsage{InputTokens: 8, CacheReadInputTokens: 7, OutputTokens: 2}, usage)
			})
			t.Run("malformed_event_keeps_observed_usage", func(t *testing.T) {
				usage := &ClaudeUsage{InputTokens: 8, OutputTokens: 2}
				parse(`{"type":"message_delta","usage":{"input_tokens":0`, usage)
				require.Equal(t, &ClaudeUsage{InputTokens: 8, OutputTokens: 2}, usage)
			})
			t.Run("explicit_zero_overwrites_every_bucket", func(t *testing.T) {
				usage := &ClaudeUsage{
					InputTokens: 8, OutputTokens: 2, CacheReadInputTokens: 7,
					CacheCreationInputTokens: 3, CacheCreation5mTokens: 1, CacheCreation1hTokens: 2,
				}
				parse(`{"type":"message_delta","usage":{"input_tokens":0,"output_tokens":0,"cache_read_input_tokens":0,"cache_creation_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0}}}`, usage)
				require.Equal(t, &ClaudeUsage{}, usage)
			})
			for _, event := range []string{"message_start", "message_delta"} {
				t.Run(event+"/independent_ttl_buckets", func(t *testing.T) {
					usage := &ClaudeUsage{CacheCreationInputTokens: 3, CacheCreation5mTokens: 1, CacheCreation1hTokens: 2}
					patch := `{"usage":{"cache_creation":{"ephemeral_5m_input_tokens":0}}}`
					if event == "message_start" {
						patch = `{"message":` + patch + `}`
					}
					var obj map[string]any
					require.NoError(t, json.Unmarshal([]byte(patch), &obj))
					obj["type"] = event
					raw, err := json.Marshal(obj)
					require.NoError(t, err)
					parse(string(raw), usage)
					require.Equal(t, &ClaudeUsage{CacheCreationInputTokens: 3, CacheCreation1hTokens: 2}, usage)
				})
			}
			for _, invalid := range []string{"null", "-1", "0.5", `"0"`, "true", "{}", "1e99"} {
				for _, event := range []string{"message_start", "message_delta"} {
					t.Run(event+"/invalid="+invalid, func(t *testing.T) {
						before := ClaudeUsage{InputTokens: 11, OutputTokens: 12, CacheReadInputTokens: 13,
							CacheCreationInputTokens: 30, CacheCreation5mTokens: 14, CacheCreation1hTokens: 16}
						usage := before
						fields := fmt.Sprintf(`{"input_tokens":%s,"output_tokens":%s,"cache_read_input_tokens":%s,"cache_creation_input_tokens":%s,"cache_creation":{"ephemeral_5m_input_tokens":%s,"ephemeral_1h_input_tokens":%s}}`, invalid, invalid, invalid, invalid, invalid, invalid)
						if event == "message_start" {
							parse(`{"type":"message_start","message":{"usage":`+fields+`}}`, &usage)
						} else {
							parse(`{"type":"message_delta","usage":`+fields+`}`, &usage)
						}
						require.Equal(t, before, usage)
					})
				}
			}
		})
	}
}

func TestParseSSEUsageIntRejectsInvalidCounters(t *testing.T) {
	for _, value := range []any{nil, -1, int64(-1), float64(-1), 0.5, "0", true, math.NaN(), math.Inf(1), float64(math.MaxInt), json.Number("0.5"), json.Number("9223372036854775808")} {
		_, ok := parseSSEUsageInt(value)
		require.False(t, ok, "invalid counter: %v", value)
	}
	for _, value := range []any{0, int32(0), int64(0), float32(0), float64(0), json.Number("0"), 17, float64(17)} {
		_, ok := parseSSEUsageInt(value)
		require.True(t, ok, "valid counter: %v", value)
	}
}
