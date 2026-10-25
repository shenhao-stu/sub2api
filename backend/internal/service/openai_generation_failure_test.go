//go:build unit

package service

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIGenerationFailureReaders(t *testing.T) {
	for _, code := range []string{"upstream_malformed_response", "upstream_malformed_function_call"} {
		for _, path := range []string{"responses_stream", "passthrough_stream", "responses_buffered", "passthrough_buffered", "chat_stream", "chat_buffered"} {
			for _, meter := range []string{"missing", "zero", "positive"} {
				t.Run(code+"/"+path+"/"+meter, func(t *testing.T) {
					usage := ""
					if meter == "zero" {
						usage = `,"usage":{"input_tokens":0,"output_tokens":0}`
					} else if meter == "positive" {
						usage = `,"usage":{"input_tokens":37,"output_tokens":2,"input_tokens_details":{"cached_tokens":11}}`
					}
					body := fmt.Sprintf(`data: {"type":"response.failed","response":{"id":"resp_synthetic","status":"failed","error":{"type":"server_error","code":%q,"status_code":502,"message":"synthetic https://private.invalid/ Bearer synthetic-secret 10.12.34.56","param":"private.invalid"}%s}}`, code, usage) + "\n\n"
					c, rec, resp, svc, account := meteredOpenAITestContext(PlatformOpenAI, body)
					capture := &chatReceiptCapture{}
					if strings.HasPrefix(path, "chat_") {
						c.Request.URL.Path = "/v1/chat/completions"
						c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), chatReceiptContextKey{}, capture))
					}
					got, err := runMeteredOpenAIReader(svc, c, resp, account, path)
					require.Error(t, err)
					var failover *UpstreamFailoverError
					require.NotErrorAs(t, err, &failover)
					wire := rec.Body.String()
					require.Contains(t, wire, `"code":"`+code+`"`)
					require.Contains(t, wire, `"status_code":502`)
					require.NotContains(t, wire, "response.completed")
					for _, secret := range []string{"private.invalid", "synthetic-secret", "10.12.34.56", `"param"`} {
						require.NotContains(t, wire, secret)
					}
					if meter == "positive" {
						require.NotNil(t, got)
						require.Equal(t, 37, got.InputTokens)
						require.Equal(t, 2, got.OutputTokens)
						require.Equal(t, 11, got.CacheReadInputTokens)
					} else if got != nil {
						require.False(t, openAIUsageHasTokens(got))
					}
					if strings.HasPrefix(path, "chat_") {
						if meter == "missing" {
							require.Nil(t, capture.meter, "a generation failure code cannot invent a final meter")
						} else {
							require.NotNil(t, capture.meter, "explicit zero must stay distinguishable from absent usage")
						}
					}
				})
			}
		}
	}
}

func TestOpenAIGenerationFailureExactContract(t *testing.T) {
	base := `{"type":"response.failed","response":{"status":"failed","error":{"type":"server_error","code":"upstream_malformed_response","status_code":502}}}`
	for _, tc := range []struct {
		name, payload string
		match         bool
	}{
		{"known", base, true},
		{"bare_error", `{"type":"error","error":{"code":"upstream_malformed_function_call"}}`, true},
		{"conflicting_envelopes", strings.Replace(base, `"response":`, `"error":{"code":"rate_limit_exceeded"},"response":`, 1), false},
		{"not_terminal", strings.Replace(base, `"type":"response.failed"`, `"type":"response.completed"`, 1), false},
		{"wrong_state", strings.Replace(base, `"status":"failed"`, `"status":"completed"`, 1), false},
		{"unknown_code", strings.Replace(base, `"upstream_malformed_response"`, `"server_error"`, 1), false},
		{"prefix_code", strings.Replace(base, `"upstream_malformed_response"`, `"upstream_malformed_response_extra"`, 1), false},
		{"message_only", `{"type":"response.failed","response":{"error":{"message":"upstream_malformed_response"}}}`, false},
		{"duplicate_code", strings.Replace(base, `"code":"upstream_malformed_response"`, `"code":"server_error","code":"upstream_malformed_response"`, 1), false},
		{"null_code", strings.Replace(base, `"code":"upstream_malformed_response"`, `"code":null`, 1), false},
		{"auth_type", strings.Replace(base, `"type":"server_error"`, `"type":"authentication_error"`, 1), false},
		{"conflict_429", strings.Replace(base, `"status_code":502`, `"status_code":429`, 1), false},
		{"conflict_503", strings.Replace(base, `"status_code":502`, `"status_code":503`, 1), false},
		{"conflict_403", strings.Replace(base, `"status_code":502`, `"status_code":403`, 1), false},
		{"string_status", strings.Replace(base, `"status_code":502`, `"status_code":"502"`, 1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, ok := openAIGenerationFailureFields([]byte(tc.payload))
			require.Equal(t, tc.match, ok)
			if ok {
				require.Equal(t, 502, f.StatusCode)
				require.False(t, openAIStreamFailedEventShouldFailover([]byte(tc.payload), "synthetic"))
				normalized := normalizeOpenAIGenerationFailure([]byte(tc.payload))
				require.Equal(t, f.Code, openAIStreamFailedEventErrorCode(normalized))
			}
		})
	}
	for _, code := range []string{"server_error", "server_is_overloaded", "rate_limit_exceeded"} {
		payload := strings.Replace(base, "upstream_malformed_response", code, 1)
		require.True(t, openAIStreamFailedEventShouldFailover([]byte(payload), "synthetic"), code)
	}
	conflict := strings.Replace(base, `"status_code":502`, `"status_code":429`, 1)
	require.Equal(t, 429, openAIStreamFailureStatus([]byte(conflict), "synthetic"))
	require.True(t, openAIStreamFailedEventShouldFailover([]byte(conflict), "synthetic"))
	// A public envelope cannot inherit an upstream diagnostic parameter.
	normalized := normalizeOpenAIGenerationFailure([]byte(strings.Replace(base, `"status_code":502`, `"status_code":502,"param":"private.invalid"`, 1)))
	require.False(t, gjson.GetBytes(normalized, "response.error.param").Exists())
}
