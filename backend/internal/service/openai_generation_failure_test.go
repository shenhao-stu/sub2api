//go:build unit

package service

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIGenerationFailureReaders(t *testing.T) {
	for _, code := range []string{"upstream_malformed_response", "upstream_malformed_function_call", "upstream_empty_completion"} {
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
					if meter == "zero" {
						require.Contains(t, wire, `"input_tokens":0`)
						require.Contains(t, wire, `"output_tokens":0`)
					}
					if meter == "missing" {
						require.NotContains(t, wire, `"usage":`)
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
		{"nested_response_status", strings.Replace(base, `"response":{`, `"response":{"status_code":429,`, 1), false},
		{"direct_json", `{"object":"response","status":"failed","error":{"code":"upstream_malformed_response","status_code":502}}`, true},
		{"direct_missing_object", `{"status":"failed","error":{"code":"upstream_malformed_response"}}`, false},
		{"direct_wrong_state", `{"object":"response","status":"completed","error":{"code":"upstream_malformed_response"}}`, false},
		{"casefold_error_status", strings.Replace(base, `"status_code":502`, `"STATUS_CODE":429`, 1), false},
		{"casefold_outer_status", strings.Replace(base, `"response":`, `"STATUS_CODE":403,"response":`, 1), false},
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

func TestOpenAIGenerationFailureJSONReaders(t *testing.T) {
	for _, code := range []string{"upstream_malformed_response", "upstream_malformed_function_call", "upstream_empty_completion"} {
		for _, reader := range []string{"responses_buffered", "passthrough_buffered"} {
			for _, meter := range []string{"missing", "incomplete_zero", "zero", "positive"} {
				t.Run(code+"/"+reader+"/"+meter, func(t *testing.T) {
					usage := ""
					switch meter {
					case "incomplete_zero":
						usage = `,"usage":{"input_tokens":0}`
					case "zero":
						usage = `,"usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}`
					case "positive":
						usage = `,"usage":{"input_tokens":37,"output_tokens":2,"total_tokens":39,"input_tokens_details":{"cached_tokens":11}}`
					}
					body := fmt.Sprintf(`{"object":"response","status":"failed","error":{"code":%q,"type":"server_error","status_code":502,"message":"https://private.invalid/ Bearer synthetic-secret 10.12.34.56","param":"private.invalid"}%s}`, code, usage)
					c, rec, resp, svc, account := meteredOpenAITestContext(PlatformOpenAI, "")
					resp.Header.Set("Content-Type", "application/json")
					resp.Body = io.NopCloser(strings.NewReader(body))
					got, err := runMeteredOpenAIReader(svc, c, resp, account, reader)
					require.Error(t, err)
					var retry *UpstreamFailoverError
					require.NotErrorAs(t, err, &retry)
					require.Equal(t, 502, rec.Code)
					wire := rec.Body.String()
					require.Equal(t, code, gjson.Get(wire, "error.code").String())
					require.Equal(t, int64(502), gjson.Get(wire, "error.status_code").Int())
					for _, secret := range []string{"private.invalid", "synthetic-secret", "10.12.34.56", `"param"`} {
						require.NotContains(t, wire, secret)
					}
					switch meter {
					case "missing", "incomplete_zero":
						require.Nil(t, got)
						require.False(t, gjson.Get(wire, "usage").Exists())
					case "zero":
						require.NotNil(t, got)
						require.False(t, openAIUsageHasTokens(got))
						require.True(t, gjson.Get(wire, "usage.input_tokens").Exists())
						require.True(t, gjson.Get(wire, "usage.output_tokens").Exists())
					case "positive":
						require.NotNil(t, got)
						require.Equal(t, 37, got.InputTokens)
						require.Equal(t, 2, got.OutputTokens)
						require.Equal(t, 11, got.CacheReadInputTokens)
					}
				})
			}
		}
	}
}

func TestOpenAIGenerationFailureZeroEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, usage string
		known       bool
	}{
		{"complete", `{"input_tokens":0,"output_tokens":0}`, true},
		{"missing_output", `{"input_tokens":0}`, false},
		{"missing_input", `{"output_tokens":0}`, false},
		{"empty", `{}`, false},
		{"null", `null`, false},
		{"duplicate", `{"input_tokens":0,"input_tokens":0,"output_tokens":0}`, false},
		{"string", `{"input_tokens":"0","output_tokens":0}`, false},
		{"negative", `{"input_tokens":-1,"output_tokens":0}`, false},
		{"invalid_total", `{"input_tokens":0,"output_tokens":0,"total_tokens":-1}`, false},
		{"invalid_cache", `{"input_tokens":0,"output_tokens":0,"input_tokens_details":{"cached_tokens":-1}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := []byte(`{"object":"response","status":"failed","error":{"code":"upstream_malformed_response"},"usage":` + tc.usage + `}`)
			got := openAIGenerationFailureUsage(payload)
			require.Equal(t, tc.known, got != nil)
		})
	}
}
