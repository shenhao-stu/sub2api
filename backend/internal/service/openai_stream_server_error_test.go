//go:build unit

package service

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenAIStreamExplicitServerErrorsBeforeOutput(t *testing.T) {
	for _, payload := range []string{
		`{"type":"error","code":null,"message":"Internal error during token generation","sequence_number":4682}`,
		`{"type":"error","error":{"code":"server_error","message":"backend failed"}}`,
		`{"type":"error","code":"internal_error","message":"backend failed"}`,
		`{"type":"error","error":{"status":503,"message":"backend failed"}}`,
	} {
		t.Run(payload, func(t *testing.T) {
			result, err, recorder := forwardGrokBillingStream(t, "data: "+payload+"\n\n")
			var failover *UpstreamFailoverError
			require.ErrorAs(t, err, &failover)
			require.Nil(t, result)
			require.Empty(t, recorder.Body.String())
		})
	}
}

func TestOpenAIStreamServerErrorDoesNotReplayOutputOrUsage(t *testing.T) {
	const failure = `{"type":"error","code":null,"message":"Internal error during token generation","sequence_number":4682}`
	for _, prefix := range []string{
		`{"type":"response.output_text.delta","delta":"already delivered"}`,
		`{"type":"response.in_progress","response":{"usage":{"input_tokens":12,"output_tokens":3}}}`,
	} {
		t.Run(prefix, func(t *testing.T) {
			result, err, _ := forwardGrokBillingStream(t, "data: "+prefix+"\n\ndata: "+failure+"\n\n")
			require.Error(t, err)
			var failover *UpstreamFailoverError
			require.False(t, errors.As(err, &failover))
			require.NotNil(t, result)
		})
	}
}

func TestOpenAIStreamServerErrorNeverOverridesRequestDenials(t *testing.T) {
	for _, payload := range []string{
		`{"type":"error","code":"content_policy_violation","message":"Internal error during token generation"}`,
		`{"type":"error","error":{"type":"invalid_request_error","code":"server_error","message":"bad input"}}`,
		`{"type":"error","code":"server_error","message":"Request violates safety policy"}`,
		`{"type":"error","message":"unclassified failure"}`,
	} {
		t.Run(payload, func(t *testing.T) {
			require.False(t, openAIStreamErrorEventShouldFailover([]byte(payload), extractOpenAISSEErrorMessage([]byte(payload))))
		})
	}
}
