//go:build unit

package handler

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSessionAdmissionHandlersDoNotReplayAndSettleObservedUsageOnce(t *testing.T) {
	for _, endpoint := range []string{"chat/completions", "responses"} {
		for _, metered := range []bool{false, true} {
			name := endpoint + "/unmetered"
			if metered {
				name = endpoint + "/metered"
			}
			t.Run(name, func(t *testing.T) {
				body := `{"error":{"type":"upstream_error","message":"This group only accepts sessions first seen here."}`
				if metered {
					body += `,"usage":{"input_tokens":12,"output_tokens":7,"cache_read_input_tokens":5,"cache_creation":{"ephemeral_5m_input_tokens":2,"ephemeral_1h_input_tokens":3}}`
				}
				body += "}"
				run := runCompatStreamUsageHandler(t, endpoint, body, true, compatStreamUsageOptions{accountCount: 2, upstreamStatus: 429})
				require.Equal(t, 1, run.upstream.calls)
				require.Equal(t, 429, run.rec.Code)
				require.Contains(t, run.rec.Body.String(), `"code":"upstream_session_not_accepted"`)
				require.Contains(t, run.rec.Body.String(), `"status_code":429`)
				run.billingRepo.mu.Lock()
				defer run.billingRepo.mu.Unlock()
				if !metered {
					require.Empty(t, run.billingRepo.commands)
					require.Empty(t, run.usageRepo.created)
					return
				}
				require.Len(t, run.billingRepo.commands, 1)
				require.Len(t, run.usageRepo.created, 1)
				cmd := run.billingRepo.commands[0]
				require.Equal(t, 12, cmd.InputTokens)
				require.Equal(t, 7, cmd.OutputTokens)
				require.Equal(t, 5, cmd.CacheReadTokens)
				require.Equal(t, 5, cmd.CacheCreationTokens)
			})
		}
	}
}
