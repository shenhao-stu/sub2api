//go:build unit

package handler

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestModelBudgetHandlersDoNotReplayAndSettleObservedUsageOnce(t *testing.T) {
	for _, endpoint := range []string{"chat/completions", "responses"} {
		for _, metered := range []bool{false, true} {
			name := endpoint + "/unmetered"
			if metered {
				name = endpoint + "/metered"
			}
			t.Run(name, func(t *testing.T) {
				body := `{"error":{"type":"budget_exceeded","message":"Insufficient API key budget: only 0 claude-sonnet-4.5 request(s) can run at once right now. This is not a platform outage or a concurrency setting. Fix: top up, or use a cheaper model."}`
				if metered {
					body += `,"usage":{"input_tokens":12,"output_tokens":7,"cache_read_input_tokens":5,"cache_creation":{"ephemeral_5m_input_tokens":2,"ephemeral_1h_input_tokens":3}}`
				}
				body += "}"
				run := runCompatStreamUsageHandler(t, endpoint, body, true, compatStreamUsageOptions{accountCount: 2, upstreamStatus: 402})
				require.Equal(t, 1, run.upstream.calls)
				require.Equal(t, 402, run.rec.Code)
				require.Contains(t, run.rec.Body.String(), `"code":"upstream_model_budget_exhausted"`)
				require.Contains(t, run.rec.Body.String(), `"status_code":402`)
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
