# Grok Free account guard

`grok_free_guard.py` replaces the existing group-78-only guard after the operator authorizes fleet-wide Free suspension. It reads the existing Sub2API OAuth tier, requires exact `free`, and uses the native admin API to stop new scheduling. It never enables accounts or alters paid/unknown/API-key accounts. A shared account is also removed from group 78; a sole group binding is retained while paused. No credentials or client request data are logged.

The existing `grok-test-free-guard.timer` keeps its five-minute schedule. Install the script atomically at its existing `ExecStart` path only after saving the old file and a non-secret account-state snapshot. Run without `--apply` for the plan, then use the same service for reconciliation. No application image restart is required. Preserve the service's loopback-only network and filesystem restrictions. Restoration of the old script does not automatically re-enable stopped accounts.

The local helper and its hash are deployment-specific dependencies. The guard aborts if the helper changes. It rechecks each account before mutation, verifies the result, reads back uncertain writes without retrying, and reports any remaining candidates. OAuth refresh remains responsible for discovering the provider plan; the five-minute guard consumes that evidence and does not call inference endpoints.

Validation:

```sh
python3 -m unittest discover -s deploy/ops -p 'test_grok_free_guard.py'
cd backend
go test -tags unit ./internal/handler -run '^TestGrokFreeQuotaOperatorRule$'
```

`grok_free_quota_error_rule.json` is an optional native error-passthrough configuration. It preserves HTTP 429 and Ops, matches only the included-Free-usage-exhaustion phrase, and returns an actionable message without raw provider URLs. The `openai` platform is included because the existing Responses handler uses that dispatch label for Grok-compatible requests. Apply through the native admin API after checking for an existing rule by name; a timed-out create must be read back, never blindly repeated. Removing or disabling only this rule restores the previous generic message.
