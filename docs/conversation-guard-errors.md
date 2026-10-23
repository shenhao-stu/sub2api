# Conversation admission errors

Exact `conversation_*` code/status pairs from a pinned upstream conversation are
terminal for the current request. Stop both same-account retries and account
failover without cooling or penalizing the aggregator account. Other sessions
on that account remain independently eligible. Ordinary concurrency 429 and
overload 503 retain their existing retry policy.

The JSON response retains the original 403, 429 or 503 with bounded, redacted
type/code/message fields. After a heartbeat has committed headers, the protocol
terminal carries the same logical status; it does not fabricate usage. Existing
metered-error accounting runs before this classifier and remains authoritative.

Release after the Getoken request-rejection classifier so the outer layer cannot
replay these errors. Preserve C2A owner affinity and administrator stops. Use
native single-writer drain and retain the old image for rollback. No database,
pricing, group, quota or retry-count configuration migration is required.
