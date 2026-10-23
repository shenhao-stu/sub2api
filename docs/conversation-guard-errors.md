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

Provider policy refusals with HTTP 403 and exact `error.type=content_refusal`
or `error.code=content_policy_violation` also stop retries and account failover.
This classification does not imply a customer violated policy; device and
conversation blocks can have the same external shape. Ordinary credential
errors keep their recovery path. A refusal carrying observed usage still
settles that usage, including cache creation TTL counters.

Client error messages redact upstream URLs, including protocol-relative URLs
and bare private-provider hostnames. Preserve status, type and field paths.
Raw restricted diagnostics remain private; do not publish request bodies,
provider credentials or source addresses in customer errors.
