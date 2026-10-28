# Rolling ingress for the customized fork

This is a deployment mechanism, not general multi-instance support. Freeze a
release manifest containing both live container IDs/images, the reviewed target
commit, artifact hashes, persistent mount identity and migration ledger. Recheck
BBS ownership and hold the existing host release lock before each mutation.

Before any overlap, prove all of the following from current production state:
- no enabled scheduled paid test or active v1 channel monitor;
- no platform quota rows, and the optional snapshot flusher is disabled;
- no active plugin installation or binding;
- identical migrations and shared receipt-journal directory/lock inode/UID;
- transactional billing dedup, journal flock and peer concurrency preservation
  are tested against PostgreSQL/Redis; sufficient disk and memory remain.

The helper fails closed if any supplied gate is unsupported or stale. It does
not collect those proofs for the operator and must not be treated as a complete
release command. Never disable customer features merely to satisfy a gate.

1. Start the candidate on private addresses, without canonical service aliases.
   Start a secondary ingress pointing to the old backend. Validate candidate
   health, migrations, journal, bounded new synthetic requests and accounting.
2. Reload only this secondary ingress to the candidate. Redirect new connections
   to the old service addresses through it using the dedicated transition NAT
   table. Existing conntrack entries and requests remain on the old process.
3. Verify new admission, then send native SIGTERM to the old process. Keep the
   candidate serving throughout the native drain and settlement cleanup. Do not
   use Docker's short stop timeout or force kill. Preserve shutdown evidence.
4. Once the old process has exited cleanly, detach its network endpoints and
   retain it for rollback. Start the primary ingress on its former addresses and
   published port. Validate it before removing the transition table. Keep the
   secondary available for DNS/connection caches, with the same target as primary.
5. Preserve both ingress configurations and their images. Future releases
   switch ingress targets before draining the old backend. Rollback changes
   only the backend target after the old build is healthy; never restore a DB.

All ingress paths disable proxy retries, including POSTs with Idempotency-Key,
and retain a 22-minute grace for upgraded streams on configuration reload. Do
not globally reload unrelated public Caddy routes unless their existing
WebSocket close grace and active connections have also been reviewed.

`rolling_fixture` and the isolated runtime test exercise old/new admission,
long JSON, SSE, WebSocket, exact 503, ambiguous EOF single-dispatch and rollback.
The production controller and private manifests stay with the operator's release
evidence because they contain live infrastructure identities and credentials.

# Optional Anthropic buffering

`account.extra.anthropic_buffered_stream_models` is an exact list of upstream
model IDs. Only nonstream Anthropic API-key passthrough requests matching the
list use an upstream stream and return one assembled JSON message. The client
protocol, cancellation policy and billing mode remain unchanged. The default
is disabled. Do not enable it globally to hide an unidentified transport fault.

Text, tool JSON, thinking signatures, citations and raw usage are retained.
Incomplete/invalid events produce an error and are never redispatched. Only
observed counters survive a failed response; absent final counters remain
unknown. Unknown content deltas fail explicitly instead of silently corrupting
an assembled message. Existing client streaming requests are unchanged.
