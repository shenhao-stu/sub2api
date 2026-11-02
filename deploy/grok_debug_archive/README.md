# Scoped Grok test request archive

The producer is default-off. Select trusted group IDs with SUB2API_REQUEST_CAPTURE_GROUP_IDS, a private persistent directory with SUB2API_REQUEST_CAPTURE_DIR, and configure MIN_FREE_BYTES and MAX_CONCURRENT with the same prefix. Capture starts after basic API-key authentication and before billing. It retains raw inbound entity bytes before dispatch and replays those same bytes once; auth/cookie headers, responses and WebSocket messages are excluded.

Payloads are gzip-compressed immediately; input manifests become ready after fsync/hash verification. Independent outcome files appear after the handler ends. Partial input is explicit and never dispatched. Storage failure rejects only selected groups before dispatch.

The reviewed source helper is deliberately restricted to this deployment's group78 and exact private directory. It lists only immutable validated objects, acknowledges independent durable input/outcome receipts, and removes source objects older than24h only after both acknowledgments and a terminal outcome. ACK tombstones remain for unknown-response reconciliation. No old debug files or production ledgers are in scope.

AgentNAS pulls through its own existing SSH alias and verifies both compressed and decompressed hashes before ACK. Local intents are reconciled read-only after unknown ACK results, including after source garbage collection. The NAS retains completed, twice-acknowledged captures for30 days, preserves unresolved objects, and enforces explicit time/space limits. The example supports the server's256MiB raw limit, gzip overhead up to512MiB, a512GiB namespace cap and100GiB NAS free-space reserve. Source minimum reserve is20GiB. Reaching a limit is reported; data is not silently discarded.

Install the scripts and configuration privately, review paths/ownership/SSH trust, manually verify one real cross-host archive and both ACKs, then enable the prepared300s launchd schedule. The template is not an installation command. Existing backup jobs and service tunnels must remain unchanged. See HELPER-PROTOCOL.md for the wire contract.

Run `python3 -m unittest -v test_nas_archive.py test_source_helper.py` in this directory. Synthetic test data contains no customer requests.
