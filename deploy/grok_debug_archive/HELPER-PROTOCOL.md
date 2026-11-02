# Netcup Grok test archive helper protocol (v1/v2 inputs)

The NAS client invokes the existing `netcup` SSH alias with strict host-key checking, no agent forwarding, and an absolute helper path supplied by root. The helper must operate only the new debug directory and exact configured test group. Source account/configuration data are outside this protocol.

## Read-only list

`HELPER manifest-list --group-id G --max-objects N --max-bytes B`

Return a JSON object no larger than 2 MiB:

```json
{"schema_version":1,"group_id":123,"items":[{"capture_id":"11111111-1111-4111-8111-111111111111","input":{"path":"11111111-1111-4111-8111-111111111111.input.json","bytes":1024,"sha256":"64 lowercase hex"},"payload":{"path":"11111111-1111-4111-8111-111111111111.request.gz","bytes":500,"sha256":"64 lowercase hex"},"outcome":null}]}
```

`outcome`, when present, is the same descriptor shape with `.outcome.json`. An input manifest is the producer's ready marker: never list a `.partial` or a payload lacking a valid input. Enforce no symlinks, UUID file names, matching capture/group IDs, descriptor size/hash, and the client's supplied bounds. List captures whose input ACK already exists too, so late outcomes can be delivered. Prefer pending ACK/outcome objects before fully acknowledged captures; do not starve later captures behind the same N acknowledged items. The helper needs a bounded stable cursor or pending-first selection when there are more than N files.

The listing envelope remains schema1. A schema2 input adds exactly seven fields to the existing input: `headers_file`, `headers_raw_bytes`, `headers_raw_sha256`, `headers_compressed_bytes`, `headers_compressed_sha256`, `headers_policy`, `headers_redacted_count`. Its listing item additionally contains `headers: {"path":"UUID.headers.gz","bytes":N,"sha256":"64 lowercase hex"}`. A schema1 item has no headers descriptor. The exact four fetch suffixes are `.input.json`, `.request.gz`, `.headers.gz`, `.outcome.json`; this does not permit arbitrary gzip or partial paths. Header compressed bytes count against the same total listing, transfer, namespace, and source GC budgets, while the per-run object limit still counts captures.

Metadata remains at most 64 KiB. Header gzip is at most 16 MiB, decoding to at most 8 MiB of UTF-8 JSON, including the encoder's final newline. The JSON has exactly `schema_version:1`, `policy:"credential_names_v1"`, `headers` (canonical ASCII HTTP token names to string arrays), and `redacted_count`. Preserve repeated noncredential values, HTAB, and parsed Unicode/obs-text; CR/LF/NUL and invalid HTTP controls are rejected. Header name/value/count bounds derive from the 8 MiB encoded total, without a smaller arbitrary per-name limit.

For policy1, split lowercased ASCII names on every non-`[a-z0-9]` character and match exact components: `authorization`, `cookie`, `key`, `apikey`, `token`, `secret`, `password`, `credential`, `signature`, `auth`, `authentication`, `jwt`, `session`, `csrf`, `xsrf`, `credentials`. Each matching name must have exactly `["[REDACTED]"]`; `redacted_count` counts matching names, not values, and must agree with the input manifest. `X-Codex-Turn-Metadata`, `X-Tokenize`, `Secretariat`, and `Monkey` are not matches. This name policy does not classify arbitrary text in other values.

The NAS fetches only those exact validated descriptor paths through OpenSSH `scp` (its default SFTP transport), under the root-supplied absolute `source_dir`. No helper command accepts arbitrary paths or account IDs. Payload is fetched and archived immediately after input publication; no outcome is required.

## ACK mutation

`HELPER ack --group-id G --capture-id UUID --kind input|outcome --object-sha256 SHA --receipt-sha256 SHA`

For `input`, object SHA is the immutable input JSON SHA. For `outcome`, it is the outcome JSON SHA. The NAS issues ACK only after payload/manifest verification and durable local publication plus a durable local receipt. The helper must match the current source object and group, record an immutable acknowledgment atomically with fsync, and reject conflicting ACKs. An identical ACK may be idempotent at the helper, but the client never blindly retries a mutation with an uncertain outcome.

For schema2, both body and headers must pass compressed SHA/bytes, full gzip decoding/raw SHA/bytes, and the header policy before a new input ACK. The immutable schema2 input receipt repeats the seven header fields, including both hashes and byte counts. Existing schema1 receipts and ACK records are unchanged and never rewritten. Outcome remains a separate ACK kind; no third headers ACK is introduced. Header rejection codes are fixed codes only. NAS source rejection summaries expose an allowlisted header reason/count and suppress all other remote strings.

Return:

```json
{"schema_version":1,"group_id":123,"capture_id":"11111111-1111-4111-8111-111111111111","kind":"input","object_sha256":"64 lowercase hex","receipt_sha256":"64 lowercase hex","accepted":true}
```

## Read-only ACK reconciliation

`HELPER ack-status --group-id G --capture-id UUID --kind input|outcome`

Return the exact acknowledgment above when present; otherwise return `{"schema_version":1,"accepted":false}`. After timeout/disconnect/nonzero/invalid ACK response, later runs may only query this read-only status. A matching accepted ACK reconciles the local intent; absent/conflicting status remains pending for explicit operator review. Never replay `ack` automatically.

## Source retention, implemented by root's helper

Preserve source compressed input for at least 24 hours from server `captured_at`, and until an exact input ACK exists. Never remove `.partial`, unacknowledged input, or active requests. If an outcome exists, retain it and the capture until the outcome ACK also exists. If no final outcome exists, preserve the capture as possibly active. The producer writes outcome only after the entire downstream handler returns: outcome existence is the terminal marker. `request_released=false` is a legitimate terminal partial/canceled/oversized request and must not prevent acknowledged retention cleanup. Failures preserve source and pending state; cleanup must not reach outside this new group/directory. The NAS applies its separate 30-day retention only to its own verified, acknowledged capture directories.

Schema2 GC includes exactly the manifest-bound `.headers.gz`, with regular/private/single-link checks and source identity rechecks before unlink. Unknown files and any `.partial` block deletion. A `.headers.gz` next to a schema1 input is unknown and is preserved. NAS retention rechecks the exact receipt binding and both artifacts before deleting an eligible schema2 directory. The external root ACK namespace, lock, durability barriers and retained tombstones are unchanged.
