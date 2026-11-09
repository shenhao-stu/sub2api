# Scoped Grok test request archive

The producer is default-off. Select trusted group IDs with SUB2API_REQUEST_CAPTURE_GROUP_IDS, a private persistent directory with SUB2API_REQUEST_CAPTURE_DIR, and configure MIN_FREE_BYTES and MAX_CONCURRENT with the same prefix. Capture starts after basic API-key authentication and before billing. It retains raw inbound entity bytes before dispatch and replays those same bytes once. Schema2 also retains all parsed HTTP header names and repeated values in an immediately compressed sidecar; credential-like names retain only the exact `[REDACTED]` marker. Responses and WebSocket messages are excluded.

MAX_CONCURRENT limits simultaneous input writers (1–16). When writers are occupied, up to 512 authenticated requests may wait for at most 90 seconds. The queue leaves bodies unread and clients can cancel. A full or expired queue returns HTTP 429 with Retry-After before dispatch. Writers are released as soon as input is sealed and reopened for the handler, including for long-lived streaming responses.

Body and schema2 header artifacts are gzip-compressed immediately; input manifests become ready after fsync/hash verification of both. Independent outcome files appear after the handler ends. Partial input is explicit and never dispatched. Storage failure rejects only selected groups before dispatch. Parsed headers include Go's special Host, Transfer-Encoding and sent Content-Length fields; original wire ordering/casing is not retained. Header JSON has an 8MiB raw/16MiB compressed hard limit, independent of the 64KiB input metadata limit, without truncation.

The reviewed source helper is deliberately restricted to this deployment's group78 and exact private directory. It lists only immutable validated objects, acknowledges independent durable input/outcome receipts, and removes source objects older than24h only after both acknowledgments and a terminal outcome. ACK tombstones remain for unknown-response reconciliation. No old debug files or production ledgers are in scope.

AgentNAS pulls through its own existing SSH alias and verifies both compressed and decompressed hashes before ACK. Local intents are reconciled read-only after unknown ACK results, including after source garbage collection. The NAS retains completed, twice-acknowledged captures for30 days, preserves unresolved objects, and enforces explicit time/space limits. The example supports the server's256MiB raw limit, gzip overhead up to512MiB, a512GiB namespace cap and100GiB NAS free-space reserve. Source minimum reserve is20GiB. Reaching a limit is reported; data is not silently discarded.

The readers accept both schema1 body-only and schema2 body-plus-headers inputs. Input ACK requires both schema2 artifacts and the credential-name policy to validate before durable receipt publication; the input receipt explicitly binds header hashes/bytes. Existing schema1 receipts remain byte-identical, and outcome ACK stays independent. Header bytes share the existing run/namespace budgets. Unknown sidecars, partial artifacts, active requests and unacknowledged captures remain protected from GC. No legacy ACK state is adopted.

Install the scripts and configuration privately, review paths/ownership/SSH trust, manually verify one real cross-host archive and both ACKs, then enable the prepared300s launchd schedule. The template is not an installation command. Existing backup jobs and service tunnels must remain unchanged. See HELPER-PROTOCOL.md for the wire contract.

For an existing live installation, replace compatible source and NAS readers first, using exact old hashes, private modes and backups, then deploy the schema2 producer. An already running old NAS process may finish its schema1 work; encountering schema2 fails closed. This source change does not install scripts, change ACK state/configuration, reload launchd, or broaden retention.

Run `python3 -m unittest -v test_nas_archive.py test_source_helper.py` in this directory. Synthetic test data contains no customer requests.

ACK state is stored in the fixed root-owned helper installation directory, outside the producer data bind. Backend startup may recursively repair ownership inside `/app/data`; it cannot change the authoritative ACK receipts. Legacy in-data state is never adopted automatically. A lock-only legacy directory may be preserved during an explicitly reviewed helper migration; any real legacy ACK requires separate reconciliation before switching.

Continuous mode is explicitly opt-in with `--continuous`. It replaces the old five-minute start interval for an authorized existing service, with progress-driven bounded batches and idle/error backoff. Use the updated exact-CID source cleanup after confirmed terminal archives; preserve the NAS 30-day policy. See the protocol's continuous backlog recovery section for the updated ACK and local-retention rules.

For an existing continuous job whose complete namespace scan consumes most of its
work window, `max_seconds` may be explicitly configured up to 1200 (the default
remains 240). Longer rounds retain the 45-second cleanup reserve and each control
RPC still has a 45-second maximum; object, byte, worker, free-space and namespace
limits do not change. Every round still scans the whole namespace before admission.
`namespace_scan_seconds` and `elapsed_seconds` report the completed scan and the
round duration before publishing its final record. This amortizes a slow scan over
more of the existing bounded batch; it does not guarantee capacity for an unlimited
arrival rate or a scan that exceeds the new window.

`max_transfer_batch_bytes` defaults to 8 MiB and may be reduced to 64 KiB–8 MiB
for a slow direct link. Each batch still contains at most 128 captures; a capture
larger than the byte target stays whole in its own batch. Bulk transfers have a
maximum 180-second window and preserve the subsequent ACK and cleanup windows.
For example, 1 MiB batches can finish on a 40 KiB/s link where 8 MiB batches
time out. This setting neither enables a proxy fallback nor changes validation,
retention, single-object size limits or the total per-run byte budget.

`ack_batch_deferred_time_limit` means fewer than 45 work seconds remain before
invoking the ACK transport. The complete 45-second ACK window must fit without
using the cleanup reserve or extending the deadline. Durable intents and source
files remain pending. An exception
after transport invocation remains `ack_batch_outcome_unknown` and cannot authorize
source cleanup without confirmed ACKs. Apply configuration changes only through a
separately reviewed installation at the job's lock boundary; this source change
does not change a running job, launchd timer, or production configuration. Validate
at least one completed new round with scan/elapsed timings, confirmed ACKs and
verified source-GC counters after installation.

`max_pages_per_run` defaults to 1 and may explicitly be set to 2. Both pages share
one complete namespace scan, the same lock and hard deadline, and one total byte
budget. Each source page and exact-ID GC call still has at most `max_objects`
captures (up to 1000); ACK batches, transfer batches and workers are unchanged.
The two-page mode also reserves 512 KiB per admitted capture for receipt/ACK
metadata, in addition to payload admission, against namespace and free-space
limits. This reservation is conservative and never credited back within the run.

Page two is admitted only after page one completes without errors, pending ACKs
or incomplete GC, and the remaining work window is at least the larger of 90
seconds and the first page's measured duration plus 45 seconds. Already admitted
UUIDs are excluded, and both pages' descriptor bytes must fit the one run limit.
An unfinished page does not advance its cursor in this mode. No namespace count
is reused across runs, no ACK or uncertain GC is retried within a run, and terminal
dual-ACK/hash checks and 30-day retention remain unchanged. `pages_completed`,
`page_duplicates_skipped` and `next_page_deferred` explain page progress; an
unconfirmed second-page operation preserves the first page's confirmed counters.
Install compatible code before adding the new config field, at the existing lock
boundary, and retain the exact previous code/config for rollback.
