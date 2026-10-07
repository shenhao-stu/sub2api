# Getoken 0.2.14+g7

Merged official v0.2.14 (`0363b8cdb`) into main `6bb9dac07`. The only merge conflict was the version file, now `0.2.14+g7`. Build from this source with `BuildType=custom`; official binary replacement remains disabled.

The upstream fixes fresh-install admin credential generation and login validation, and rejects EasyPay order-signature reuse as a forged payment notification. Return URLs discard client query parameters, callbacks reject nonstandard fields, and valid signed notifications still work. Existing admin records are not recreated. No new database migrations are introduced.

Vue and source-map-js security updates and Codex remote API-key catalog discovery are included. XLSX advisory exceptions remain scoped to lazy, write-only administrator export; this is not a claim that the dependency is vulnerability-free.

Grok behavior is retained from deployed g6: official CLI metadata sync, no-downgrade compare-and-set, explicit outdated-version rejection retry, Heavy-only admission, quota-aware cooldowns and client-error classification remain. The prior comment-only SSE stall limitation is not solved by this upstream tag. CommandCode, native Jev/WARP routing, image origin routing, soft-deleted-key settlement, billing deduplication and WebSocket per-turn revalidation are retained. Do not infer paid CommandCode validation without a subscription.

## Production backup safety

The existing daily job staged full PostgreSQL dumps under `/tmp`, a tmpfs on this host. Kernel OOM records on October 6 and 7 followed its 04:17 server-local start; Sub2API was killed, and WARP was also affected on October 7. Move staging to the existing disk-backed backup directory, reject memory filesystems, use a single-job lock and private permissions. `deploy/getoken-daily-backup.sh` preserves the existing logical snapshots, integrity check, Borg retention and archive format. Compression uses zstd level 6 instead of 19: the initial level-19 run processed only about 1 GB in ten minutes on this host, so extreme compression needlessly extended the backup window. It updates an existing schedule, not a new monitor.

Deploy an isolated candidate with exact source/binary/image checks, verify anonymous requests remain rejected, switch routing, then drain the prior instance before natural termination. Preserve running CRS (a different service that also names its executable sub2api), Getoken, C2A, PostgreSQL and Redis. Keep the previous image and configuration for rollback; never restore a stale financial database over new traffic.

## Lint follow-up

CI exposed 34 existing diagnostics in the custom overlay. Resource cleanup now explicitly discards non-actionable close errors, internal type assertions fail closed, equivalent boolean conditions and connection-state branches use the configured style, and a test-only compact conversion wrapper moved into the unit test file. Two HTTP integration tests retain their real repository transport with a narrowly documented test-only depguard exemption. Production dependency rules and security checks remain enabled. These changes preserve lifecycle, scheduling and billing conditions and are covered by the affected package regressions.
