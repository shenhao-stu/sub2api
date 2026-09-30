# Getoken v0.2.11+g1

This build integrates [official v0.2.11](https://github.com/Wei-Shaw/sub2api/releases/tag/v0.2.11), commit `96f4c115c9749078f90cbf210a01d39baf3f53b6`, onto the production fork `f9335ecc566d24188d5570fc02bda296f92a0060`.

The previous official baseline is `a60a29549f488a854966aaec9541abbe006cac22`, already preserved as a parent of `1581749`. Historical merges produce three merge bases. Integration therefore uses the complete 89-file upstream delta from that explicit baseline, applied with Git's three-way patch handling, and retains the official commit as the second merge parent. Reversing the upstream delta in a temporary index restores the exact production tree `353b41f4331a444ae17d6181bc9101991a2eaf61`.

## Changes

- Official inflight balance reservations, GPT-6.1 Sol support, Astra Ultrafast pricing, Claude reset-credit redemption, API-key creation limits, subscription labels, Codex remote catalogs and Claude Code fallback routing.
- Short version `0.2.11+g1`: `g` identifies Getoken and the suffix numbers local revisions. Semver build metadata does not falsely make this an older upstream version. The sidebar constrains long badges; tooltip and details preserve the complete value.
- Regression coverage combines reservation ownership with mandatory billing after client cancellation, including queued, stopped and overflowing workers. Grok quota tests now wait for background refresh completion instead of overwriting a live `sync.Map`.

## Preserved invariants

| Area | Required behavior | Verification |
| --- | --- | --- |
| Billing admission | Unpriced forwarded models are rejected before paid upstream work; supplier health is not penalized for local pricing errors | Existing pricing-admission tests and gateway tests |
| Interrupted requests | Observed usage survives cancellation, partial stream failure and pre-content deadlines; mandatory billing survives worker overflow/shutdown | Gateway/service regressions plus `TestMandatoryBillingRetainsInflightReservationAfterCancellation` |
| Async video | Accepted jobs retain durable owner/price receipts; deleted keys can settle authorized work; duplicate polling cannot double-charge or reprice settled jobs | Isolated PostgreSQL `TestR251*SQL` |
| WebSocket | Each turn retains its own identity and validates the actual ingress model price | Existing WebSocket ingress/billing regressions |
| Grok | Model-scoped cooldowns, true quota reset boundaries, Fast alias/default scheduling and explicit exhausted-account stops remain | Grok scheduler/quota tests and live configuration fingerprints |
| Command Code | Provider/Go credentials, quota handling, billing and Go-first model choices remain | Command Code service/handler/component tests; relevant implementation unchanged |
| Custom updates | Official binary replacement and official rollback remain prohibited server-side for custom builds | Update-service and VersionBadge tests |
| Deployment | 540-second application drain plus bounded cleanup fits the existing 600-second container allowance | Source hashes and guarded deployment preflight |

The 311 migration files, backend dependencies and frontend dependency lock are unchanged. No third-party executable or new runtime dependency is introduced. Claude redemption uses fixed Anthropic endpoints, account/organization coordination and durable idempotency; an unknown outcome is fenced instead of automatically retried.

## Operational boundaries

Inflight reservations preserve the upstream fail-open policy for cache failure, skip subscription billing and allow a lone request under the existing balance policy. They estimate cost and are not a hard upper bound on every final bill. The reservation lifetime covers the active handler and its submitted billing tasks; a provider video job that outlives its creation request still relies on the existing durable reconciliation path. Do not present this release as a mathematical guarantee against every possible overdraft.

API-key creation limits default to 200 active keys and 60 creations per user per hour; existing keys continue working. Zero disables each limit. Use `billing.inflight_reservation` for reservation settings and retain explicit operator overrides.

Deploy from the tested source with `BuildType=custom`; verify the binary digest, image revision, static configuration and live usage after the switch. Roll back the application image if required, preserving new billing records, durable receipts and keys. Never restore an old full database over current usage. Preserve the disabled legacy Grok health timer: its broad error matching previously created false seven-day cooldowns. Real paid Command Code Go validation remains dependent on a future subscription supplied by the owner.
