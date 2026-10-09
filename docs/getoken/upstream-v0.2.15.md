# Getoken 0.2.15+g10

Official v0.2.15 (`f2669c8cf62555cd92389b3f55920e9e6e7c6ff2`) is merged into the custom fork. Build with `BuildType=custom`; automatic replacement with the official binary remains disabled. The short public version is `0.2.15+g10`.

## Preserved contracts

The upgrade includes upstream Go 1.27.2 and dependency security fixes, provider catalogs and routing, protocol compatibility fixes, and WebSocket per-turn price refresh. Retain the fork's stricter key revalidation: revoked or reassigned keys and unavailable authoritative authentication state cannot continue a WebSocket session. Missing pricing remains an admission error, not a zero-cost success. Soft-deleted-key settlement, billing deduplication, observed partial usage and no replay after metered output remain intact. Getoken's separate policy removing failed input estimates is unchanged by this release.

Grok keeps automatic official CLI version synchronization, the no-downgrade update rule, one bounded retry for an explicit outdated-version rejection, Heavy admission, quota classification and the distinction between client errors and upstream failures. Upstream SSO consent and empty-completion recovery are included. Do not replace the existing fingerprint with a fixed version copied from release notes.

Command Code uses one platform and the upstream Provider catalog, protocol routing and organization-aware usage components. Go retains its CLI transport because the public Provider API excludes that plan. Nine obsolete preset/quota source and test files were removed; fixed destinations, redirect prevention, ZDR validation and billing boundaries remain. See [Command Code accounts](../COMMAND_CODE.md) for the current setup and paid-account acceptance checklist.

## Ops corrections

- Unsupported independent `alpha/search` endpoints enter a one-hour capability cache tied to their origin. This suppresses repeated unsupported calls without disabling ordinary chat. It does not create search support where none exists.
- A structured wallet-exhaustion response stops same-account pool retries and uses the existing atomic quota cooldown. Its minimum is ten minutes; a longer recorded window, Retry-After or administrator policy is preserved. State writes survive caller cancellation.
- Scheduler cache projections retain account mode, origin and protocol/capability metadata so those decisions remain valid after cache refresh.
- Claude queue keepalives contain `event: ping` followed by the ping data frame. This fixes a real wire-format mismatch with strict Anthropic clients and guards. Tests exercise both user-slot and account-slot waiting; the downstream protocol guard remains strict. A later follow-up also names locally generated Messages error events. The Anthropic passthrough normalizes a data-only ping only after observing a complete, single-data-line frame; unknown, mixed and multiline frames remain unchanged. It buffers at most one candidate line, preserves usage draining, and records only an account ID when this repair is applied.

## Deployment evidence and invariants

Application changes passed full backend unit/integration CI, targeted race checks, frontend typecheck/build/lint and 2,810 frontend tests. Real candidate and public synthetic requests covered GPT Responses, Grok Chat and Claude/Gemini Messages, complete stream termination, authoritative usage, positive charges and released quota holds. Paid Command Code acceptance still requires a subscribed account; mocks cannot establish entitlement.

Validate the actual Getoken-to-candidate relay path with an owned bounded key before shifting customers. Health checks and a connection from the container namespace do not exercise the application's private-address guard. A versioned Docker hostname is not automatically trusted just because it is internal. Public-domain detours can add an edge timeout to long non-streaming requests.

The reviewed rollout preserves the canonical business URL and temporarily uses explicitly configured per-account HTTP egress to a tested candidate. Snapshot and compare-and-set only the owned account rows, preserve existing proxy policies, and restore the exact original settings after draining. A deployment must refuse unexpected configuration drift, fail its canary early, and retain a rollback path through the previous image and configuration. Never weaken the network guard to make a rollout work.

Wait for in-flight connections to finish before retiring the old process; do not force-close long client streams. End with one Sub2API serving container, leave other services and shared PostgreSQL/Redis running, and retain one previous image for rollback. Never restore an old financial database over new traffic. Clean only identified unreferenced images and transfer/build intermediates. Use a finite verification window rather than creating a recurring automation.

## Issue #7949 verification and production acceptance

[PR #7939](https://github.com/Wei-Shaw/sub2api/pull/7939), referenced by [issue #7949](https://github.com/Wei-Shaw/sub2api/issues/7949), merged as `f2669c8c`, the v0.2.15 tag itself. The fork already contains its OAuth search-history repair. Transform, HTTP passthrough and WebSocket first/continuation paths use the compatibility normalizer. The unchanged upstream implementation preserves cached-only search, no-tools semantics, Lite trigger ordering and the API-key/native-compact boundaries. The dedicated regressions passed. The 48-hour production audit found two historical third-party API-key errors before this upgrade and none afterward; that does not establish entitlement or correctness for every external provider.

On 2026-10-09, g10 was promoted after all 14 applicable CI checks and the actual candidate transport canary passed. Its seven public model/protocol calls had complete responses, authoritative usage, positive billing and released holds. After the previous process drained naturally, canonical-route Claude/GPT canaries passed again. The production image is `94386a44daa2`; its binary SHA-256 is `ab20a79bcc9ac29ccb6b6f00cb49a06c6852d101c35b5f1e1a659dc40607600d`. Build source `7cbadf23e` and validated main `93f3bd7c9` differ only in test expectations.

The finite monitor recorded 90 successful health checks and 839 positive usage rows, all settled, without new Redis evictions or settlement failures. A longer request window had 1,262 successes among 1,268 calls, zero protocol-mismatch/edge-timeout failures and zero failed input estimates; remaining failures came from upstream interruption or rejection. A subsequent settlement audit matched all 2,416 positive rows older than three minutes to billing deduplication records. These are bounded observations, not a claim that external services cannot fail.

The earlier rollout had two operational incidents: a public-domain detour caused two edge timeouts, and an untrusted versioned internal hostname caused 492 rejected requests, including seven synthetic checks. All affected requests had zero charges. The corrected rollout retains the canonical URL and verifies the application's real transport before switching. Cold rollback preparation also isolates the old image's DNS alias until it is healthy; warm rollback reuses a draining process without recreating it. The rollout and rollback boundaries have 18 local regression checks. Cleanup removed six historical stopped containers and eleven obsolete image IDs while retaining mounted data, active images and recovery versions.
