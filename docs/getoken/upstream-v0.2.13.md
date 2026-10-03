# Getoken 0.2.13+g2

This release merges official tag `v0.2.13` (`3040209f2`) into the existing Getoken fork. It does not replace the fork with an upstream release binary.

The TypeSafe API-key identity now supports upstream billing-probe configuration, and `typesafe.ai` is recognized as an official API domain. This removes the default-probe validation failure during account creation. It does not add a balance API or establish that a credential is unused or fully funded.

The upstream deleted-key settlement fix is already covered by the fork's stronger implementation: a completed authorized request can settle against a soft-deleted API key while preserving its usage counters and checking the original user ID. Keep that implementation. `ErrAPIKeyNotFound` now denotes a missing row or an owner mismatch, not a normal soft deletion, so ignoring it would weaken transaction integrity. The regression tests cover successful settlement, idempotent replay, database failure and owner/missing-counter rejection.

Official v0.2.13 changes no Grok source. Preserve the existing hourly official CLI metadata sync, no-downgrade compare-and-set, one retry only after an explicit obsolete-version rejection and an actual version advance, account-scoped quota handling, Heavy-only admission, Fast aliases and video/stream billing. CommandCode, ingress security, dependencies, database migrations and frontend remain unchanged.

Validation must use the repository's build tags: ordinary `go test` does not run `unit` or `integration` files. Unit tests require `-tags unit`; database semantics are additionally exercised against an isolated test PostgreSQL using `PG_TEST_DSN`. Docker integration-harness compilation is not an executed integration suite.

The upstream security-reporting policy is retained in `.github/SECURITY.md`; its reporting address and advisory URL belong to the upstream project. Deployment-specific findings must be handled privately by the deployment operator.

Release operations: build while the current instance serves traffic, verify an isolated candidate on a separate loopback port without the trusted `sub2api` alias, validate a bounded real request and revoke its key, then transfer trusted ingress and drain the old instance. The steady state is one Sub2API instance with one verified rollback image. Preserve unrelated services and do not globally prune caches or volumes.

The g2 follow-up recognizes xAI's exact structured refusal, `I'm sorry, I can't help with that request.`, alongside its existing shorter form. Both return a fixed `content_policy_violation` message through the existing error writer. They remain request-scoped: no account cooldown or failover, no invented usage, and actual upstream metering remains billable. Generic permission errors, entitlement failures and quoted or extended sentences are not newly classified as content refusals. HTTP regression tests exercise Responses, both Chat paths and Messages before and after heartbeat commitment without sending rejected customer prompts to any provider.
