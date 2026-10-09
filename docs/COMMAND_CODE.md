# Command Code accounts

This fork preserves its existing providers and offers opt-in Command Code accounts through the built-in platform. No separate relay process is required. Existing accounts are not automatically converted.

## Configure an account

In **Accounts → Add account**, select the **Command Code** platform and an API-key account. Choose Go for a Go subscription, or the Provider mode for a plan with public API access. Both modes use platform `command_code`; `credentials.account_mode` is `go` or `payg`. The `payg` value identifies the Provider transport, not a claim that the subscription has no included credits.

| Mode | Fixed origin | Upstream protocol |
| --- | --- | --- |
| Go (experimental) | `https://api.commandcode.ai` | CLI `/alpha/generate` |
| Provider | `https://api.commandcode.ai/provider` | Messages, Responses or Chat Completions, selected by model |

Enter the API key from Command Code Studio, select the appropriate proxy and group, and configure model mapping and explicit prices. Go uses the public model catalog without sending its key; Provider uses the shared platform catalog and protocol routing. Discovery preserves full model IDs and does not prove subscription entitlement. Failure is reported rather than substituting another provider's catalog or assuming free prices. Choose an explicit model for account tests, which generate a small response and can consume credits.

The old OpenAI/Anthropic Provider presets and separate Go quota dialog were removed in v0.2.15+g8. Provider protocol routing and account usage now use the upstream platform components. No legacy accounts existed in the audited deployment; the upgrade does not silently convert legacy `extra.provider` accounts. A manually maintained legacy account must be recreated on the Command Code platform with its original key, group, proxy and prices reviewed.

[Official Provider documentation](https://commandcode.ai/docs/provider) excludes Go from public Provider API access. Keep the Go CLI adapter for that plan; its private protocol may change independently. Other eligible subscriptions use the Provider transport and their applicable credits. There is no automatic fallback between these modes or fabricated token-refresh flow. A real subscription test remains required before relying on the Go adapter in production.

Provider accounts can require zero data retention with the account setting or request header `x-cmd-zdr: 1`. Invalid or ambiguous headers are rejected. Go does not support this guarantee and rejects requests requiring it.

## Go quota and exhausted credit

Go and Provider share the platform's account usage and balance components. The read-only workflow first resolves the organization with `/alpha/whoami`, then queries credits, subscription and usage summary as needed. It sends `orgId` only for an organization account. It uses the saved account key, proxy and transport, fixed official HTTPS endpoints, bounded responses and request deadlines; redirects are disabled. Missing subscription or usage data remains unavailable rather than being interpreted as a zero balance or unlimited allowance.

Rolling five-hour and weekly windows, monthly credits, purchased credits and organization spending limits retain their distinct meanings. Purchased credits can exempt the account from subscription windows; a model-scoped spending cap does not disable unrelated models. The structured Go insufficient-credit rejection enters the shared recoverable balance-stop workflow, instead of permanently disabling the account. Unrelated validation errors do not trigger that classification. Before any client output, bounded failover remains available; observed usage must be settled and cannot be replayed. The Go account test keeps a 512-token budget to avoid false truncation on reasoning models.

The retained Go transport was originally reviewed against Fwind43/sub2api commits `c27038d9f5bd4f9b98fc7911f11948617efbdd91` and `74417b974a7f78d0dc5ea6c9407759a6cb20d799`, and CPA-CommandCode-Provider `36c60d52189c57d73ae3379218fa9c1c769b9eaf`. Transport and billing remain native to this fork; no sidecar or upstream script is installed.

## Protocol and accounting boundaries

The Go adapter independently converts supported text, image input, reasoning effort and function tools to the CLI wire format and back through the existing Chat Completions, Responses and Messages adapters. It rejects unsupported request semantics before conversion; it does not silently emulate server-side state, compact requests, provider-hosted tools or structured-output guarantees. Use the Provider mode when its broader API features are required.

Credentials are restricted to Command Code's fixed HTTPS origin and endpoint set. Credential-bearing requests never follow redirects. Incoming authorization, cookies and account header overrides cannot replace the selected account's key. The public model catalog sends neither account keys nor cookies. Go uses a fixed CLI identity and does not apply ordinary custom header overrides. No browser cookie import, local OAuth listener, telemetry fingerprint emulation, shell execution or third-party runtime dependency was added.

The existing group pricing, scheduling, proxy, retry, billing and usage-log pipelines remain authoritative. A missing price is still an admission error; an explicitly configured zero price retains its meaning. A native Go stream succeeds only with a valid final event and authoritative usage. EOF, malformed events and missing usage fail. Observed input, output, cache-read and cache-write usage is retained when a later event fails; the relay does not estimate tokens from text or retry a metered failed completion. Reasoning tokens are a subset of output, not an extra chargeable bucket.

Command Code accounts disable upstream WebSocket and pool modes. Go additionally disables passthrough and compact modes. Its Messages bridge requires `max_tokens >= 128` and cannot preserve thinking/cache-control blocks; the Responses bridge does not accept encrypted reasoning history, `reasoning.effort: none` or a Responses-shaped body at the Chat endpoint. The private protocol has bounded request, event and aggregate response sizes. Unsupported semantics return a client error before an upstream request rather than triggering provider/model failover.

## Subscription acceptance tests

After adding a subscribed account in an isolated test group with explicit prices:

1. Confirm its allowed models and a small text account test. Check the selected upstream endpoint in Ops.
2. Exercise JSON and streaming requests through Chat Completions, Responses and Messages where supported.
3. Run a function call and its tool result; verify IDs, complete JSON arguments, tool-choice behavior and stop reason. Verify image input only on a model that supports it.
4. Compare input/output/cache usage with Command Code's own usage record and the local bill. Test repeated client IDs against existing deduplication behavior.
5. Cancel a stream, trigger a controlled invalid request and inspect the resulting Ops event and any observed usage. A partial stream must never acquire a false success terminator.
6. For Provider ZDR, verify either the promised route or its explicit upstream rejection. For Go, verify a ZDR request is rejected locally.

Mock protocol, accounting, isolation and regression tests do not replace these real-account checks.

## Research provenance

The implementation was written independently after reviewing the official API and pinned public sources, including [Fwind43/sub2api](https://github.com/Fwind43/sub2api), [CPA-CommandCode-Provider](https://github.com/Fwind43/CPA-CommandCode-Provider), [MAXeaglet/commandcode-proxy](https://github.com/MAXeaglet/commandcode-proxy), [patlux/pi-commandcode-provider](https://github.com/patlux/pi-commandcode-provider) and [CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI). No CPA code or unaudited third-party proxy binary is bundled. Static review found integration risks in those implementations and cannot prove the absence of every backdoor in any external repository.
