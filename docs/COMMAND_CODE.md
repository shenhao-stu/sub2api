# Command Code accounts

This fork preserves its existing providers and adds opt-in Command Code accounts. No database migration or separate relay process is required. Existing accounts are not converted.

## Configure an account

In **Accounts → Add account**, select the **Command Code Go** card for a Go subscription, or choose a Provider preset for another supported plan. The preset selects the appropriate platform and API-key type:

| Preset | Platform | Base URL | Upstream protocol |
| --- | --- | --- | --- |
| Command Code Provider (OpenAI) | OpenAI | `https://api.commandcode.ai/provider` | Responses or Chat Completions |
| Command Code Provider (Anthropic) | Anthropic | `https://api.commandcode.ai/provider` | Messages |
| Command Code Go (experimental) | OpenAI | `https://api.commandcode.ai` | CLI `/alpha/generate` |

Enter the API key from Command Code Studio, select the appropriate proxy and group, and configure model mapping and prices. Public model discovery does not require the key. It filters the official catalog by protocol, preserves full model IDs and does not prove that a subscription can use every listed model. Discovery failure is reported; it does not substitute OpenAI's catalog or infer free prices.

For an OpenAI Provider account, the Responses mode and supported capabilities control Responses/compatibility routing; native Chat Completions input uses the official Chat endpoint. Models that support only Messages require the Anthropic preset. Choose an explicit model for an account test. Tests generate a small response and therefore can consume credits.

[Official Provider documentation](https://commandcode.ai/docs/provider) states that GOAT, Pro, Max and Team API calls consume their plan credits. The Go plan does not support this official API. The experimental Go adapter uses an internal CLI protocol, which may change independently. There is no automatic fallback between these two modes and no fabricated token-refresh flow. A real subscription test is required before relying on the Go adapter in production.

Provider accounts can require zero data retention with the account setting or request header `x-cmd-zdr: 1`. Invalid or ambiguous headers are rejected. Go does not support this guarantee and rejects requests requiring it.

## Go quota and exhausted credit

Saved Go accounts offer **Check quota** in the edit dialog. It reads `/alpha/billing/credits` with that account's saved key, proxy and transport, displaying monthly credit and five-hour/weekly windows. The request has a 20-second deadline and a bounded response; it never imports browser cookies, accepts an alternate origin or falls back to another account's key. Missing values remain unavailable, distinct from a measured zero. Save credential changes before checking. Quota snapshots do not automatically change scheduling based on rounded balances.

The official structured `BAD_REQUEST` insufficient-credit rejection stops the affected Command Code account and allows bounded failover before client output. Verify or replenish the subscription before manually re-enabling the account. Generic validation errors do not disable accounts. A rejected response carrying observed usage is accounted for without replay. Go's connection test uses a 512-token output budget to avoid the false truncation seen with a 64-token test on reasoning models.

This update follows Fwind43/sub2api commits `c27038d9f5bd4f9b98fc7911f11948617efbdd91` and `74417b974a7f78d0dc5ea6c9407759a6cb20d799`, and CPA-CommandCode-Provider `36c60d52189c57d73ae3379218fa9c1c769b9eaf`. Transport and billing remain native to this fork; no sidecar or upstream script is installed.

## Protocol and accounting boundaries

The Go adapter independently converts supported text, image input, reasoning effort and function tools to the CLI wire format and back through the existing Chat Completions, Responses and Messages adapters. It rejects unsupported request semantics before conversion; it does not silently emulate server-side state, compact requests, provider-hosted tools or structured-output guarantees. Use the official Provider preset when its broader API features are required.

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
