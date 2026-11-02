# Grok cooldown boundaries

The gateway classifies failures once at the provider boundary. HTTP, metered errors and terminal stream errors use the same state transition. The team overlay preserves the account's selected reset, including delays below 30 seconds, with its existing one-hour cap. An expired team observation cannot start another cooldown. A duplicate transport-layer default must not extend a shorter selected deadline. The existing bounded backoff for repeated OAuth 429s remains in effect; it is separate from this overlay bug.

Model-specific free usage blocks the named account/model and the same team/model. Other models remain available. Transient model capacity remains local to that account/model. Account quota still uses persisted cooldowns and existing recovery checks; neither successful failover on another account nor a request-shape error clears a real exhausted quota.

A spending-limit message does not identify the exhausted billing window. Long cooldowns require a successful billing observation within the current window and a finite utilization of at least 100 percent. When weekly and monthly quotas are both exhausted, recovery waits for both. Calendar dates, absent utilization, failed observations and observations from an earlier period cannot justify a long block; the gateway uses the existing ten-minute probe interval in those cases.

Existing persisted cooldowns need evidence-based reconciliation. A successful quota probe clears only the exact rate-limit generation observed before that probe. A billing-only response is not proof that model inference recovered. Do not clear all cooled accounts or erase a newer concurrent limit. Keep account pause flags, proxy settings and credentials unchanged. Legacy deadlines can be corrected only against verified current billing evidence, with a compare-and-swap update and scheduler notification.

For text requests, product-scoped billing must identify an exhausted text product
such as `GrokBuild`. An exhausted `GrokImagine` product cannot establish a text
reset. Aggregate-only legacy observations remain supported. Each candidate
official window must be future, successfully observed inside that window, and
no more than 32 days away; an invalid longer window cannot hide a valid shorter
one. `usage_limit_reached` and the precise usage-limit-reached message use this
recoverable native cooldown path, including HTTP 402.

Full administrators can arm the same native gate without sending another model
probe:

```text
POST /api/v1/admin/grok/accounts/:id/quota-cooldown
{}
```

The empty object requires an exhausted official text billing window. If an
administrator has instead observed the upstream usage-limit error with no known
text reset, the only accepted observation code is:

```json
{"observed_error_code":"usage_limit_reached"}
```

The response separates `retry_at` from nullable `official_reset_at` and labels an
unknown reset as `source: "retry_probe"`. Its ten-minute retry interval is local
policy, not an invented upstream reset. No timer, billing probe, inference call,
credential refresh, or automatic enabling is performed by this endpoint. It does
not accept arbitrary reset timestamps, inactive accounts, or scoped admin keys.
Persistence atomically extends the existing rate limit and notifies the existing
scheduler; a concurrent longer hold is preserved. Database timestamp precision
must not admit before a finer official boundary.

`schedulable` remains a separate administrator switch. For an operator-owned
temporary pause, arm and verify the cooldown first, then explicitly enable only
that authorized account with the existing schedulable endpoint. Its temporal
gate stays closed until the selected boundary. An unrelated or later manual
pause is never enabled by cooldown expiry. At an unknown-reset retry boundary,
normal traffic can try the account again; another usage-limit response re-arms
the cooldown and fails over through the existing path. Do not add paid polling or
treat a retry boundary as proof that provider quota has reset.
