# Gemini empty Chat Completions

Gemini may return HTTP 200 with input/cache usage and no assistant content. The raw Chat Completions gateway previously treated usage as enough to release the stream and reported success. Usage proves metering, not delivery.

The gateway now tracks delivery independently for Gemini model routes. It retains a bounded non-content prefix until text, reasoning, tools, or media arrive. An empty result returns a non-failover `upstream_empty_completion` with the observed, allowlisted finish reasons. Headers that have already been committed keep HTTP 200 and carry logical 502 in an SSE error. Ops records the failure and the handler does not append a second terminal error. No content means no fabricated success terminator.

All observed usage remains in the forward result and client error. Inclusive input/cache accounting, pricing, and usage idempotency remain unchanged. Empty errors without usage cannot estimate a bill, and are not automatically retried because missing counters do not prove the supplier performed no work. The prefix is limited to 64 KiB; overflow is a failed delivery and still drains metering.

This does not repair unknown provider generation failures. A historical input-only failure without its native terminal frame must remain unresolved at that layer; do not infer a specific safety or tool error from a synthetic regression fixture.
