# Authenticated final usage receipts

GET `/v1/sub2api/usage/:request_id` returns immutable final Anthropic streaming metering owned by the current authenticated API key. The request ID is the server-generated `X-Client-Request-ID`; no supplied owner/user/key ID changes the query. The exact read-only UUID route remains available after billing quota exhaustion or expiry; ordinary generation still passes billing checks. Manual-disabled credentials, user/IP/group validation and authentication remain enforced. Responses are `Cache-Control: no-store`.

`source: wire_final` means the final metering the downstream would receive, including existing wire-level cache-TTL rewrites. It is not the locally transformed/priced usage_logs record, nor a claim about a provider invoice. Token counters, model, service tier, web-search count, endpoint and recorded time are returned; account identities, prices, keys and request/response bodies are excluded.

A valid input meter and final output event are required; primers, malformed/negative/fractional/null counters and missing records never become zero receipts. A valid output delta followed by message_stop or [DONE] is supported. The actual normal and passthrough stream paths capture evidence even after client disconnect. A bounded receipt persistence failure cannot skip existing Sub2API billing.

Migration246 only adds provider_usage_receipts. Its `(api_key_id, request_id)` primary key and insert-on-conflict behavior preserve the first final record; there is no historic backfill. Old binaries remain compatible. Rollback keeps this evidence table and the original usage/balance ledger intact.

The capability response header advertises the endpoint protocol, not a guarantee that every upstream or converted protocol provides final evidence. Missing records return404, storage failures503; callers must keep an unresolved state and must not replay generation to obtain a receipt.
