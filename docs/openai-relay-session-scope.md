# HTTP Responses relay session isolation

An API-key relay can serve many authenticated downstream users through one
upstream key. Their equal client session IDs must not become the same upstream
session. Enable `accounts.extra.openai_relay_session_scope: true` only for an
OpenAI API-key relay that supports HTTP Responses. The default is false; OAuth
and existing API-key accounts retain their current behavior. WebSocket mode is
disabled for opted-in accounts so it cannot bypass this HTTP boundary.

The namespace uses the authenticated Sub2API user/key and the sorted affinity
tags already authenticated by `AccountAffinity`. Raw client identity headers
are never trusted. For GeToken transit, its exact internal Sub2API key must be in
`account_affinity_settings.trusted_api_key_ids`, and GeToken must stamp the user
header from its authenticated user. Do not trust customer-distributed keys.

Both Responses request builders and the native Images builder project session headers and the explicit
session/thread/cache fields into this namespace. Prompt/history/tool data and
numeric precision are preserved. Requests without a session hint get a unique
request-local ID, stable across attempts; they do not silently share a session
through an identical initial prompt.

Provider-issued `previous_response_id` values are unchanged. Opted-in responses
write only a namespaced owner record in the existing durable owner store, with
the native TTL and cancellation-safe write budget. A different tenant cannot
find this record, and there is no unscoped fallback record. A continuation must
also pass the selected relay's strict ownership check before dispatch. Legacy
unscoped responses cannot be continued through an opted-in relay; send the full
history as a new request instead. Disabling the option does not migrate records.

Validation: service tests cover both builders, trusted transit versus spoofed
headers, direct keys, ten concurrent identities, stable retries, tool history,
SSE terminal usage, foreign continuation rejection before dispatch, persisted
ownership after restart, cancellation, default-off and OAuth behavior. Test
upstreams are local fixtures; production rollout still requires a canary with
the actual authenticated ingress and unique new requests.

Turn metadata in the header and JSON-encoded client_metadata is parsed as an
object and only correlation fields are namespaced. Non-session fields, including
client signals and exact integer values, are preserved. Malformed metadata is a
terminal 400 before dispatch. Top-level thread/parent identifiers are scoped too.

Opaque x-codex-turn-state is never rewritten. On actual response-header commit,
the native TTL-backed owner store binds its digest to the authenticated principal
and selected Sub account. Only that owner/account may echo the exact value.
Unknown, expired, or foreign state is rejected before dispatch; there is no
silent strip or unscoped fallback. Abandoned staged responses do not establish
ownership. Redis failures do not open this boundary.

Images also scope x-codex-image-turn-id, preserving edit-turn affinity without
sharing a caller's ID with another tenant. Multipart image bytes remain opaque
and unchanged; vm2api's Images route selects affinity from the scoped header,
not arbitrary multipart fields. No idempotency/replay header or replayable request
body is introduced. Default-off accounts keep their existing headers and body.
