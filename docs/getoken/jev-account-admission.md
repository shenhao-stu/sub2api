# Jev account admission

TypeSafe support is part of the complete official v0.2.12 integration. The deployed Getoken `0.2.12+g2` already includes the native `/v1/systemone` route, account tests, group routing and input-token billing. Adding credentials alone does not require replacing the application binary.

## Admission invariant

An account requested as unused and fully funded must satisfy all of these conditions before entering a production group:

- Its credential is well formed and does not duplicate an existing account.
- There is evidence of its usage and balance before the connection test.
- A minimal native System One call returns a valid answer.

The public [API reference](https://docs.typesafe.ai/api) and the inspected public OpenAPI expose model listing and inference, but no balance or historical-usage endpoint. An HTTP 200 response proves the credential can complete that request; it does not establish that the account was previously unused or fully funded. Keep unknown eligibility separate from exhausted or invalid credentials. If the operator confirms a previously filtered export, record that provenance without describing it as an independent balance measurement.

A connection test itself consumes input tokens. Preserve its timestamp and reported usage so later reconciliation can distinguish this known test from earlier use. At the time of this review, the official [model documentation](https://docs.typesafe.ai/models) lists input-only pricing of $0.042 per million tokens; the existing fork fallback matches that price. Do not change existing customer multipliers as part of credential import.

## Import and verification

Use the supported admin API in bounded batches. Deduplicate before writes and reconcile uncertain write outcomes before retrying. Create accounts without production group bindings until their eligibility and native call results pass. Increase batch size only after checking scheduler latency, database and Redis load, and other groups' health.

Keep credentials and email addresses out of logs, public documentation and command-line arguments. Send credentials only to the official TypeSafe endpoint or the operator's own gateway over the intended secure channel. Disable credential-bearing redirects during external probes. Respect rate limits and `Retry-After`; persistent errors pause the batch rather than switching credentials to circumvent a limit.

Use a dedicated TypeSafe group and preserve the native protocol. Jev is not a Chat Completions or Responses model. Verify a real `/v1/systemone` gateway call, matching usage and billing, then revoke temporary test keys. Final counts must reconcile eligible, imported, duplicate, rejected and unknown records against the input file.

## Verification record, 2026-10-02

The inspected production image and binary still match functional commit `56376acff321704a2aef8fdbb12dd01ba92f1a2f`, version `0.2.12+g2`. Official release `5106065716e494204fc0e8db16f68f6e9d576be0` is an ancestor. Existing billing, security, CommandCode and Grok policies were preserved without another deployment.

Eight sampled credentials returned HTTP 200 with `jev-1.13.0` and valid typed answers. The supplied export contains no balance or historical-usage fields, so this sample is not a statement of full eligibility, a full-file test, or a completed bulk import. The private project journal retains the evidence and remaining dependency on eligibility information.
