# Capture acceptance Ops closeout

Malformed JSON capture probes intentionally produce a native local 400. Leaving
their Ops rows unresolved makes a successful release look like a service failure.
Do not relax parsing, suppress all such errors, whitelist a User-Agent, delete
history, or resend the probes to address this.

After capture acceptance has passed, create a new closeout manifest from its
immutable receipt. Each entry must include the exact Ops `id`, server
`request_id`, `client_request_id`, `api_key_id`, `user_id`, `group_id`,
`user_agent`, and the receipt file's `proof_sha256`. Use `schema_version: 1`
and a `probes` array. Include only the deliberate, expected local 400 cases.
Manifests are trusted operator input, not a client-controlled classification.

Run on the existing Sub2API admin/database host, with its existing access:

```sh
python3 closeout.py reviewed-manifest.json
python3 closeout.py reviewed-manifest.json --execute --receipt-dir new-closeout-run
```

The first command is read-only. The second rechecks every exact record, rejects
upstream context or any usage/dedup/receipt evidence across keys, then calls the
native `PUT /api/v1/admin/ops/request-errors/:id/resolve` on loopback. It records
a durable per-row intent before each PUT and checks persisted resolution. It
does not send inference requests, mutate SQL tables, delete logs, change billing,
or write keys to disk. A timeout or unknown response leaves its intent in place;
reconcile with the read-only command before any further action. Never rerun the
original capture or model request to close its Ops row.

Use a fresh closeout directory for each release, preserve the original acceptance
receipt unchanged, and archive the closeout receipt with the release checklist.
The release is not operationally complete until these expected probes are
resolved and the real customer failures remain visible. Ops' native unresolved
filter then excludes the acknowledged probes; historical/all views retain them.

Tests (no network/database/model):

```sh
python3 -m unittest discover -s ops/capture-closeout -p 'test_*.py'
```
