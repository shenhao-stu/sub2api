# Private image pools

An OpenAI Images group can prefer a private pool for eligible generation requests and use its existing provider when the pool cannot accept the request. Selection, account limits, image permissions, security checks and the customer’s billing stay in the same gateway process. No customer key or second gateway request is needed. Unset the route variables to disable selection. Configuration changes take effect when the gateway process starts with the new environment.

## Grok

Set `SUB2API_PERSONAL_GROK_IMAGE_POOL` to `CLIENT_GROUP_ID/DONOR_GROUP_ID/EXACT_MODEL[,EXACT_MODEL]`. The client group must remain OpenAI. Create a distinct exclusive Grok donor group with no issued customer keys and add verified operator-owned OAuth accounts without removing their existing memberships. Eligible model IDs are `grok-imagine-image` and `grok-imagine-image-2.0`. Enable only models verified on the donor pool. The gateway requires exact identity mapping, an official xAI HTTPS endpoint, an immediately available slot and a passing native media eligibility check. It sends no client session affinity into the donor group.

This route accepts single nonstream JSON generations with `model`, `prompt`, optional `n=1`, optional size tier `1K` or `2K`, and explicit `response_format` of `url` or `b64_json`. Other input shapes, pixel dimensions, omitted output format and edits use the client group’s original provider. A local capacity failure can fall back before inference. An explicitly classified credential acquisition rejection may fall back before inference. Once the provider receives a generation, an error or an empty successful response does not cause another generation.

## Nano Banana 2

Configure these values when a private bridge supports the admission protocol below.

| Environment variable | Value |
| --- | --- |
| `SUB2API_PERSONAL_NANO_IMAGE_POOL` | `CLIENT_GROUP_ID/DONOR_GROUP_ID` for an OpenAI client group and a distinct exclusive OpenAI donor group |
| `SUB2API_PERSONAL_NANO_IMAGE_PEER` | Exact `http[s]://<private-or-loopback-IPv4>:<port>` without path or trailing slash |
| `SUB2API_PERSONAL_NANO_ADMISSION_PUBLIC_KEY` | Base64 standard encoding of the bridge’s 32-byte Ed25519 public key |
| `SUB2API_PERSONAL_NANO_IMAGE_TIERS` | Comma-separated verified tiers. Defaults to `1K`. Options are `1K`, `2K`, and `4K` |

Use dedicated OpenAI API-key accounts for the bridge credentials. Set `extra.personal_nano_image_adapter=true`, exact identity mapping for `nano-banana-2`, the pinned peer as `base_url` (an optional `/v1` suffix is allowed), no account proxy and membership only in the donor group. Do not issue customer keys for this group. Peer credentials, donor account priorities and the bridge private signing key stay under operator control. A marked account is excluded from normal gateway scheduling, including ordinary image, sticky and text paths. Only the admitted private request can use it. Unsupported requests can still select the original account, even if an adapter was accidentally bound to the client group.

For `POST /v1/images/generations`, the private route supports one square image with `model=nano-banana-2`, a nonempty prompt of at most 16 KiB, optional `n=1`, `response_format=b64_json`, `quality=auto`, `stream=false`, `aspect_ratio=1:1`, approved `image_size` tiers or matching square `size` (`auto`, `1024x1024`, `2048x2048`, `4096x4096`). Edits, masks, streams, different output formats, other models, incompatible sizes, unverified tiers, extra parameters and duplicate keys use the existing provider.

The bridge must restrict ingress to the gateway’s actual numeric peer address. Use private HTTP only on an isolated same-host bridge. For a network crossing use HTTPS with a trusted certificate for the pinned IP, plus an authenticated network boundary. The private transport connects directly to the exact numeric endpoint, sends only its own auth, correlation and challenge headers, and disables redirects, proxies and replay of the POST body. The gateway’s general upstream URL policy does not change.

### Capacity refusal and retry

Each submission carries a tenant-scoped `X-Request-ID` and fresh cryptographic 128-bit `X-Nano-Attempt` (lower-case hex). The correlation value is a hash of the authenticated principal and an explicit idempotency key or bounded client/gateway correlation ID. Raw client identity and request headers are not forwarded. The adapter must atomically and durably claim the logical ID across its tiers before issuing a refusal or starting inference. An existing claim returns a terminal conflict. A later incoming request with a newly generated ID is a distinct intent.

Before either donor or original provider selection, both configured models acquire a single PostgreSQL intent in the existing `idempotency_records` table. The unique scope/key is retained for 30 days, also for a client shape only the original provider can serve. Competing requests for the same logical ID receive 409; an unavailable or indeterminate store returns 503 without inference. A successful intent acquisition only records ownership. It is never reclaimed to replay an ambiguous submission, and does not represent a successful generation. Disable donors by suspending their dedicated group membership while keeping the route and its admission fence configured until outstanding work and client retries have expired. Preserve both intent stores through rollback.

A new refusal before provider dispatch can return HTTP 429 with `error.code=adapter_busy` and a single `X-Nano-Admission-Proof`. Its value is a standard Base64 Ed25519 signature over these UTF-8 lines, joined with `\n`, without a trailing newline.

```text
nano-admission-v1
429
POST
/v1/images/generations
<logical request ID>
<attempt challenge>
adapter_busy
```

The gateway verifies the signature, request binding and absence of usage or output before falling back to the original group. Unsigned saturation, upstream quota errors, duplicate requests, redirects and ambiguous provider failures remain terminal. Keep the bridge’s durable intent store across restarts and rollback. Test real output geometry, one customer usage row, one billing dedup and dispatch counts on primary, pre-dispatch fallback and ambiguous failure paths before enabling a production client group.
