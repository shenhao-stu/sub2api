# Personal Nano image bridge

The bridge accepts OpenAI Images requests for `nano-banana-2`. Its private Bearer key fixes the source to one CPA pool. A Pro key selects `gemini-3.1-flash-image`; an Ultra key selects `ultra/gemini-3.1-flash-image`. Only configure pools that completed an independent native image probe. The bridge makes one non-streaming native `generateContent` POST and never switches pools or credentials itself. CPA retains its own scheduling and retry policy.

## Build and run

Go 1.23+; standard library only. For Linux x86-64, run:

```sh
go test -race ./...
go vet ./...
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o nano-adapter .
```

Supply environment variables through a protected service environment:

- `NANO_PRO_KEY`, `NANO_PRO_CPA_KEY` for the Pro ingress key and its CPA key.
- `NANO_ULTRA_KEY`, `NANO_ULTRA_CPA_KEY` for the Ultra pair. Either pair may be omitted. Ingress keys are distinct and at least 24 characters.
- `NANO_BIND` defaults to `127.0.0.1:57300`. `NANO_CPA_ADDR` defaults to `127.0.0.1:57240`. Both must be loopback or private numeric IPv4 addresses with a port. Use the verified internal CPA listener assigned to this deployment. Restrict ingress to the actual private consumer and use a dedicated unprivileged service account.
- `NANO_STATE_DIR` is required. Use an absolute, persistent 0700 directory writable by the service. Replay markers contain a SHA-256 digest of each explicitly identified request and no customer payload. Keep this directory across service restarts.
- Invoke the compiled binary with this environment. It logs startup or configuration failures, and has no payload, credential or per-request logging.

## Wire contract

- Authenticated `GET /v1/models` lists only `nano-banana-2`.
- `POST /v1/images/generations` accepts JSON with exact `model` and a nonempty `prompt` of up to 16 KiB. `POST /v1/images/edits` accepts multipart fields plus 1–3 uploaded JPEG/PNG reference files named `image` or `image[]`. Both use the same result shape.
- Supported options are `n=1`, `response_format=b64_json`, `stream=false`, `quality=auto`, plus either a supported square `size` or an `aspect_ratio` and `image_size`. All are optional. Square sizes are `1024x1024`, `2048x2048`, `4096x4096` or `auto`. Accepted aspect ratios are `1:1`, `2:3`, `3:2`, `3:4`, `4:3`, `9:16`, `16:9`, `4:5`, `5:4`, `21:9`. Image sizes are `1K`, `2K` or `4K`. The default is 1:1 and 1K. Explicit conflicting sizes fail before dispatch. Confirm each tier against actually delivered dimensions before public sale.
- Masks, URL inputs, URL output, custom output format, transparent background, streaming, other quality levels, unknown fields and extra aliases receive an explicit 4xx before dispatch. Returned bytes are original JPEG/PNG, including measured dimensions in `data[0].size` and MIME in `data[0].mime_type`. The client receives one final non-thought image after a STOP. A provider text response, invalid/truncated image, multiple candidates or images, oversized body or unfinished output is an error.
- `gemini_usage_metadata` carries only integer token counts actually returned by CPA. Missing counts stay missing. A standard OpenAI usage object is included only when explicit prompt, candidate, and total counts reconcile exactly, including observed thoughts. Measured cache and reasoning details are retained when present. Measured usage also survives a complete native error envelope or a completed response without a valid final image. Truncated or unavailable accounting is left unknown. Supplier costs and token-class prices are never inferred. Responses return a fresh adapter `X-Request-ID` and CPA's `X-Cpa-Trace-Id` when supplied.
- Limits: 2 concurrent submissions total, 20 MiB request, 24 MiB upstream wire response, each image 10 MiB / 17 MP, 170 seconds per upstream POST. Oversize/contention fails explicitly. Upstream host/paths are fixed, redirects and transport retries are disabled, and the bridge does not fetch customer URLs.
- For replay protection pass a stable `Idempotency-Key`, `X-Client-Request-ID` or `X-Request-ID`, in that order of preference. The first use reserves a durable marker before dispatch. Reuse returns 409 across both tiers and restarts, regardless of the original outcome. No replies or prompts are stored. If the consumer strips all identifiers, the bridge can only enforce one upstream attempt per inbound HTTP request. A subsequent new inbound request cannot be recognized as a duplicate. On an uncertain outcome reconcile the original CPA trace before taking any further action.

## Signed no-dispatch receipt

For g67, configure a dedicated `NANO_ADMISSION_SIGNER` with a standard-base64 Ed25519 seed (32 private bytes). Set `NANO_ADMISSION_PEERS` to comma-separated exact private IPv4 peers, for example a verified Sub container address ending in /32. In signed mode all endpoints authorize the actual TCP peer address; forwarded headers are ignored. Keep the link on the same isolated private host network. The subcommand `admission-public-key` emits only the standard-base64 public verification key.

The consumer creates one stable logical `X-Request-ID` and a fresh 32-character lowercase-hex `X-Nano-Attempt` challenge for each adapter attempt. With a challenge, only the logical header selects the claim ID. On saturation the adapter must obtain an exclusive durable negative claim before issuing 429 with code `adapter_busy`. It signs the UTF-8 fields `nano-admission-v1`, `429`, `POST`, request path, logical ID, challenge, `adapter_busy` joined by line feeds with no final newline. `X-Nano-Admission-Proof` is the standard-base64 signature; `X-Request-ID` echoes the logical ID in that signed response. The claim permanently fences both tiers on this shared state directory. Prior claims return unsigned 409. A CPA 429, an uncertain outcome or a duplicate attempt never receives a signature.
