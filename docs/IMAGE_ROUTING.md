# Image relay operation

The Getoken image channels use the existing origingateway API-key accounts.
Native requests use `/v1/images/generations` or `/v1/images/edits`. The provider's
Responses probe is negative; do not force Responses support to make a model
appear schedulable. A catalog entry alone does not establish endpoint support.

On 2026-10-04, provider `/v1/models` and bounded real image requests verified:

- The standard GPT image account serves `gpt-image-2`, `gpt-image-2.5-flare`, and
  `gpt-image-2.5-sunburst`, with identity model mappings.
- The existing 4K-group key advertises `gpt-image-2`. Do not substitute a different
  `-4k` product merely because it appears in the provider's public price list.
- The previous nioflow fallback accounts were suspended because their balance
  could not cover image precharges. Keep existing credentials for explicit
  operator recovery; do not retry depleted accounts indefinitely.

Getoken's `image` channel now uses its existing standard image service key,
instead of the Codex pool. Unsupported GPT Image 1/1.5 catalog rows are offline;
they are not aliases for GPT Image 2. Retail prices and group ratios are unchanged.

For each change, back up only the affected configuration privately, use the
account management API to synchronize scheduling caches, and verify a real
image plus exactly one billing-dedup entry. Credentials, prompts and generated
images do not belong in operational reports. Roll back touched configuration
fields only; never restore an old database over current usage.

This is a configuration correction on `0.2.13+g5`, not a new Sub2API binary.
