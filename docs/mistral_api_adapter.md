# Mistral OCR API Adapter

Status: evaluation-ready; production promotion remains gated  
Date: 2026-08-28  
Related: `converter_adapter_architecture.md`, `converter_evaluation.md`,
`rulebook_corpus_cost.md`

## Frozen evaluation recipe

The canonical recipe is
`blobforge/recipes/mistral-ocr-4.1-v1.json`, with identity:

```text
blake3:982a97ca1d45f5a0ac30dd8c7507efb594688d1b949f406ef4620f3352e723c7
```

It selects `mistral-ocr-4-1`, Mistral SDK 2.9.4, block confidence,
paragraph blocks, embedded images, provider Markdown joined with two line
feeds, and page-level source-map publication. Maximum pages, maximum list-price
spend, cache location, credentials, and timeout are execution policy and do not
change output identity.

The named provider model is intentionally recorded as a mutable alias. Mistral
echoes a model identifier but does not return an immutable checkpoint digest.
This is sufficient for an honest comparative evaluation, but not the project's
strongest reproducibility tier.

## Paid-call checkpoint

Every run requires a durable response cache. The CLI defaults to
`~/.cache/blobforge/mistral-responses`; use `--response-cache` to place it on a
backed-up local volume. The cache key binds the exact source SHA-256, canonical
recipe digest, model, and provider request flags. Each entry contains the
complete provider response but no API key, signed upload URL, or temporary
provider file ID.

For one cache key, a Linux advisory file lock spans cache lookup, upload, OCR,
and atomic response persistence. Concurrent identical attempts therefore
produce one paid request. A process crash releases the kernel lock. The
successful response is fsynced and atomically renamed with mode `0600` before
response validation, asset extraction, or MDAF packaging begins.

Malformed or incomplete captured responses fail closed. They are not silently
deleted or repurchased: preserve the entry for diagnosis and remove it only
after explicitly deciding that another billable call is appropriate. The
cache contains rulebook text and images and must be protected and backed up as
sensitive source-derived data.

## Validation and publication

Before packaging, the adapter requires:

- returned page indices to exactly cover `0..source_pages-1` in order;
- `usage_info.pages_processed` to equal the source page count;
- every returned page to contain Markdown;
- images to contain valid base64 before publication.

Image filenames cannot escape the bundle and are deterministically prefixed by
page and image position, avoiding cross-page collisions. Media types come from
the data URL rather than an untrusted provider filename. Markdown image links
are rewritten to the packaged asset path before UTF-8 offsets are measured.

The source map currently publishes exact provider page boundaries and average
page confidence. The full native response retains ordered blocks, pixel
rectangles, dimensions/DPI, block types, tables, confidence values, usage, and
returned model. Region mappings are deliberately not inferred merely because
a provider block has a rectangle: a region is publishable only after its block
content is proven to correspond to an exact final-Markdown byte range. That
normalizer is still a promotion task.

## Operation

Synchronize the isolated environment once, export the secret only to the API
worker process, and set both independent ceilings:

```bash
uv sync --project evaluators/mistral
MISTRAL_API_KEY=... uv run blobforge evaluate mistral book.pdf \
  --max-pages 500 --max-cost-usd 2.00 \
  --response-cache /srv/blobforge/mistral-responses \
  --output book.mistral.mdaf
```

A cache hit does not require `MISTRAL_API_KEY`. The CLI prints whether the
response was captured or replayed, returned usage, and the normalized
$0.004/page list-price estimate. Actual billed amount and promotional credits
are account-ledger facts not returned by OCR; record them separately in the
future experiment ledger.

No paid call should start until rights are recorded for the selected test pages
and the adjudication/holdout set and scoring rubric are frozen.

## Remaining production gates

- persist response checkpoints in shared worker storage, not only one host;
- add the experiment-attempt ledger for billed amount and credits;
- validate exact block-to-final-Markdown correspondence before publishing
  region rectangles;
- exercise timeout/cancellation and real malformed/partial provider responses;
- run two bounded adjudication passes and independent Vulcan import;
- decide whether the provider's mutable checkpoint identity is acceptable for
  the selected production quality tier.
