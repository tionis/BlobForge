# Datalab Convert API Adapter

Status: bounded evaluation adapter
Date: 2026-08-30

BlobForge's first Datalab candidate uses the managed Convert v1 endpoint in
`accurate` mode. It requests paginated Markdown and extracted images, retains
the complete provider response as native evidence, removes only verified page
delimiters, rewrites raster assets to safe local paths, and publishes exact
page-only Markdown mappings. It intentionally does not request paid word,
table-cell, or list-item bounding-box add-ons. Those change both evidence and
billing and therefore require a separate recipe.

The canonical recipe is
`blobforge/recipes/datalab-convert-accurate-v1.json`, with identity:

```text
blake3:c1dc8c06bf29a7a5f1639a4a0bdfc8be1250745d5f6e13438c68b1e38df9bc6f
```

## Quota and replay safety

Execution requires `--max-pages`, a positive `--max-cost-usd`, and
`--confirm-api-rights`. The local adapter verifies the PDF page count before
submission and sends that exact count as Datalab's `max_pages`. Datalab does not
offer a pre-request Convert quote or a server-enforced dollar cap, so the dollar
ceiling cannot prevent the first charge. The adapter checks the returned
`final_cost_cents` and fails if it exceeds the operator ceiling; the page cap is
the actual pre-request exposure bound. Keep the first run within the account's
$5 trial until measured billing is known.

Responses are keyed by exact source SHA-256, recipe digest, endpoint, mode, and
output-affecting options. A Linux kernel lock serializes identical work. The
submission and validated same-origin polling URL are atomically stored
immediately after POST; a restart resumes polling instead of resubmitting. The
complete successful response is atomically cached before validation or MDAF
packaging. Corrupt, mismatched, and cached-failed entries fail closed. The tiny
interval between provider acceptance and recording the polling URL is the only
remaining crash window in which the API contract cannot prove whether a charge
occurred.

Only HTTPS polling URLs on `www.datalab.to` under `/api/v1/convert/` receive the
API key. Credentials are inherited through `DATALAB_API_KEY`; they never enter
requests, cache files, recipes, artifacts, diagnostics, or command arguments.
Datalab deletes server-side results one hour after completion, making immediate
local capture mandatory.

## Cost evidence

The native response preserves Datalab's `cost_breakdown`. Diagnostics report:

- list cost from `list_cost_cents` when the response supplies it;
- billed cost from `final_cost_cents`;
- credits/discount as the non-negative difference only when both values exist.

The first live Convert response returned only `final_cost_cents` despite the
current SDK documentation describing both fields. BlobForge records list price
and credits as unavailable in that case; it never relabels the billed amount as
list price or invents a credit value.

The first eight-page accurate-mode canary returned `final_cost_cents=6`, a
13.12-second provider runtime, four images, no parse-quality score, and no model
version object. The cached response packages to canonical MDAF identity
`blake3:2071347f7728035763d51c2de451dd6fde7c0542fb9e30891f3abc5e4982522f`
and replays byte-for-byte with both provider keys unset.

Two first-response compatibility fixes were needed after the paid response was
already safely cached: optional documented list-cost evidence and translating
the managed model identity to MDAF's valid `mutable-alias` vocabulary. No MDAF
existed on either previously undefined failure path, so the frozen recipe
digest was retained and the response was never repurchased.

The second eight-page table canary again reported `final_cost_cents=6`, with a
19.14-second provider runtime inside 25.2 seconds end to end. It exposed a
serialization-only difference: the live HTTP object retained provider key
order while cache loading returned the same object in canonical sorted order.
The native rendition is now always serialized with sorted keys, and a focused
test requires live/cached native bytes to match. The unreviewed live-order MDAF
was preserved under `/tmp`; the canonical artifact is
`blake3:3a4551a34a4ba805287e16ac9a1a4b4794d48bcb720dec05ca28b7046076dafa`
and is byte-identical to a replay with both provider keys unset. The cached
response was not repurchased.

This derived difference must not be interpreted as a formal provider credit
ledger. Production promotion still needs a coordinator attempt ledger that
stores provider billing fields separately from artifact identity.

## Running the bounded canary

Planning never contacts Datalab:

```bash
uv run blobforge evaluate datalab SOURCE.pdf \
  --max-pages 8 --max-cost-usd 0.10 \
  --confirm-api-rights --plan \
  --output storypath.datalab.mdaf
```

Remove only `--plan` once the credential is visible. A cache hit can be replayed
without `DATALAB_API_KEY`. The current official contract is documented at
<https://documentation.datalab.to/docs/recipes/conversion/conversion-api-overview>
and <https://documentation.datalab.to/api-reference/convert-document>.

## Promotion gaps

- Freeze or obtain immutable managed model identities; the current API exposes
  only a provider-managed recipe class.
- Decide whether a second JSON or HTML+bbox recipe provides enough mapping and
  structural value to justify its extra request/add-on cost.
- Add shared worker checkpoint storage and a billed-attempt ledger.
- Resolve description bleed, page-screenshot extraction, and actual table-cell
  fidelity in the blinded table campaign before treating the adapter as
  production-ready.
