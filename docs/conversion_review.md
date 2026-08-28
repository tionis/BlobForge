# Blinded Conversion Review

Status: runnable local canary  
Date: 2026-08-29
Related: `converter_evaluation.md`, `mistral_api_adapter.md`

## Purpose

`blobforge review-bundle` turns two or more source-mapped MDAFs for the same PDF
into a browser-local, page-by-page quality review. The public bundle calls the
outputs Candidate A/B/C rather than exposing converter names. A separate
mode-`0600` key maps labels back to exact artifact identities, tools, models,
and paths.

The page view places the source PDF beside raw Markdown from every candidate.
Only exact single-page mapping intervals are accepted; a span covering multiple
pages is rejected instead of being duplicated into misleading page evidence.
Reviewers score text, reading order, hierarchy, lists, tables, assets,
references, source mapping, and wiki utility from 1-5 and can write page notes.
An inline guide anchors 1 as unusable, 3 as acceptable, and 5 as
publication-ready. Explicit N/A distinguishes an absent or unassessable feature
from a missing score.

For selected pages, linked archive assets are copied to neutral candidate paths
such as `assets/B/001.png`; original filenames never enter the public bundle.
Only PNG, JPEG, GIF, and WebP files whose leading bytes match their declared
MIME type are loaded as browser previews. Unsupported or signature-mismatched
assets are reported but not loaded. The raw Markdown target is neutralized too,
preserving placement and syntax without leaking converter-specific names.

Scores autosave in browser local storage when available; direct `file://`
browsing still works when storage is blocked and can always export a JSON
result. A prior partial export from the exact campaign can be imported to resume
in a new bundle or browser session; a mismatched campaign is refused. The
exported result contains the campaign digest and blinded scores, not the
unblinding key.

## Existing eight-page test

The first real bundle compares Poppler, Marker 1, and Docling across all eight
pages of `Storypath_Ultra_Tasty_Bit_03_Shadows_and_Mirrors.pdf`:

```text
.blobforge-migration/evaluations/reviews/
  storypath-ultra-tasty-bit-03-local-v6/index.html
  storypath-ultra-tasty-bit-03-local-v6/review.json
  storypath-ultra-tasty-bit-03-local-v6/source.pdf
  storypath-ultra-tasty-bit-03-local-v6/assets/{B,C}/...
  storypath-ultra-tasty-bit-03-local-v6.key.json
```

Campaign identity:

```text
blake3:77957f19a06b1ddf8288840aa59f2992482eeeab004314134496c9f90e33a468
```

The generated JavaScript smoke test found 8 pages, 3 candidate columns, 27
scoring controls with 27 N/A choices, two page-one asset previews, the inline
guide, and `source.pdf#page=1`. Both previews were visually opened after
generation. All three input MDAFs already pass BlobForge and Vulcan validation.
Open `index.html` in Firefox to review it. The v6 campaign digest matches v3,
so the already submitted page-one export remains valid.

## Continuing after page-one unblinding

The A/B/C mapping was disclosed after the first score, so collecting more
scores under that mapping would no longer be blinded. Pages 2-8 therefore use a
fresh randomly seeded campaign:

```text
.blobforge-migration/evaluations/reviews/
  storypath-ultra-tasty-bit-03-remaining-v1/index.html
  storypath-ultra-tasty-bit-03-remaining-v1.key.json
```

Its campaign identity is
`blake3:f8183298733ee442bd2b3f52c7554e3dcbc5110052d349392121dbbf2a22c694`.
The assignment was verified to differ from the disclosed campaign without
printing the new mapping. Do not import the page-one export into this campaign;
the browser rejects it because the digest differs. Export pages 2-8 separately,
then summarize the two campaigns as independent evidence.

## Regeneration

The output directory and private key must not already exist. This prevents an
evaluation from silently overwriting prior scores or changing its unblinding
key.

```bash
uv run blobforge review-bundle \
  "/home/eric/rulebooks/rulebooks/Storypath Ultra Related/Storypath_Ultra_Tasty_Bit_03_Shadows_and_Mirrors.pdf" \
  .blobforge-migration/evaluations/rulebooks/storypath-ultra-tasty-bit-03.poppler.mdaf \
  .blobforge-migration/evaluations/rulebooks/storypath-ultra-tasty-bit-03.marker1.mdaf \
  .blobforge-migration/evaluations/rulebooks/storypath-ultra-tasty-bit-03.docling.mdaf \
  --pages 1-8 \
  --seed storypath-ultra-tasty-bit-03-local-v1 \
  --output .blobforge-migration/evaluations/reviews/storypath-ultra-tasty-bit-03-local-v7
```

The campaign identity is independent of artifact argument order. It binds the
source digest, sorted artifact identities, selected pages, scoring dimensions,
and a hash of the label seed. The public files omit source paths, artifact
identities, producer names, tools, and models. Markdown is inserted via DOM
`textContent`, never interpreted as HTML.

Use `--random-seed` for a human campaign so the label assignment is not
predictable from a documented seed. Use an explicit `--seed` only for
reproducible engineering fixtures. The generated seed is written solely to the
mode-`0600` key so later summarization can verify the assignment.

## Adding the Mistral candidate

The exact whole-document test is eight pages and $0.032 at list price. Planning
never contacts the provider:

```bash
uv run blobforge evaluate mistral \
  "/home/eric/rulebooks/rulebooks/Storypath Ultra Related/Storypath_Ultra_Tasty_Bit_03_Shadows_and_Mirrors.pdf" \
  --max-pages 8 --max-cost-usd 0.04 \
  --confirm-api-rights --plan \
  --output .blobforge-migration/evaluations/rulebooks/storypath-ultra-tasty-bit-03.mistral.mdaf
```

The plan reports source pages, frozen recipe identity, cost, both ceilings,
cache path, credential presence, rights confirmation, and readiness. After
setting `MISTRAL_API_KEY` and independently confirming that this source may be
submitted to Mistral, remove only `--plan` to execute. The adapter captures the
successful response before packaging, so a packaging retry does not buy the
same conversion twice.

Afterward, validate it and generate a new four-candidate bundle; do not alter
the existing three-candidate campaign:

```bash
vulcan artifact validate \
  .blobforge-migration/evaluations/rulebooks/storypath-ultra-tasty-bit-03.mistral.mdaf

uv run blobforge review-bundle SOURCE.pdf \
  POPPLER.mdaf MARKER1.mdaf DOCLING.mdaf MISTRAL.mdaf \
  --pages 1-8 --seed storypath-ultra-tasty-bit-03-api-v1 \
  --output .blobforge-migration/evaluations/reviews/storypath-ultra-tasty-bit-03-api-v1
```

Keep the key outside any bundle sent to a reviewer. Unblind only after scores
are exported.

## Importing a result

The summarizer validates the result format, campaign digest, selected pages,
dimensions, candidate labels, scores, and N/A values against the private key.
It then reports page/slot coverage and per-converter dimension counts, N/A
counts, and means. Unknown or cross-campaign data fails closed.

```bash
uv run blobforge review-summarize review-export.json \
  --key storypath-ultra-tasty-bit-03-local-v6.key.json \
  --output storypath-ultra-tasty-bit-03.summary.json
```

The summary is unblinded and therefore private evaluation evidence. Output
creation refuses to overwrite an existing file. A blank score remains
incomplete; N/A counts as a completed review slot but never enters a mean.
The private key retains the label seed so the summarizer can recompute both the
campaign digest and deterministic label assignments before attributing scores;
a modified or older unverifiable key fails closed.
