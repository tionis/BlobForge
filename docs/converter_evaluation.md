# Converter Evaluation and Cost Model

Status: proposed evaluation protocol  
Pricing checked: 2026-08-27  
Primary workload: illustrated tabletop rulebooks imported by Vulcan

## Recommendation

Do not choose a single converter from vendor benchmarks. Run a frozen,
rulebook-specific bake-off with four first-class candidates:

1. Marker 2 `balanced` and `fast` as separate recipes;
2. Docling's standard PDF pipeline, with a VLM recipe evaluated separately;
3. Mistral OCR 4.1 with blocks, confidence, and image extraction enabled;
4. Chandra 2 or the Datalab managed API as the high-quality VLM challenger.

Keep current Marker 1 outputs as the regression baseline. Google Document AI
Layout is a useful commercial control if time permits, but its $10/1,000-page
layout tier and different output contract make it a secondary candidate rather
than the first API choice.

The initial architectural preference is:

- best overall validated recipe as the default;
- a cheap fast recipe for born-digital bulk conversion;
- an expensive fallback recipe for scans, complex tables, and failed quality
  gates;
- no automatic engine cascade until a deterministic classifier and maximum
  spend policy are evaluated.

## Current candidate facts

### Mistral OCR 4.1

Mistral documents OCR 4.1 at $4 per 1,000 ordinary pages and $5 per 1,000
annotated pages. The OCR API can return page Markdown, paragraph-level blocks
and bounding boxes, page/block/word confidence, extracted images, page
dimensions, headers/footers, tables, the returned model identifier, and usage
information. Those fields are unusually well aligned with MDAF source maps and
native renditions.

Sources:

- <https://docs.mistral.ai/models/ocr-4-1>
- <https://docs.mistral.ai/api/endpoint/ocr>
- <https://docs.mistral.ai/inference/pricing>

Use the pinned model `mistral-ocr-4-1`, never `mistral-ocr-latest`, for a
production recipe. Blocks and block confidence are ordinary OCR request fields;
custom structured annotations are a separate capability. Retain $5/1,000 as a
conservative ceiling until pilot billing confirms the selected request shape.
Provider retention, training, regional processing, copyright authorization,
rate limits, and batch terms are launch gates and require contract review rather
than assumptions based on the API shape.

The current general pricing table lists OCR Batch at $0.40/1,000 pages, while
the general Batch guide describes a 50% discount. Treat this as unresolved
documentation and verify a metered pilot rather than budgeting the lower rate.

### Marker 2

Marker 2.0.0 is a substantial rewrite, not a drop-in upgrade from the pinned
1.10.2 stack. Its official release describes `balanced`, `fast`, and no-OCR
modes; Surya OCR 2 and an inference server are part of the OCR path. Marker can
emit Markdown or a JSON block tree, page bounding boxes for relevant blocks,
assets, and additional debug geometry. JSON/native output is a promising MDAF
rendition, but the adapter must prove how its block text aligns with final
Markdown.

The project reports, on its own B200 benchmark, 76.0 overall / 2.9 pages per
second for balanced, 66.6 / 7.4 pages per second for fast, and 43.6 / 23.7 pages
per second for CPU no-OCR. These are candidate-provided results and sizing hints,
not acceptance evidence for BlobForge.

Sources:

- <https://github.com/datalab-to/marker/releases/tag/v2.0.0>
- <https://github.com/datalab-to/marker>
- <https://pypi.org/project/marker-pdf/>

Marker code is Apache-2.0, while its model weights have additional commercial
conditions. Record and approve the exact model license that applies to the
deployed checkpoints. Production also requires immutable model revision or
manifest checksums and an explicitly provisioned inference backend.

### Docling

Docling's `DoclingDocument` retains a content tree, reading order, tables,
pictures, hierarchy, page layout, bounding boxes, and provenance. It can export
Markdown and a lossless JSON representation. This makes it the cleanest local
candidate for direct MDAF mapping even if another engine eventually wins on raw
Markdown quality.

Sources:

- <https://docling-project.github.io/docling/concepts/docling_document/>
- <https://docling-project.github.io/docling/usage/supported_formats/>
- <https://github.com/docling-project/docling>

The code is MIT; each model's own license still applies. Test the standard PDF
pipeline and any GraniteDocling/VLM pipeline as separate recipes because their
quality, resource use, mappings, and provenance differ.

### Chandra 2 / Datalab

Chandra 2 emits Markdown, HTML, JSON, detailed layout information, images, and
page metadata. The project reports strong rulebook-relevant performance on
tables, math, scans, multilingual text, and complex layouts, but those published
benchmarks are supplied by the same vendor and require independent validation.
Its open weights also carry material commercial, attribution, share-alike, and
competitive-use conditions; self-hosting cannot be treated as an ordinary
permissive dependency.

Sources:

- <https://github.com/datalab-to/chandra>
- <https://github.com/datalab-to/chandra/blob/master/MODEL_LICENSE>

Datalab's public pricing page was not machine-readable during this review.
Obtain an exact page/batch quote and data-processing terms before cost ranking
the managed service.

### Google Document AI control

Google currently lists Enterprise Document OCR at $1.50 per 1,000 pages and
Layout Parser at $10 per 1,000 pages. Plain OCR is not equivalent to structured
Markdown with source mappings; Layout Parser is the relevant comparison.

Source: <https://cloud.google.com/products/document-ai/pricing>

## API cost scenarios

These are conversion charges only. They exclude storage, downloads, taxes,
currency conversion, retries, duplicate A/B runs, and engineering time.

| Corpus pages | Mistral OCR 4.1 | Mistral annotated budget | Google Layout control |
| ---: | ---: | ---: | ---: |
| 10,000 | $40 | $50 | $100 |
| 100,000 | $400 | $500 | $1,000 |
| 500,000 | $2,000 | $2,500 | $5,000 |
| 1,000,000 | $4,000 | $5,000 | $10,000 |

For `P` pages, retry/reprocessing factor `R`, and per-1,000-page price `C`:

```text
conversion_cost = (P / 1000) * C * R
```

Use `R = 1.10` for an initial budget, not as a retry target. If 10% of pages are
also sent to a second engine, add 0.10 times that engine's per-page cost. The
real corpus calculation requires a page inventory; bytes and document counts
are not adequate proxies for page-billed APIs.

Example: 500,000 pages through annotated Mistral with a 10% budget margin is
`500,000 / 1,000 * $5 * 1.10 = $2,750`.

## Local compute comparison

For effective worker cost `H` dollars per active hour and measured sustained
throughput `Q` pages per second:

```text
local_compute_per_1000_pages = 1000 * H / (3600 * Q)
```

Add electricity, idle time, storage, failed attempts, API-server hosts, operator
time, and hardware depreciation to `H`. Measure `Q` on the actual worker and
corpus; do not use a vendor GPU benchmark as a capacity promise.

Using Marker 2's published B200 throughput only as an illustration:

| Recipe | Published Q | Active GPU-hours / 1,000 pages | Mistral $4/page-tier compute break-even H |
| --- | ---: | ---: | ---: |
| Marker 2 balanced | 2.9 pages/s | 0.0958 h | $41.76/h |
| Marker 2 fast | 7.4 pages/s | 0.0375 h | $106.56/h |
| Docling comparison reported by Marker | 2.1 pages/s | 0.1323 h | $30.24/h |

The break-even column solves `H / (3600 * Q) = $0.004/page`. At Mistral's
conservative annotated rate, multiply it by 1.25. This comparison is raw active
compute only and says nothing about output quality or source-map fidelity.

## Evaluation corpus

The current 43 exact-byte-distinct PDFs (9,465 pages) are both the initial
production workload and the stable full-book acceptance corpus. Confirm and
record evaluation/API rights per source before upload. Sampling individual
pages alone misses
cross-page headings, continued tables, running furniture, references, and
reading order.

Complete books are not automatically ground truth. Existing Marker Markdown is
a regression baseline, while a smaller adjudication set receives human labels
and a hidden subset remains unavailable during adapter tuning. Every scored
backend emits a validated MDAF through the common contract described in
`converter_adapter_architecture.md`; loose native outputs are not comparable
evaluation results.

Required strata:

- born-digital single-column prose;
- two- and three-column layouts;
- clean, noisy, skewed, compressed, and old scans;
- German, English, mixed-language, unusual fonts, and diacritics;
- dense stat blocks, sidebars, callouts, footnotes, and marginalia;
- bordered, borderless, merged-cell, and continued tables;
- inline/display equations and unusual symbols;
- full-page art, maps, diagrams, captions, and text over backgrounds;
- bookmarks, hyperlinks, internal page references, and printed page labels;
- very large books and known historical BlobForge failures.

Start with all 43 unique PDFs plus approximately 5-10 deeply reviewed pages per
source or canonical edition. This 215-430-page adjudication set is sufficient
for the first round and is much cheaper to maintain than broadly annotating the
corpus before the metrics and review workflow have proven useful. Expand strata
later and keep a hidden holdout set so adapter tuning does not overfit reviewed
pages.

## Ground truth and metrics

Gold data should describe what Vulcan needs, not reproduce one engine's native
schema:

- normalized text and reading order;
- heading text, level, and section extent;
- semantic table grid and cell text;
- equations and code blocks;
- asset presence, crop, caption, and reference;
- page and region provenance for representative output spans;
- cross-references and printed page labels;
- acceptable alternatives where Markdown representation is genuinely
  ambiguous.

Measure:

| Dimension | Metrics |
| --- | --- |
| Text | CER/WER by language and scan class; dropped/duplicated text |
| Reading order | block order accuracy or Kendall correlation; furniture leakage |
| Markdown | structural validity, list/code/math preservation, heading F1 and level accuracy |
| Tables | TEDS/GriTS-style structure, cell text accuracy, continuation handling |
| Assets | recall, precision, correct link/caption, crop IoU, duplicate rate |
| Source map | Markdown byte coverage, page accuracy, rectangle IoU, invalid-boundary rate, confidence calibration |
| Outline | node/title/level F1, complete span alignment, Vulcan import acceptance |
| Rulebook utility | question-answer evidence recovery, stat-block fidelity, manual correction minutes/page |
| Operations | pages/s, p50/p95 latency, peak RAM/VRAM, failure/retry rate, bytes/page, cost/page |
| Reproducibility | repeated output/text digest stability, pinned model proof, native evidence completeness |

Run every recipe at least twice. Store its MDAF, raw sanitized rendition,
metrics, logs, exact environment, and evaluator version. Blind human reviewers
to engine identity and randomize side-by-side ordering.

## Selection gates

A candidate is ineligible as the default if any of these fail:

- generated artifacts do not validate in both BlobForge and Vulcan;
- final Markdown spans cannot be related to source evidence without invalid
  UTF-8 offsets or systematic fabricated precision;
- exact tool/model revision cannot be recorded;
- native response cannot be safely retained or meaningfully redacted;
- license, source-document rights, privacy, regional processing, or retention
  terms are unacceptable;
- retry behavior cannot be bounded by idempotency and spend limits;
- whole-book failure rate exceeds the agreed threshold;
- material rulebook strata regress against Marker 1 without a reviewed tradeoff.

After gates, use a Pareto decision rather than hiding tradeoffs in one score.
For a summary view, suggested quality weights are 30% text/semantic fidelity,
20% reading order and Markdown structure, 15% tables/equations, 15% hierarchy,
15% source-map fidelity, and 5% assets. Cost, latency, legal terms, and operations
remain explicit axes.

## Pilot output

The evaluation command should produce a machine-readable result keyed by source
and recipe, plus a human report containing:

- per-stratum quality and confidence intervals;
- complete failures and degraded fallbacks;
- normalized dollars per 1,000 pages and per average book;
- actual GPU/CPU hours and power where available;
- mapping coverage/accuracy and MDAF capability rates;
- disagreement examples and rendered visual comparisons;
- recommended default, fast path, fallback, and rejected candidates;
- assumptions that still require a larger production canary.
