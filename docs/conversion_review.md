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
New v2 campaigns score text, inline formatting, reading order, hierarchy,
lists, tables, assets, references, source mapping, and wiki utility from 1-5
and can write page notes. Inline formatting covers semantically meaningful
bold, emphasis, code, superscript, and related spans. Stored v1 campaigns keep
their original nine dimensions and remain independently valid.
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

After rating a page, **Copy previous ratings** deep-copies only its ratings into
the next selected page. Notes remain page-specific. The button is disabled on
the first page or when the preceding page has no ratings, and replacing ratings
already entered on the current page requires confirmation. This makes repeated
layouts a one-click baseline without marking pages merely because they were
viewed.

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

## Adding the Datalab candidate

The first Datalab recipe uses accurate paginated Markdown with images and no
paid bbox add-ons. Planning is provider-free:

```bash
uv run blobforge evaluate datalab SOURCE.pdf \
  --max-pages 8 --max-cost-usd 0.10 \
  --confirm-api-rights --plan \
  --output .blobforge-migration/evaluations/rulebooks/storypath-ultra-tasty-bit-03.datalab.mdaf
```

Unlike Mistral's published fixed per-page price, Datalab returns exact
list/final cents only after conversion and exposes no preflight quote. The
eight-page cap bounds the first trial; the adapter checks the returned billed
amount against the requested ceiling and captures the response before
packaging. See `datalab_api_adapter.md` for the replay and accounting contract.

## Hosted campaign result

The canonical four-candidate review-v2 campaign contains Marker 1, Docling,
Mistral OCR 4.1, and Datalab Convert accurate across all eight Storypath pages:

```text
.blobforge-migration/evaluations/reviews/storypath-hosted-v1/index.html
.blobforge-migration/evaluations/reviews/storypath-hosted-v1.key.json
```

Campaign identity:

```text
blake3:4f10cea83474b0a728199b05707d5eb3188bb0854bc798759c9aeb2cf5a900cc
```

The complete export was validated before unblinding: all 320 rating slots are
complete, comprising 252 numeric values and 68 N/A values. A=Marker 1.10.2,
B=Mistral OCR 4.1, C=Datalab Convert accurate, and D=Docling 2.122.0. Mistral
leads wiki utility (5.0) and assets (4.857). Marker and Docling lead lists
(5.0), while Docling trails inline formatting (3.0). Datalab's image
descriptions bleed into primary text and one whole-page raster was extracted;
its current 3.0 wiki-utility result is a recipe blocker. See
`converter_benchmark_results.md` for the complete dimension table and defects.

The separate large-book local campaign compared Marker 1 and Docling on eight
table/stat-block-heavy London Falling pages:

```text
.blobforge-migration/evaluations/reviews/london-falling-local-v1/index.html
```

Campaign identity:

```text
blake3:f31eabad8aacc5f4b10ebb96976d5a5491048252a6813df593d74458cab26d67
```

The reviewer stopped numeric scoring because both candidates failed the table
acceptance purpose. Unblinding after that verdict maps A to Marker and B to
Docling. Docling's table screenshots are readable but not usable structured
wiki content; Marker also fails to recover usable tables. This is retained as
qualitative blinded evidence rather than assigning arbitrary relative scores.

## Four-way London Falling table campaign

The hosted challenger uses an eight-page fixture so the paid exposure remains
the same as the Storypath canary. Review pages map to the original PDF as
`1→12, 2→23, 3→31, 4→38, 5→64, 6→78, 7→90, 8→92`. Each page was raster-checked
against its original before submission. The campaign compares Marker 1,
Docling, Mistral OCR, and Datalab accurate:

```text
.blobforge-migration/evaluations/reviews/
  london-falling-tables-hosted-v1/index.html
  london-falling-tables-hosted-v1/REVIEW_NOTES.md
  london-falling-tables-hosted-v1.key.json
```

Campaign identity:

```text
blake3:9a366ab22d1557b1f665b7c76f08ab90db14b670ad0c4d823ed043a8a6b0d3a1
```

Score table semantics by correct headers, rows, columns, and cell associations.
A table screenshot can receive asset credit but is not structured table output.
Record duplicated content, invented/missing cells, and generated descriptions
inside body text. The public bundle has no engine names; keep the mode-`0600`
key unopened until the complete review export is saved.

The reviewer completed fixture pages 1-3 and reported that the remaining pages
repeated the same outcome. The partial export validates at 80 numeric ratings,
36 N/A values, and 116/320 slots. It was unblinded only after that stopping
decision: A=Marker, B=Mistral, C=Datalab, and D=Docling. Across the rated pages,
table means are 1.0/5.0/4.0/1.0 respectively. Mistral and Datalab both score 4.0
for wiki utility; Marker and Docling score 1.0. The five unrated pages remain
qualitative evidence and are not assigned copied scores after unblinding.

The result establishes Mistral as the provisional complex-table backend and
Datalab as the challenger. It also establishes a post-processing requirement:
use ordinary Markdown tables only for rectangular grids and a sanitized
semantic HTML table when faithful output requires `colspan` or `rowspan`. See
`table_output_strategy.md` for the representation and consumer-validation gate.

The follow-up composite recipes replay the same paid responses without API
keys. A fresh two-candidate campaign compares only the normalized Mistral and
Datalab outputs and includes strict rendered table previews alongside raw
Markdown:

```text
.blobforge-migration/evaluations/reviews/
  london-falling-tables-wiki-v2/index.html
  london-falling-tables-wiki-v2.key.json
```

Its campaign identity is
`blake3:efd4e84ff559de4e497fb51ae406b288f7de91224bb223288bc84fb0af8853ce`.
The reviewer rated the first two pages and stopped after the same structural
difference was clear. Strict validation accepted 26 numeric ratings, 14 N/A
values, and 40/160 slots. No scores were reconstructed for pages 3-8.
Unblinding maps A to Mistral-wiki and B to Datalab-wiki. Both scored 5.0 for
text, reading order, source mapping, and the one rated asset. Mistral scored 5.0
for tables and wiki utility versus Datalab's 3.0 and 4.0; Datalab scored 5.0 for
hierarchy versus Mistral's 3.0. Mistral converted the relevant tables
consistently, while Datalab had only one correct HTML table with spans and left
inconsistent grids unchanged. This selects Mistral-wiki for complex tables.

Datalab's retained image alt text contains the unexplained Chinese adjective
`阴森` in an otherwise English description. It exists in the native provider
response; the normalizer removed only the exact duplicated body paragraph and
did not invent or translate the alt text.

The cached Storypath responses were subsequently replayed through both wiki
profiles without provider keys. Mistral removed exactly the 16 typed running
footers criticized in the original review and made no other Markdown-content
change: headings, outline hierarchy, links/assets, lists, emphasis, and prose
remain equal. Datalab's Markdown remained byte-identical because its defects do
not satisfy the intentionally conservative evidence rules. Both composites
pass deterministic replay and Vulcan import. No additional blinded campaign is
needed for an unchanged candidate and an exact removal of already-adjudicated
page furniture.

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
