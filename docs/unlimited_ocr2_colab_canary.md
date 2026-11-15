# Unlimited-OCR-2 Colab GPU Canary

Status: completed; MDAFs packaged and blinded campaigns ready; no recipe promoted  
Executed: 2026-11-14 to 2026-11-15 (Colab A100); analyzed and packaged 2026-11-15

## Outcome

Unlimited-OCR-2 is a credible grounded-transcription challenger. It is not yet
a better complete extractor than Mistral wiki. On the retained Storypath page,
its transcription equals v1 and is second only to Mistral. On London Falling's
dense stat-block tables, it produces semantic HTML tables with `colspan`
comparable to Mistral wiki. Each mode, however, has a silent content defect that
no runner check detects:

- page-local (gundam) mode dropped one whole table on one page; and
- multi-page (base) mode misreads small type and dropped one table on another
  page.

The upstream Transformers path is far too slow to serve: about 9.5 generated
tokens per second on an A100, or 95-210 seconds per page. Its peak VRAM of
6.7-7.6 GiB fits a 12-16 GiB GPU.

## Run and identities

| Evidence | Identity |
| --- | --- |
| Model | `baidu/Unlimited-OCR-2` revision `9cf077c4b206e0e2a31879df937b0b0bcba3661c`, every loaded file SHA-256 pinned |
| Runtime | Python 3.12.3; torch 2.13.0+cu126; transformers 4.52.1; triton 3.7.1; PyMuPDF 1.27.2.2; Pillow 13.0.0 |
| GPU | NVIDIA A100-SXM4-40GB, compute capability 8.0, driver 580.82.07 |
| Page-local recipe | `sha256:5a672d5bced522927fe538e8ddad1081e6f55a2819d5647bdb3278152cb80396` (gundam tiles) |
| Multi-page recipe | `sha256:a9b530758e67ffbb6f761aaac68877b338036513dcad37a8285a4060434c5f77` (one 1024-px base view per page) |
| Storypath source | SHA-256 `d0463c98…2d2e5` (8 pages) |
| London Falling fixture | SHA-256 `d9c71699…6a46` (8 table pages) |
| Retained v1 raster | SHA-256 `064cbc8d…827f` |

Recipe identifiers are evaluation identities, not registered BlobForge recipe
digests. The plan's 50 page generations all completed in one session, about
1.9 A100-hours of generation. The evaluator files in the evidence tree are
byte-identical to `evaluators/unlimited_ocr2/`.

## Output format

v2 replaces v1's `<|det|>label [box]<|/det|>` tags with
`<BLOCK>label<LEVELn>[x1, y1, x2, y2]<FORK>text`.

- **Heading levels:** in page-local mode `<LEVELn>` restarts at 1 on every
  page. In multi-page mode the levels stay consistent across the document; for
  example, London sections use levels 2-4 throughout.
- **Continuations:** `<MERGE>` joins a paragraph across a column break.
  Multi-page output also labels page-break continuations `cross_text` and
  `append_text`. Storypath's 8-page chunk produced six of them.
- **Tables:** emitted as HTML with `colspan`, plus a few `rowspan`s in
  multi-page mode.
- **Other labels:** `header`, `footer`, `page_number`, `image` and `other`.

The runner parser originally knew only v1 syntax and reported zero blocks. It
now parses both syntaxes (analysis parser 2). `reanalyze` recomputed every
unit's analysis from the unchanged raw output and token IDs; the original
analysis is retained in `results-parser1-backup.tar.gz`.

## Runner checks

Every unit finished with EOS. No page-guard intervention (suppressed early
stop, forced page break or forced EOS) occurred, and the n-gram guard altered
greedy choices only in London `multi-8` (15 times). The only structural failure
is one malformed block in London `multi-8`, `<BLOCK>other[508,polygons`, which
has no box and no `<FORK>`.

Both repeated page-local units were byte-different. In each case the text is
identical once box coordinates are blanked; only boxes moved, by at most 2 on
the 0..999 scale. That GPU nondeterminism means a raw-byte identity cannot
serve as a repeatability gate. The summary now reports text identity
separately.

## Retained page versus v1 and the other extractors

The scoring uses the v1 CPU canary's unchanged reference, normalization and
metrics (`score_retained_page.py`).

| Extractor | CER | WER | Word F1 |
| --- | ---: | ---: | ---: |
| Mistral OCR 4.1 | **0.091%** | **0.546%** | 0.9945 |
| Unlimited-OCR-2, retained raster, page-local | 0.183% | **0.546%** | **0.9973** |
| Unlimited-OCR-2, PyMuPDF render, page-local | 0.183% | **0.546%** | **0.9973** |
| Unlimited-OCR-2, page 4 of the 8-page chunk | 0.183% | **0.546%** | **0.9973** |
| Unlimited-OCR v1 Q4_K_M (CPU) | 0.183% | **0.546%** | **0.9973** |
| Marker 1.10.2 / Docling 2.122.0 | 0.389% | 0.682% | 0.9959 |
| Datalab Convert Accurate | 0.457% | 0.955% | 0.9925 |

The only edits are again the four omitted decorative diamond bullets. v2 adds
a heading level to the title. It still emits no inline emphasis or list syntax
and extracts no assets.

## Storypath: page-local versus multi-page

On prose, base mode is nearly identical to gundam mode: across eight pages it
differs by 0-6 words per page. Its differences are character misreads
(`twin`→`tuin`, `bloodied`→`blooded`, `alter`→`after`). The 2-page and
8-page chunks produced the same text. Multi-page mode adds what page-local
cannot: consistent heading levels and explicit cross-page paragraph joins.

## London Falling: dense tables

The embedded text layer covers only 548 words, so no automatic text reference
exists. Structure and agreement:

| Output | Tables | Rows | Cells | `colspan` | `rowspan` |
| --- | ---: | ---: | ---: | ---: | ---: |
| Mistral wiki v1 | 34 | 265 | 1,104 | 364 | 0 |
| Datalab wiki v1 (plus 165 pipe rows) | 18 | 116 | 581 | 113 | 0 |
| Unlimited-OCR-2 page-local | 32 | 250 | 1,053 | 316 | 0 |
| Unlimited-OCR-2 multi-8 | 32 | 268 | 1,139 | 296 | 4 |

Bag-of-words agreement (F1) is 0.948 for page-local versus raw Mistral and
0.962 versus raw Datalab. Raw Mistral and raw Datalab agree with each other at
0.925; multi-8 versus Mistral is 0.917.

Visual adjudication of printed page 88 (fixture page 7, six stat-block tables):

- **Page-local:** the digits and text are accurate. Moira's SR4A and SR5 tables
  are merged into one block with all content retained. The Ork Hooligan SR5
  table is omitted entirely, and nothing flags the omission.
- **Multi-8:** keeps all six tables but misreads small type: `SR5`→`SRS`,
  `SR4A`→`SRA`, `0/0`→`QD`, `Armor 0`→`9`, `Edg 2`→`22`,
  `MY BACK`→`MV BACK`. It also drops columns (`Arm`, `CM`).

On fixture page 1, multi-8 omits the final table, which page-local retains.
It correctly reads `Dianna Elwes`, where page-local wrote `ELVES`. Base mode's
single 1024-pixel global view per page is the likely cause of the small-type
errors. Long documents in base mode therefore trade per-character accuracy on
dense pages for document-level structure.

## Throughput

| Run | Seconds per page | Generated tokens per second | Peak allocated VRAM |
| --- | ---: | ---: | ---: |
| Storypath page-local | 96 | 9.1-10.2 | 7.27 GiB |
| Storypath multi-8 | 95 | 9.6 | 6.98 GiB |
| London page-local | 206 | 9.2-10.6 | 7.27 GiB |
| London multi-8 | 213 | 9.3 | 7.60 GiB |

Prefill took under 3 seconds (5 seconds on the very first unit). Decode
dominates because upstream's Transformers loop generates one token per
Python-level forward pass with no batching or CUDA graphs. On the retained
page, the A100 took 120 seconds, barely faster than v1 Q4 on the CPU at 147
seconds. These numbers measure only this runtime. They are not a serving
estimate; any throughput claim needs the authors' SGLang path. Mistral
processed the same eight Storypath pages in 6.9 seconds.

## Decision

- Keep Unlimited-OCR-2 in the matrix as a grounded, self-hostable challenger.
  Do not route jobs to it.
- Its strengths are document-consistent heading levels and cross-page
  continuation joins (multi-page), semantic tables, near-Mistral prose
  transcription, and modest VRAM.
- It carries three blockers. Both modes can silently omit content, so a
  production recipe needs an independent coverage check, such as comparison
  against the PDF text layer where one exists, or against the other mode's
  output. Base mode is not accurate enough on dense small type. The available
  runtime cannot meet corpus throughput.
- A hybrid recipe is the plausible next experiment. It would take each page's
  text from page-local gundam output and use a multi-page pass only for
  hierarchy and continuations. It would need its own recipe identity,
  reconciliation rules and blinded review.
- The local packager, the blinded campaigns and a heuristic coverage check now
  exist (see below). Human scoring of both campaigns is the next decision
  gate. Measure an SGLang serving path before making any cost or throughput
  decision.

## Packaging

`evaluators/unlimited_ocr2/adapter.py` packages cached runner output through the
converter-bundle v1 ABI and the shared MDAF builder (`package.py`, local, no
GPU). It verifies every unit's raw digest, recipe and page coverage, and every
raster digest. Its normalization rules are:

- `<LEVELn>` titles become ATX headings (levels clamped to 1-6). A title
  without a level stays a paragraph.
- `<MERGE>` and `append_text` join the block to the preceding paragraph.
  A trailing hyphen is kept as printed.
- HTML tables are re-serialized through the strict semantic-table allowlist.
  A non-rectangular grid is kept verbatim in a fenced block, never padded.
- Header, footer, page-number, empty and malformed blocks stay out of the
  Markdown but remain in the retained native renditions.
- Image boxes become PNG crops of the rendered page. A diagnostic labels them
  as derived crops, not embedded PDF images.
- Text `<` is escaped, so model text cannot inject HTML.
- Each block maps to its page plus a point rectangle (pixel × 72/300, valid
  for these unrotated pages whose cropbox starts at the origin). There is no
  confidence, because the model reports none.

| MDAF | Identity | Tables (kept verbatim) | Joins | Malformed | Selectors |
| --- | --- | ---: | ---: | ---: | ---: |
| Storypath page-local | `blake3:28a587b1…f0c` | 0 | 2 | 0 | 103 page + 103 rectangle |
| Storypath multi-8 | `blake3:86913498…d35` | 0 | 6 | 0 | 100 + 100 |
| London page-local | `blake3:78ab199f…de2` | 32 (6) | 0 | 0 | 124 + 124 |
| London multi-8 | `blake3:361ffc66…08e` | 32 (8) | 0 | 1 | 120 + 120 |

All four pass BlobForge and Vulcan validation. Repackaging reproduced every
identity byte for byte. About a fifth of the model's tables are not
rectangular grids. Mistral wiki retains its own invalid grids the same way, so
the difference is visible in review rather than hidden.

## Blinded campaigns

Mistral comparisons use wiki-v10, the current production recipe. They were
produced by offline artifact reprocessing (`blobforge reprocess`, zero
provider calls) from the retained wiki-v3 Storypath and wiki-v1 London
parents. Both derivatives pass Vulcan.

| Campaign | Digest | Candidates | Pages |
| --- | --- | --- | --- |
| `reviews/storypath-uocr2-v1` | `blake3:6ae6ab14d619999efe5d918a04ec26efb651bba4df8d9f8adb9e8e79088c0690` | Mistral wiki-v10, Datalab wiki v1, Marker 1, Unlimited-OCR-2 page-local and multi-8 | 1-8 |
| `reviews/london-falling-tables-uocr2-v1` | `blake3:f1f852b3500993d96170fed11a752e54203d6c74233db27aa6f6427a98ece06a` | Mistral wiki-v10, Datalab wiki v1, Unlimited-OCR-2 page-local and multi-8 | 1-8 |

Labels use a private random seed, and both keys are mode 0600 and unopened.
The public files contain no engine names. Asset file formats (PNG versus JPEG)
loosely group candidates; earlier campaigns share this weakness.

## Coverage check

`run_unlimited_ocr2.py coverage` combines two heuristic omission signals:

1. per-page recall of the PDF's own text-layer words, on pages with at least
   50 words, after dehyphenation; and
2. disagreement between two runs (page-local versus multi-page) in missing
   words and table counts.

Complete Storypath pages recall 0.93-0.99. The remaining misses are
decorative `Y` glyphs, split ligatures and running footers. The threshold of
0.90 flags both visually confirmed omissions: page-local London page 7 at
0.848 and multi-8 London page 1 at 0.798. It also flags multi-8 London pages
3, 5 and 8 (0.874-0.878), whose small-type misreads genuinely degrade the
text. Cross-mode disagreement independently flags pages 1 and 7 (table counts
3 versus 4 and 5 versus 4) and London page 3. It flags no Storypath page.

The check works only where a usable text layer exists, and the thresholds are
calibrated in-sample on two confirmed omissions. Treat flags as review
triggers, not acceptance criteria.

## Evidence

The ignored evidence tree is
`.blobforge-migration/evaluations/unlimited-ocr2-colab-canary/`. It contains:

- the evaluator copy, inputs and plan;
- `out/` (environment records, recipes, renders with transforms, and each
  unit's raw output, token IDs and `result.json`);
- `summary.json`, `retained-page-comparison.json` and `score_retained_page.py`;
- the parser-1 backup;
- `coverage.json` and `mdaf/` (the four packaged artifacts).
