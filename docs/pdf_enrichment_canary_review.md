# PDF Enrichment Canary Review

Date: 2026-08-27; corrected candidate reviewed 2026-08-28

Status: first recipe rejected; corrected candidate passes its born-digital
rulebook scope and remains gated only on operational telemetry and explicit
freeze

Rejected recipe:
`blake3:cf33db6438b2a2fbe1e44538bf05cb64a40bf9d88e3f211b1276933c580e1598`

Corrected candidate:
`blake3:0e7e6c1ba4bb6a8920a58cd08fe3c957bd48b729cbccc5733ffec3d47876a569`

## Corrected candidate decision

The corrected candidate resolves every failure that rejected the first recipe
and passes the born-digital rulebook canary. It is suitable for the final
recipe-freeze decision; it is not yet authorization to run the unbounded
backfill.

This recipe is intentionally scoped to digitally generated PDFs, especially
illustrated pen-and-paper rulebooks with usable embedded text. It has no OCR
stage and makes no quality claim for image-only or scan-heavy PDFs. Such inputs
may produce little or no new mapping and should be routed to a future,
separately identified OCR recipe when BlobForge gains that use case. Scan/OCR
coverage is therefore an applicability boundary, not an acceptance gate.

The implementation now uses the nearest trusted anchors on both sides,
enforces monotonic pages, refines defensible regions from disjoint Poppler word
evidence, and separates region publication from page-only publication. Fuzzy
prose regions cannot span multiple Poppler blocks. Exact whole-block matches
retain block geometry, while insufficient region evidence preserves only the
page selector. A verifier independently rejects page regressions, page/region
method violations, duplicate rectangles, and report/count disagreement.

The original ten-document/153-page canary was repeated twice. Every MDAF
identity was stable, and all ten passed BlobForge and independent Vulcan
validation. Coverage increased from 1,411 to 1,666 of 2,355 blocks (59.9% to
70.7%) and from 319,013 to 381,074 of 492,744 semantic bytes (64.7% to 77.3%).
Of the 1,666 mappings, 1,078 have region evidence and 588 are explicitly
page-only.

The expanded canary added five complete difficult books: the 436-page German
Cthulhu rulebook, the older low-text-density *Paths of Storytelling*, the
highly visual *Cortex Prime*, the dense 502-page Shadowrun 5 core book, and the
modern 389-page *Curseborne* core book. Together the 15 artifacts cover 1,957
pages and 31,997 Markdown blocks. They publish 20,047 mappings (62.7% block
coverage): 13,044 region mappings and 7,003 page-only mappings. Semantic-byte
coverage is 4,678,882 of 10,282,814 bytes (45.5%). Lower byte coverage in the
long books is accepted here because omitted precision is safer than forced
evidence.

All 15 artifacts pass catalog/lineage verification, BlobForge validation, and
independent Vulcan validation. A complete invariant audit found zero page
regressions and zero duplicate published rectangles. Manual review covered 51
unique records: the lowest-confidence region, lowest-confidence page-only, and
highest-confidence mapping in every document plus every retained mapping at a
known v1 failure span. The sample included rotated text, tables, columns,
sidebars, forms, dark backgrounds, unusual fonts, German text, and image-heavy
pages. All region samples selected the intended text; page-only samples
selected the intended page without claiming a rectangle. Rendered source
sheets remain temporary because they contain copyrighted book pages.

The five-book expansion took about 23 minutes at concurrency two on the local
CPU host. Individual 400–500-page books took roughly 15–18 minutes. This fits
the 32-GiB deployment target, but the bulk runner should record per-document
runtime and peak memory and use size-aware concurrency before the complete
backfill begins.

Remaining freeze gates are formal acceptance of the publication policy and
runtime/peak-memory recording sufficient for the bulk audit. Equation handling
is assessed when it occurs in the born-digital rulebook corpus, but a dedicated
scientific-document benchmark is outside this recipe's target. Restart safety
is covered by a regression that recovers a `processing` catalog row and
replaces an interrupted partial destination atomically.

## First-candidate decision

Do not freeze this recipe and do not run it over the remaining 1,367 legacy
artifacts. Its artifacts are structurally valid and useful as retained
experimental evidence, but the current confidence and publication rules admit
unsupported region precision and a small number of wrong-page/repeated-text
mappings.

The next recipe must constrain candidates with both preceding and following
trusted anchors, distinguish page confidence from region confidence, and emit
page-only mappings when Poppler block geometry is too coarse. Word/line-level
geometry should be retained so split Markdown paragraphs do not inherit a
whole column or card rectangle. Repeated source geometry must be rejected or
explicitly justified instead of silently reused.

## Review method

The inspection covered all 10 canary documents and all 153 pages indirectly
through complete-book invariants. It then manually inspected 35 unique
aligner-created mappings:

- the lowest- and highest-confidence mapping in every document;
- every page-order regression;
- every source rectangle assigned to more than one Markdown span; and
- additional confidence quartiles as a textual cross-check.

For each visual sample, the source PDF page was rendered at 144 DPI and the
published point rectangle was overlaid in red. The exact UTF-8 Markdown span
was compared with the text visible in that region and with retained Poppler
evidence. This was a mapping audit, not a blinded converter-quality comparison;
blinding remains required for the later multi-engine benchmark.

The audit also reconstructed all 1,411 published rectangles from their one- to
three-block Poppler evidence and checked normalized equality, confidence bands,
page order, geometry reuse, and selector bounds. BlobForge, catalog/lineage,
and independent Vulcan validation remained green for all ten artifacts.

## Quantitative findings

The automated canary mapped 1,411 of 2,355 Markdown blocks (59.9%) and 319,013
of 492,744 semantic bytes (64.7%). Structural validation found no invalid
artifact, span, or out-of-bounds published geometry. Accuracy inspection found:

| Confidence | Mappings | Normalized exact | Similarity below 0.90 | Length ratio below 0.80 |
| --- | ---: | ---: | ---: | ---: |
| 0.72–0.80 | 35 | 0 | 32 | 30 |
| 0.80–0.90 | 44 | 0 | 36 | 30 |
| 0.90–<1.00 | 456 | 0 | 11 | 9 |
| 1.00 | 876 | 841 | 9 | 4 |

Here, “length ratio” compares normalized Markdown and selected source text. It
is a useful coarse-selector signal, not a labeled correctness score. Formatting,
hyphenation, and apostrophe differences explain many non-exact high-confidence
matches.

The invariant audit found two page-order regressions and six reused-rectangle
groups involving 13 mappings. The latter include genuine split/join cases, but
also repeated labels such as “Qualities,” “Vulnerability,” “Telepathic,” and
“Unusual Anatomy” where distinct Markdown occurrences were attached to the
same printed occurrence. A confidence threshold alone cannot solve exact
repeated-text ambiguity.

## Manual findings

High-confidence unique text is generally excellent: headings, credits,
paragraphs, and footer text at confidence 1.0 were tightly localized in every
sampled document. Most mappings at or above 0.90 also selected the correct
content, with occasional excess text caused by coarse Poppler blocks.

The current lower band is not safe to publish as region-level evidence:

- `48e2191ac748…` mapped the Immense template's pool values to a rotated pool
  graphic on page 6, then returned to the correct page-2 `ENORMOUS (2)` heading.
  Candidate search jumped past a future trusted anchor.
- `bf6a7475c959…` similarly mapped “The Dusk Court does” to page 11 before the
  next Markdown heading mapped back to page 10.
- `d2f46fe213e8…` mapped two separate Markdown paragraphs to the same full
  `UNMASKED (SOCIAL)` box because Poppler exposed them as one block.
- several otherwise correct low-confidence mappings selected a full column or
  card while the Markdown span represented only a paragraph within it.
- exact repeated labels sometimes received confidence 1.0 at the wrong
  occurrence, demonstrating that lexical confidence is not calibrated source
  location confidence.

Low coverage in the condition-card and character-sheet documents is expected
from rotated, form-like, and spatial layouts and is preferable to forced
mappings. The problem is not low coverage itself; it is the unsupported
precision among mappings that were published.

## Required next iteration

1. Bound every unseeded candidate with the nearest trustworthy anchors on both
   sides, and reject unexplained page regressions.
2. Retain line/word boxes and derive the narrowest defensible union instead of
   always publishing whole Poppler blocks.
3. Calibrate page and rectangle confidence separately. Preserve a correct page
   selector without a rectangle when only page identity is defensible.
4. Track consumed evidence and repeated normalized text. Permit overlap only
   for an explicit split/join relationship recorded in diagnostics.
5. Add invariant tests for future-anchor jumps, repeated labels, coarse
   split-blocks, and page-only fallback.
6. Produce a new recipe digest, rerun the same ten documents, repeat this audit,
   then extend the canary to difficult born-digital tables, columns, sidebars,
   forms, and image-heavy pages before considering the full backfill.

The current ten outputs remain immutable local experiments. No production
object, conservative base MDAF, coordinator row, or source PDF was changed.
