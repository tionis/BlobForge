# PDF Enrichment Canary Review

Date: 2026-08-27

Status: recipe rejected for bulk backfill

Recipe:
`blake3:cf33db6438b2a2fbe1e44538bf05cb64a40bf9d88e3f211b1276933c580e1598`

## Decision

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
   then extend the canary to scans, tables, equations, sidebars, and image-heavy
   pages before considering the full backfill.

The current ten outputs remain immutable local experiments. No production
object, conservative base MDAF, coordinator row, or source PDF was changed.
