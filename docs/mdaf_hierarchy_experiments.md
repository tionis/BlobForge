# TOC-led hierarchy experiments

2026-09-05. Nine distinct retained Mistral OCR responses across ten supplied
MDAF filenames were evaluated locally. No source PDF upload, provider request,
or production mutation was needed. Private books and artifact identities are
not committed as fixtures.

## Decision

Publish `mistral-ocr-4.1-wiki-v5.json`, normalization `wiki-v4`,
instead of modifying the frozen v4 recipe. This is major-section recovery for
reviewable wiki imports, not a guarantee of reproducing every printed part,
chapter, and subsection relationship. Following operator review, the worker
default and routing policy revision 3 now select v5. Production deployment and
historical replay remain pending; see [rollout guidance](routing_and_recipe_workers.md#wiki-v5-rollout-and-historical-migration).

The original geometry-led rules failed on combined/localized contents labels,
unlabelled multi-page tables, split chapter titles, and missing title blocks.
They also promoted large subsections while silently missing some TOC entries.

The new priority order is:

1. A compact, flat contents list, when supported by contents detection.
2. Numbered TOC chapter/appendix series, retaining introductory entries.
3. Explicit shallow TOC headings spanning the book, excluding partial heading
   evidence that only survives near the end of a flattened contents list.
4. Document-relative title geometry, always marked as requiring review.

Contents detection accepts language labels as hints, but also uses repeated
title/page-entry density. Continuation pages are clustered. TOC columns remain
separate when joining wrapped titles. Native multiline titles and adjacent
authored headings can align to one title without changing primary Markdown.
Single-line prefix matches cannot borrow an unrelated longer title's geometry.

## Alignment and uncertainty

Observed unique footer labels are preferred. At least three unique title
alignments and 75% agreement are required to infer a document-wide offset.
That inference is recorded in the hierarchy report; it never manufactures
observed labels or enables additional citation links. Page-only boundaries can
recover explicitly selected TOC entries when OCR missed their title. Leading
page prose/images stay with the section. Conflicting title/page evidence and
unmatched selected entries are reported rather than called complete recovery.

Geometry uses an upper title-size cohort, plus short/wide titles only when the
document's major-title cohort supports that style. We compared relative-height
ratios 0.55, 0.65, and 0.75. Eight books had identical selected boundaries across
that sweep; Chronicles of Darkness lost valid chapters at 0.75. We retained
0.65. Earlier experiments that used height alone, applied a wide-title rescue
globally, or treated appendices as a complete chapter series were rejected.

## Observed improvements

| Book | Frozen v4 major sections | TOC-led result |
| --- | ---: | --- |
| Changeling 2e | fallback | Seven chapters, four appendices, foreword and introduction |
| Chronicles of Darkness | 11 | Removes the spurious individual-scenario promotion; recovers introduction and wrapped major titles |
| Cortex Prime | 24 | Seven explicit TOC groups |
| Cthulhu 7 German | fallback | Sixteen chapters plus six back-matter entries |
| Curseborne Omnibus | 4 | All thirteen top-level TOC entries |
| London Falling | 5 | Six TOC entries, including player handouts |
| Shadowrun 5E Core | 15 | Sixteen major sections, retaining chapter/fiction boundaries |
| Rigger 5.0 | fallback | Contents cluster recognized; geometry-supported major sections recovered |
| Storypath Ultra | 7 | Seven major sections retained, including the split first chapter title |

Major counts exclude root and front matter. Count agreement alone is not a
semantic-quality score. The ratio comparison preceded the final wrapped-column
title repair, which additionally recovers a major Chronicles title.

## Reproduce and inspect

Final chapter imports (including root and front matter) contain 15/15/9/24/15/
8/18/15/9 notes respectively in the table's order. Asset checks cover 1,425
assets per depth across the nine books. The applied-import checks cover full,
non-overlapping byte spans, zero source bytes stranded in roots, identical
asset bytes, and existing targets for generated navigation and citation links.
Primary Markdown and retained native response bytes match the original MDAFs.
The Python suite passes 363 tests plus 5 subtests, and both distribution builds
pass. Vulcan's fmt, warning-free clippy, and complete workspace tests pass using
the already documented isolated login-profile workaround on this host.

```sh
uv run python scripts/evaluate_book_outlines.py /path/to/mdafs
uv run blobforge reprocess original.mdaf \
  --recipe blobforge/recipes/mistral-ocr-4.1-wiki-v5.json \
  --output upgraded.mdaf --source-name 'Known Book.pdf'
unzip -p upgraded.mdaf extensions/dev.tionis.blobforge/hierarchy.json
vulcan artifact import upgraded.mdaf --destination Books/Known \
  --hierarchy outline --through-level 2 --dry-run --output json
```

Use `--through-level 3` for first-level topics. New evaluator/worker selection
is `evaluate mistral-wiki-v5` / `recipe-worker --mistral-recipe v5`; the existing
rights, quotas, exact-job recipe, and cache controls still apply.

The full corpus is now development/regression evidence, not an independent
holdout. Remaining risks include erroneous OCR labels, mixed page-number
sequences, flattened TOC tiers, unusual typography, and partial title loss.
The geometry route cannot report a true missing-chapter rate without an
independently annotated hierarchy. Its evidence and diagnostics must remain
reviewable; Vulcan still explicitly selects the alternative outline authority.

MDAF v1 needs no schema or SPEC change: these are existing alternative outlines,
source intervals, immutable derivatives, and namespaced evidence reports.
Vulcan remains provider-neutral. Its bundled import skill and authority/depth
workflow remain applicable without a new command or provider-specific logic.
