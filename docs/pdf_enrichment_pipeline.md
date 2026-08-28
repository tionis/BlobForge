# PDF Markdown Enrichment Pipeline

Status: `pdf-enrichment/v1` frozen for born-digital legacy backfill

Date: 2026-08-28

Related: `conversion_program_roadmap.md`, `local_mdaf_migration.md`,
`mdaf_redesign.md`, `converter_adapter_architecture.md`

## Decision

Build PDF enrichment as a reusable, versioned pipeline consuming either
historical Markdown or new converter output. It returns structured evidence to
the shared MDAF builder; it does not own packaging or silently rewrite the
converter's Markdown.

Its first production use is a derived-artifact backfill over the 1,377 legacy
Marker results. The same modules then become Marker 1 and Marker 2 recipe stages
and a fallback for converters without reliable native mappings.

## Implemented baseline

The first vertical slice implements:

- `document-evidence/v1` Python types for PDF pages/blocks and Markdown blocks;
- Poppler bbox-layout extraction with exact version identity, point geometry,
  page dimensions, and a sanitized lossless JSON rendition;
- loss-aware Markdown segmentation bound to final UTF-8 byte spans;
- legacy-evidence seeding, token-indexed monotonic search, bounded fuzzy
  refinement, ambiguity rejection, and page/rectangle mappings;
- page-bound clipping for Poppler content outside crop bounds while retaining
  raw native coordinates and reporting every clipping event;
- an extension report with block/byte coverage and rejection diagnostics;
- new derived MDAFs with exact lineage and retained base evidence/assets;
- recipe-keyed SQLite resumability, verification, bounded multi-hash canaries,
  and an explicit `--all` gate for bulk work;
- stricter BlobForge provenance validation aligned with Vulcan.

The first experimental recipe was
`blake3:cf33db6438b2a2fbe1e44538bf05cb64a40bf9d88e3f211b1276933c580e1598`.
Recipe identity changes with output-affecting alignment, geometry, or search
policy, leaving earlier experiments auditable but unselected.

The first real canary processed 10 rulebooks / 153 pages with zero failed
artifacts. All ten pass BlobForge validation, catalog/lineage verification, and
independent Vulcan validation. Aggregate alignment covered 1,411 of 2,355
Markdown blocks (59.9%) and 319,013 of 492,744 semantic Markdown bytes (64.7%).
Per-document byte coverage ranges from 9.4% to 98.0%; this variance helps expose
unsuitable document classes and is not proof of mapping accuracy.

The first systematic manual review rejected this recipe for bulk use. Exact and
high-confidence unique matches were generally precise, but the lower confidence
bands contained whole-block rectangles for partial Markdown spans, two
wrong-page/order regressions, and exact repeated labels attached to reused
source geometry. See `pdf_enrichment_canary_review.md` for the protocol,
measurements, examples, and required next iteration. The ten derived artifacts
remain experimental evidence; the complete backfill stays gated.

## Corrected candidate

Recipe
`blake3:0e7e6c1ba4bb6a8920a58cd08fe3c957bd48b729cbccc5733ffec3d47876a569`
implements the required second iteration:

- nearest preceding and following trusted anchors bound candidate pages;
- all newly published mappings are page-monotonic;
- Poppler line and word IDs/geometry are retained in native evidence;
- word sequence refinement derives a narrow region from disjoint evidence;
- fuzzy prose regions are limited to one source block and must pass separate
  score, Markdown-token-coverage, and normalized-length thresholds;
- a strong page match with insufficient geometry emits a page-only method;
- exact whole-block equivalence may use clipped block geometry;
- an independent publication validator rejects page regressions, duplicate
  rectangles, method/selector disagreement, and report/count disagreement.

Its generation-2 recipe identity includes every output-affecting threshold and
policy. The report contract remains compatible while adding explicit region
and page-only mapping counts. Previous recipe outputs remain immutable local
experiments and are not silently selected.

The 15-document/1,957-page born-digital canary passed all structural,
catalog/lineage, BlobForge, and independent Vulcan checks. It has zero page
regressions and zero duplicate published rectangles; 51 visually reviewed
mappings were correct at their advertised precision. Repeat conversion of the
original ten documents produced identical MDAF identities. See
`pdf_enrichment_canary_review.md` for measurements and adjudication details.

The candidate is explicitly a born-digital PDF recipe for illustrated
pen-and-paper rulebooks with usable embedded text. It does not invoke OCR and
is not expected to enrich image-only or scan-heavy sources. OCR support, if
later needed in BlobForge, must be a distinct recipe with its own model,
provenance, cost, and acceptance corpus rather than an invisible fallback.

The publication policy is accepted for this declared scope. Runtime and
peak-memory telemetry, append-only attempts, cached page classification, and
size-aware isolated-process scheduling passed a real three-document canary.
The recipe is frozen and the explicit `--all` backfill is now authorized. See
`enrichment_backfill_operations.md` for measurements and recovery procedures.

## Contract

Required inputs are the exact source PDF, exact base Markdown, base artifact
identity when applicable, retained native converter evidence, and a frozen
enrichment recipe.

The intermediate result contains:

- unchanged or explicitly normalized final Markdown;
- ordered Markdown blocks with final UTF-8 byte spans;
- PDF pages, dimensions, text runs/blocks, geometry, and coordinate metadata;
- span-to-source mappings with method and confidence;
- a complete aligned outline when its publication bar is met;
- additional structures and relationships in a versioned extension;
- coverage, ambiguity, rejection, validation, and activity diagnostics.

Only the shared builder writes `text.md`, `source-map.json`, `outline.json`,
renditions, extensions, and `provenance.json`.

## Stages

### PDF evidence extraction

Extract page indexes, dimensions, rotation and boxes; native text at available
granularity and geometry; fonts and styles; images, drawings, links,
annotations, bookmarks and printed labels; reading order, columns, tables,
figures, captions, equations, furniture and sidebars; and, only for a distinct
OCR-capable recipe, page render fingerprints for scan/OCR fallback.

Retain lossless sanitized native evidence. Normalized coordinates always
declare unit and origin. Page-only evidence is useful; missing geometry is not
approximated as a full-page rectangle.

### Markdown segmentation

Parse the exact final Markdown into headings, paragraphs, list items, tables,
code, equations, image references, captions, and HTML blocks while maintaining
a UTF-8 byte-offset ledger. Calculate offsets after authorized normalization
and asset-link rewriting. Parser characters or Python string indexes must not
be published as byte offsets without conversion against final bytes.

### Candidate generation and alignment

Generate candidates using, in descending strength:

1. preserved Marker page anchors;
2. native converter block IDs or page boundaries;
3. exact TOC/bookmark-to-heading matches;
4. normalized exact text matches;
5. locality-constrained fuzzy matches;
6. optional OCR evidence only in a separately identified OCR-capable recipe.

Comparison may normalize Unicode, whitespace, ligatures, hyphenation, soft
hyphens, punctuation, and line breaks, but published spans always target final
Markdown bytes and original source locators.

Use monotonic, page-windowed sequence alignment. Strong anchors constrain
windows; scoring considers text, order, type, style, page proximity, and
geometry. Support split/joined paragraphs without forcing every block to
match. Repeated furniture, page numbers, table cells, and short phrases receive
ambiguity penalties.

### Structure and relationships

Derive or preserve heading hierarchy and section extents, reading order and
grouping, list nesting, semantic table structure, captions, references,
bookmarks, printed labels, and page furniture.

Core MDAF v1 receives only schema-supported information. Stable block IDs,
relationships, rejected candidates, confidence components, and other details
belong in a versioned
`extensions/dev.tionis.blobforge.pdf-enrichment/` member or native rendition;
do not add undeclared properties to core JSON.

### Publication filtering

Publish the narrowest precision supported by evidence:

- Markdown span to page and region when geometry is defensible;
- span to page only when page identity is strong but geometry is not;
- no mapping when unsupported or ambiguous.

Every mapping uses a stable namespaced method and, when meaningful, calibrated
confidence. Confidence does not excuse a known-bad mapping. Partial and
overlapping mappings are valid when they represent genuine evidence.

Emit `outline.json` only when all nodes have valid heading/section byte spans
and the hierarchy is complete. Keep partial native outlines in the extension.

## Identity and provenance

Recipe identity includes the PDF extractor, OCR model/revision, Markdown
parser, normalization, candidate and alignment logic, thresholds, furniture and
structure policies, confidence calibration, publication rules, outline and
reference policies, extension schema, MDAF schema, and packager version.

Performance settings remain runtime provenance unless proven output-affecting.
A legacy-derived artifact identifies its exact base artifact. Historical Marker
and model versions remain unavailable.

The activity graph distinguishes historical conversion, current PDF evidence
extraction, alignment/structure analysis, and MDAF packaging. The new process
must never appear to be recovered historical conversion provenance.

## Validation and measurement

Reject spans outside `text.md` or splitting UTF-8, invalid page intervals or
coordinates, non-finite/out-of-bounds geometry, unknown sources/assets, invalid
outline topology/spans, unstable method names, and inconsistent identities.

Report Markdown bytes and blocks mapped at page and region precision; unmapped
content by type and reason; ambiguity and confidence distributions;
monotonicity/overlap rejections; furniture decisions; OCR fallback pages and
model identity; outline completeness; time and peak resources; and repeated-run
digests. Measure labeled accuracy separately from coverage—incorrect extra
mappings are a regression.

## Canary and backfill

The current canary uses 10-20 complete born-digital documents spanning columns,
tables, rotated layouts, sidebars, unusual fonts, full-page art, and known
failures. Deeply review selected pages while complete books expose cross-page
behavior. Image-only scans are outside this recipe's declared applicability.

Freeze `pdf-enrichment/v1` only when:

- BlobForge and Vulcan validate all artifacts;
- sampled page mappings meet the agreed accuracy threshold;
- geometry has no systematic coordinate errors;
- unmapped and ambiguous content remains visible;
- deterministic repeat runs have stable identity;
- interruption/restart does not corrupt or duplicate output;
- sources and base artifacts remain recoverable and unchanged.

Bulk execution is append-only and resumable. Each row records source, base
artifact, enrichment recipe, attempt, derived artifact, validation, metrics,
and error. Failure never replaces the conservative legacy artifact. Publication
and selection remain separate so derived results can be audited first.

Implemented local commands are:

```bash
# Refuses an accidental unbounded run.
uv run blobforge migrate enrich --workspace .blobforge-migration

# One source, a bounded pending batch, or an explicit multi-document canary.
uv run blobforge migrate enrich SHA256 --workspace .blobforge-migration
uv run blobforge migrate enrich --limit 10 --workspace .blobforge-migration
uv run blobforge migrate enrich SHA256_A SHA256_B --jobs 2 \
  --workspace .blobforge-migration

# Current recipe coverage and read-only artifact/catalog audit.
uv run blobforge migrate enrich-status --workspace .blobforge-migration
uv run blobforge migrate enrich-verify --workspace .blobforge-migration

# Deliberately gated full backfill; wait for manual canary approval.
uv run blobforge migrate enrich --all --jobs 2 \
  --workspace .blobforge-migration
```

## Reuse and future media

Marker 1 and Marker 2 share segmentation, structure, mapping filters,
diagnostics, and packaging. Docling, Datalab, and Mistral adapters preserve
their potentially stronger native evidence and use common alignment only to
fill gaps or bind native blocks to final Markdown. Do not flatten rich evidence
to Markdown and reconstruct it afterward.

The boundary generalizes beyond PDFs: timed transcripts can map document byte
spans to time selectors, and images to regions. PDF extraction therefore
remains a module rather than a worker-wide assumption.
