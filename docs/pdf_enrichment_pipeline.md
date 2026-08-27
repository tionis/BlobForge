# PDF Markdown Enrichment Pipeline

Status: proposed design for review

Date: 2026-08-27

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
figures, captions, equations, furniture and sidebars; and page render
fingerprints for scan/OCR fallback.

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
6. OCR evidence for pages without useful native text.

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

The canary uses 10-20 complete documents spanning native text, scans, columns,
tables, equations, sidebars, unusual fonts, full-page art, and known failures.
Deeply review selected pages while complete books expose cross-page behavior.

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

## Reuse and future media

Marker 1 and Marker 2 share segmentation, structure, mapping filters,
diagnostics, and packaging. Docling, Datalab, and Mistral adapters preserve
their potentially stronger native evidence and use common alignment only to
fill gaps or bind native blocks to final Markdown. Do not flatten rich evidence
to Markdown and reconstruct it afterward.

The boundary generalizes beyond PDFs: timed transcripts can map document byte
spans to time selectors, and images to regions. PDF extraction therefore
remains a module rather than a worker-wide assumption.
