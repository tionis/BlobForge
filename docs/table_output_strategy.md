# Table Output Strategy

Status: composite implementation, Vulcan import, and human review passed
Date: 2026-08-30

## Evidence

The blinded London Falling table campaign establishes two distinct quality
classes on three independently rated pages:

| Engine | Table mean | Wiki utility | Principal limitation |
| --- | ---: | ---: | --- |
| Mistral OCR 4.1 | 5.0 | 4.0 | Repeated page header in body text |
| Datalab Convert accurate | 4.0 | 4.0 | Image descriptions bleed into body text |
| Marker 1.10.2 | 1.0 | 1.0 | Table structure is unusable |
| Docling 2.122.0 | 1.0 | 1.0 | Table structure is unusable; adds screenshots |

Pages 4-8 were not assigned reconstructed numeric scores. The reviewer reported
that they repeated the same outcome, which is retained as qualitative evidence.
The local engines are therefore rejected for this table class; Mistral is the
provisional table backend and Datalab is the hosted fallback/challenger.

## Primary representation

Pipe-table Markdown remains appropriate only for a rectangular table in which
every logical row has the same cell grid and no cell spans multiple rows or
columns. It cannot faithfully represent `colspan` or `rowspan`.

When spans or multi-level headers are required, BlobForge should emit a narrow,
sanitized HTML table inside `text.md`:

- allow only `table`, `caption`, `thead`, `tbody`, `tfoot`, `tr`, `th`, and
  `td` elements;
- allow only bounded positive-integer `colspan` and `rowspan`, plus `scope` on
  header cells;
- escape all provider text and reject style, script, event, URL, and arbitrary
  attributes;
- prefer semantic header cells and sections over layout-only markup;
- never use HTML merely to reproduce fonts, colors, borders, or page geometry.

MDAF v1 fixes the primary member to `text/markdown` but leaves Markdown variant
and feature declarations open. It does not prohibit raw HTML. BlobForge now has
a fixture proving that its validator and Vulcan's import/decomposition path
preserve the allowlisted table and `colspan` attributes without flattening
them. Composite artifacts explicitly declare the raw-HTML feature. The blinded
reviewer reconstructs only allowlisted table elements and attributes through
DOM operations; it never assigns provider HTML to `innerHTML`. Human rendering
review remains the final gate.

## Normalization pipeline

Table processing must occur before final UTF-8 source-map spans are calculated:

1. retain the provider-native table/block representation as immutable evidence;
2. remove repeated page headers and footer logos only when page-position and
   recurrence evidence identifies them, never by a global text/image rule;
3. isolate generated image descriptions as asset metadata or captions instead
   of unmarked body paragraphs;
4. construct a logical cell grid and validate that spans form a non-overlapping
   rectangle;
5. choose pipe Markdown for simple grids and allowlisted HTML for merged cells;
6. serialize deterministically, then compute outline and source-map byte spans;
7. retain table-level page/region mappings and native cell geometry. Publish
   finer mappings only where the final cell text can be aligned honestly.

If the grid is invalid or ambiguous, preserve a readable image as a secondary
asset and native evidence, but do not present it as successful structured table
extraction. The primary Markdown must carry an explicit diagnostic or a
conservative text fallback.

## Routing consequence

For born-digital rulebooks with complex tables, route to Mistral when external
processing rights, privacy policy, and cost ceiling permit it. Datalab remains
the fallback after description isolation. Marker remains a useful local option
for ordinary prose/list-heavy rulebooks but is not a table fallback. Docling
table screenshots may be retained as secondary evidence, not as structured wiki
content.

The consumer fixture and focused composite review have passed. Mistral-wiki is
the selected complex-table recipe: it scored 5.0 for tables and wiki utility on
both rated pages and consistently converted the relevant tables. Datalab-wiki
scored 3.0 for tables and 4.0 for wiki utility; its hierarchy was better, but
most inconsistent provider grids correctly remained unconverted. Production
routing remains gated on a hidden holdout, shared cache/billing infrastructure,
and managed-model provenance. Existing immutable MDAFs are not rewritten.
