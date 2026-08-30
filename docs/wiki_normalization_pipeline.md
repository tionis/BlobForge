# Wiki Normalization Pipeline

Status: evaluation-ready composite recipes
Date: 2026-08-30

## Boundary and identity

Hosted extraction and wiki normalization are separate, versioned stages. A
composite run records its own canonical recipe digest in the artifact while
using the frozen provider recipe digest as the durable response-cache key.
Changing deterministic local normalization can therefore produce a new
immutable MDAF without buying the same provider conversion again.

The converter bundle ABI accepts multiple exact tools plus Markdown variant and
feature declarations. The shared builder remains the only component allowed to
package MDAF. Composite artifacts record the provider adapter and
`blobforge-wiki-normalizer` in the same conversion activity and declare
`raw-html` plus `semantic-html-table-v1`.

| Composite recipe | Digest | Provider cache recipe |
| --- | --- | --- |
| `mistral-ocr-4.1-wiki-v1.json` | `blake3:52d29542b2171c154f877d59e4e16019b85296ac4d12a6de97d2080a81a18dba` | `blake3:982a97ca1d45f5a0ac30dd8c7507efb594688d1b949f406ef4620f3352e723c7` |
| `datalab-convert-accurate-wiki-v1.json` | `blake3:fcc851f8e84d0c22e44200208ccd50d76319c5aec6d3bc1de6bc9b026d3ac502` | `blake3:c1dc8c06bf29a7a5f1639a4a0bdfc8be1250745d5f6e13438c68b1e38df9bc6f` |

## Evidence-specific behavior

Mistral supplies ordered typed blocks, page dimensions, and block geometry.
The wiki profile rebuilds each page from those blocks, removes provider-typed
headers and footers, removes only image blocks confined to the bottom ten
percent of a page with at most fifteen-percent page height, and converts typed
pipe-table blocks into strict semantic HTML.

Datalab supplies page-delimited Markdown and extracted image bytes but no
equivalent block geometry. Its profile therefore makes fewer claims: it removes
only a paragraph exactly duplicating the immediately preceding image alt text,
and removes recurring final small images only when at least half the pages (and
at least three pages) have dimensionally consistent images with shared
non-generic alt-text tokens. Parseable pipe-table paragraphs use the same
semantic serializer. Ambiguous content is retained.

Both profiles package only assets referenced by final Markdown. Native provider
responses remain immutable rendition evidence. Normalization occurs before
final UTF-8 page spans, outline derivation, and artifact identity calculation.

## Table safety

The serializer validates a non-overlapping rectangular logical grid, bounds
`rowspan` and `colspan` to positive integers no greater than 1,000, emits only
semantic table elements, and escapes provider text. Inline markup is limited to
`strong`, `em`, `code`, `sub`, `sup`, and `br`, with no attributes. Invalid or
ambiguous pipe tables remain unchanged rather than being reported as converted.

Blank provider cells following a non-empty cell are interpreted as a column
span. This is an evaluation hypothesis supported by the London Falling table
sample, not ground truth; the blinded composite campaign must confirm it and a
hidden holdout remains required before production routing.

## Real fixture result

Keyless replay of the already-paid eight-page London Falling cache produced:

| Profile | Artifact identity | Cleanup evidence |
| --- | --- | --- |
| Mistral wiki v1 | `blake3:1a6b3dad11b78eb1c2912bab9f87b6c23aeb77dde383a33c011bc183f8866534` | 8 headers, 13 footers, 5 footer images removed; 34 tables converted |
| Datalab wiki v1 | `blake3:bbed7f449c82e4c53f7aa552f1431ab434fe840f5c8b9d5c5159b6effc4b3fab` | 8 exact descriptions and 7 footer images removed; 18 tables converted |

Both artifacts retain eight exact page mappings, one referenced asset, pass
BlobForge and independent Vulcan validation, import into a temporary Vulcan
vault, and reproduce byte-for-byte on a second keyless cache replay. Vulcan
created 51 Mistral notes and 48 Datalab notes while preserving `<table>` and
`colspan`. The fresh blinded campaign is
`.blobforge-migration/evaluations/reviews/london-falling-tables-wiki-v2/` with
identity `blake3:efd4e84ff559de4e497fb51ae406b288f7de91224bb223288bc84fb0af8853ce`.

Two independently rated pages were sufficient to expose the repeated
structural distinction. Mistral-wiki scored 5.0 for tables and wiki utility;
Datalab-wiki scored 3.0 and 4.0. Datalab led hierarchy 5.0 to 3.0, while both
scored 5.0 for text, reading order, source mapping, and the one applicable asset
rating. Pages 3-8 remain unrated rather than receiving copied scores. The result
selects Mistral-wiki for the complex-table class despite its weaker heading
hierarchy.

## Remaining limitations

- The table serializer cannot infer trustworthy row spans from provider pipe
  Markdown; richer provider cell geometry needs another adapter boundary.
- Page-only mappings remain honest but do not locate individual cells.
- The Datalab furniture rule depends on recurrence and intentionally does
  nothing for short or inconsistent documents.
- Datalab may generate linguistically inconsistent alt text; the reviewed
  response inserted `阴森` into an English image description. Preserve this as
  native evidence, but do not treat provider captions as verified facts.
- Context-aware dingbat normalization and Docling screenshot suppression are
  separate work and are not part of these recipes.
- Managed model aliases remain unpinned, and production promotion still needs
  shared checkpoints, billing ledgers, holdout review, and routing policy.

## Prose-heavy Storypath regression

The same composite recipes were replayed without API keys against the cached
eight-page Storypath response. Mistral produced artifact
`blake3:5b1074c707e16069c8ea0172cd90557f57c4eee32c77ff4c886c0d96bca35568`.
Its profile removed 16 provider-typed footer blocks totaling 166 UTF-8 bytes:
page numbers and running titles such as `SHADOWS AND MIRRORS`, `MAKING COPIES`,
and `NEW EDGES`. A complete line diff found no other change. Markdown headings,
outline title/level pairs, image links, asset members, lists, emphasis, and body
prose remain identical. Eight page mappings and all 19 outline nodes remain.

Datalab produced artifact
`blake3:646bb02b391704d6f27af4e52eb0bc8ba01efc3c7c9d78a879bbd2d821ba36ea`.
Its final Markdown is byte-identical to the raw Datalab artifact: no description
was an exact duplicate paragraph, no image met the recurring-footer threshold,
and there were no parseable table blocks. This is correct fail-closed behavior
but does not repair Datalab's already-reviewed description/list defects.

Both composites reproduce byte-for-byte on a second keyless replay, pass
independent Vulcan validation, and import into a temporary vault as 20 notes
with 2 Mistral or 4 Datalab assets. Because Mistral's changes are exactly the
previously reviewed running footers and Datalab's text did not change, another
blinded campaign would duplicate existing human evidence. Mistral-wiki strictly
supersedes raw Mistral for this canary.
