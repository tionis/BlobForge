# Converter benchmark results

Date: 2026-08-29
Host tier: 32 GiB RAM, CPU-only  
Status: engineering canary; not a scored quality verdict

All reported outputs are MDAF v1 artifacts produced through ConversionBundle
v1 and accepted by both BlobForge and Vulcan. Model aliases are deliberately
reported as unpinned, so these runs are not production-reproducible recipes.

## Two-page fixture

| Engine | Elapsed | Text bytes | Words | Markdown headings | Table rows | Assets | Mapped pages |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Poppler | 0.9 s | 2,251 | 290 | 0 | 0 | 0 | 2/2 |
| Docling 2.122.0 standard | 40.2 s | 2,113 | 288 | 4 | 4 | 0 | 2/2 |
| Marker 1.10.2 | 175.6 s | 2,210 | 306 | 4 | 4 | 0 | 2/2 |
| Marker 2.0.0 no-OCR | 67.5 s | 2,085 | 297 | 4 | 0 | 0 | 2/2 |

Marker 2 no-OCR still performs local layout conversion and needed Datalab's font
asset on first use, but it does not start Surya's VLM inference backend. This is
a fast/layout control, not a substitute for the balanced OCR recipe.

## Eight-page rulebook canary

Source: `Storypath_Ultra_Tasty_Bit_03_Shadows_and_Mirrors.pdf`

| Engine | Elapsed | Text bytes | Words | Markdown headings | Assets | Mapped pages |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Poppler | 0.7 s | 35,340 | 4,369 | 0 | 0 | 8/8 |
| Docling 2.122.0 standard | 269.3 s | 26,404 | 4,267 | 18 | 2 | 8/8 |
| Marker 1.10.2 | 519.2 s | 25,910 | 4,264 | 18 | 2 | 8/8 |

The first Docling packaging pass revealed absolute temporary image links. The
adapter now rewrites those targets to `assets/...` before byte-span generation,
and the shared validator rejects absolute paths, `file:` targets, and references
to absent assets. The corrected artifact passed Vulcan and contains no temporary
path. The shared packager also now derives an aligned `outline.json` whenever an
adapter does not provide a richer native outline; this applies uniformly to
future converter runs.

Vulcan's independent `artifact import --dry-run` successfully planned the
corrected Docling book as one root plus 18 heading-derived notes and two assets
in a disposable vault. This exercises the intended MDAF-to-wiki consumer path,
not only archive validation.

## Blinded human review

The exact exports are retained as:

- `docs/evaluation_results/storypath-ultra-tasty-bit-03-page-1.review.json`;
- `docs/evaluation_results/storypath-ultra-tasty-bit-03-pages-2-8.review.json`.

Page 1 used the original campaign. Because its A/B/C assignment was then
disclosed, pages 2-8 used a new random assignment. The second export validates
against campaign
`blake3:f8183298733ee442bd2b3f52c7554e3dcbc5110052d349392121dbbf2a22c694`
and unblinds A as Docling 2.122.0, B as Marker 1.10.2, and C as Poppler. All
seven pages contain qualitative notes. Pages 2-4 have complete numeric ratings
(54 scores and 27 N/A values); pages 5-8 were described relative to that stable
baseline because repeating every selector was too cumbersome.

The following means combine the independently blinded numeric scores for PDF
pages 1-4. The original page-one asset rating is excluded because actual images
were not visible in that first UI.

| Dimension | Poppler | Docling | Marker 1 |
| --- | ---: | ---: | ---: |
| Text | 1.00 | 4.00 | 4.50 |
| Reading order | 1.00 | 4.75 | 4.50 |
| Hierarchy | 1.00 | 4.75 | 4.00 |
| Lists | 1.00 | 4.00 | 4.00 |
| Source mapping | 5.00 | 5.00 | 5.00 |
| Wiki utility | 1.00 | 4.00 | 4.75 |

Qualitative findings across the complete document:

- raw Poppler formatting is consistently unusable for the wiki despite correct
  page association;
- every converter exposes the dingbat list marker as `Y`; the embedded
  `FantasyRPGDings` font remains the leading explanation, but any repair must
  use font/layout evidence rather than globally replacing ordinary `Y` text;
- Marker preserves text and wiki-ready structure best overall, but repeatedly
  promotes headings to H1 and incorrectly starts a new list item for the
  cross-page continuation `wrong place`;
- Docling generally has the more restrained heading hierarchy and equally good
  reading order, but uses less accurate middle-dot symbols and flattens `Twin
  Talents` and `Ancestral Memory` onto the same heading level;
- neither structured converter represents the `Playing Your Evil Twin` boxed
  callout as a container around its two paragraphs;
- on page 8, Marker has substantially better structure and its 1632x1275 image
  is higher resolution than Docling's 611x470 extraction.
- a supplemental post-unblinding inspection confirms the reviewer's recollection
  that Marker preserves inline emphasis better: its Markdown contains 39 bold
  and 17 italic spans, including semantically useful list labels, ability names,
  headings, and quotations, while this Docling output contains none. Counts
  alone are not a quality score, so this remains separate from the blinded
  means and becomes an explicit dimension in the next rubric revision.

The reviewer now supports copying the previous page's ratings as an explicit
baseline while retaining page-specific notes, addressing the interaction cost
that caused pages 5-8 to remain qualitative.

## Interpretation

Poppler is the speed and text-layer recall control. It is roughly three orders
of magnitude faster than Marker 1 on the rulebook canary, but it supplies no
useful hierarchy or extracted images. The human review establishes Marker 1 as
the current quality leader for this one rulebook, especially for text, final
wiki utility, inline emphasis, page-eight structure, and image resolution.
Docling remains a serious candidate: it has better hierarchy and slightly
better reading order in the scored pages while taking about half as long. This
is not enough to select a default recipe: the sample contains no rated tables or
references, both engines have material structural defects, and both model
identities remain mutable.

Marker 2 no-OCR is runnable on this host. Marker 2 balanced still needs a pinned
Surya endpoint (`llama-server` for a CPU feasibility probe or a suitable GPU
server), and model snapshots/checksums must be frozen before any production
recipe is accepted.

No paid Mistral request was made. The frozen 9,465-page corpus would cost about
$37.86 at the configured $0.004/page standard ceiling or $47.33 at the
conservative annotated ceiling. The adapter requires both a page limit and a
dollar limit before it will submit a document.

## Next evaluation gates

- select and label representative hard pages and a hidden holdout;
- capture peak RAM, artifact bytes, failures, and cancellation/resume behavior;
- freeze local model directories and hashes;
- import canaries into a disposable Vulcan vault for blinded wiki review;
- then decide whether MinerU, PP-StructureV3, Datalab, or other hosted controls
  add enough independent value to justify their setup or spend.
