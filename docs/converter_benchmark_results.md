# Converter benchmark results

Date: 2026-08-27  
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

## Interpretation

Poppler is the speed and text-layer recall control. It is roughly three orders
of magnitude faster than Marker 1 on the rulebook canary, but it supplies no
useful hierarchy or extracted images. Docling and Marker 1 are nearly identical
in coarse word, heading, and asset counts on this book; Docling took about half
as long. These counts do not establish reading-order, table, equation, caption,
or wiki quality. A blinded review of labeled hard pages remains mandatory.

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
