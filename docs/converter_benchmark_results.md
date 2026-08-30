# Converter benchmark results

Date: 2026-08-30
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
- `docs/evaluation_results/storypath-ultra-tasty-bit-03-pages-2-8.review.json`;
- `docs/evaluation_results/storypath-ultra-tasty-bit-03-inline-formatting-supplement.json`.

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
- the reviewer observed while still blinded that one structured candidate
  preserved inline emphasis better, but omitted the note from the browser
  export. Unblinding attributes that candidate to Marker. A subsequent artifact
  check supports the observation: Marker contains 39 bold and 17 italic spans,
  including semantically useful list labels, ability names, headings, and
  quotations, while this Docling output contains none. No numeric score was
  reconstructed after export; the observation is retained as qualitative
  blinded evidence and becomes an explicit dimension in the next rubric.

The reviewer now supports copying the previous page's ratings as an explicit
baseline while retaining page-specific notes, addressing the interaction cost
that caused pages 5-8 to remain qualitative.

## Enterprise CPU host

The Debian 13 enterprise desktop has 16 logical CPUs, 31 GiB RAM, 31 GiB swap,
and 524 GiB free disk. A secret-free checkout and isolated uv environments were
staged under `/home/eric/blobforge-eval`; source PDFs were already present in
`/home/eric/rulebooks`.

On the same eight-page Storypath source, Docling completed in 115.0 seconds and
Marker 1 in 100.8 seconds. Markdown, source maps, assets, and native evidence
were byte-identical to the earlier artifacts. Their MDAF identities differ only
because the current shared packager includes the derived `outline.json` in the
activity outputs, while the retained older canaries predate that provenance
fix. These enterprise timings are the fair isolated comparison.

The 98-page `Shadowrun - London Falling.pdf` was selected as a table/stat-block
stress book. Docling completed a valid artifact in 1,015.9 seconds while Marker
overlapped for part of the run. Independent Vulcan validation reports 98/98
mapped pages, 1,723,253 Markdown bytes, 115,960 words, 467 outline nodes, 1,311
table rows, and 163 assets. Docling used roughly 3.0-3.5 GiB RSS. Marker
completed in 3,260.3 seconds (54m20s), with 98/98 mapped pages, 819,143
Markdown bytes, 65,546 words, 487 outline nodes, 1,584 table rows, and 89
assets. It reached approximately 13.8 GiB RSS.

The nearly twofold Docling/Marker word-count difference is not itself a quality
signal: it may represent better retention, duplicated content, synthetic
captions, or overcapture. A blinded review-v2 campaign over PDF pages 12, 23,
31, 38, 64, 78, 90, and 92 is the adjudication gate.

Concurrency proved technically safe because the host retained about 10 GiB
available plus swap, but it is not the intended scheduler policy. One large
Marker conversion at a time is the safe 32-GiB default. The overlapped
full-book wall times are stress/throughput observations, not isolated engine
benchmarks.

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

## Hosted eight-page canary

Mistral OCR 4.1 processed all eight Storypath pages in 6.9 seconds. Its native
usage reports 8 pages / 9,909,035 bytes and its published list-price estimate
is $0.032. The API response does not expose the account's actual billed amount
or credit attribution. Canonical artifact identity is
`blake3:cb906843d778f3328175fa869251e39520015d1aec8bddef59b9cab5915112e8`.

Datalab Convert accurate completed provider processing in 13.12 seconds. Its
response reports 8 pages and `final_cost_cents=6`, so the billed amount was
$0.06 within the $0.10 ceiling. Contrary to current SDK documentation, this
response omitted `list_cost_cents`; list price and credit/discount attribution
remain unavailable. Canonical artifact identity is
`blake3:2071347f7728035763d51c2de451dd6fde7c0542fb9e30891f3abc5e4982522f`.

Both artifacts pass BlobForge and independent Vulcan validation with 8/8 exact
page mappings and 19 outline nodes. Mistral has 26,262 Markdown bytes, 4,266
words, 19 headings, and 2 assets. Datalab has 27,484 Markdown bytes, 4,465
words, 19 headings, and 4 assets. These counts need blinded review.

The first Mistral live packaging exposed response-object insertion-order drift:
cache replay had identical semantic content but a different native JSON member
digest. Native response serialization is now key-sorted. The initial unscored
artifact/campaign were preserved under `/tmp`, and two keyless cache packages
are byte-identical at the canonical identity above. Datalab also produced
byte-identical keyless replays. No request was repurchased during either fix.

The frozen 9,465-page corpus would cost about
$37.86 at the configured $0.004/page standard ceiling or $47.33 at the
conservative annotated ceiling. The adapter requires both a page limit and a
dollar limit before it will submit a document. The new Datalab accurate
evaluator requires the same operator controls and enforces Datalab's 200 MB /
7,000-page limits before upload, but its dollar ceiling is necessarily checked
against returned billing because Convert exposes no preflight quote.

## Hosted Storypath review v2

The complete eight-page, four-candidate export for campaign
`blake3:4f10cea83474b0a728199b05707d5eb3188bb0854bc798759c9aeb2cf5a900cc`
was transcribed from the reviewer export and accepted by the strict campaign
validator: 8/8 pages, 252 numeric ratings, 68 N/A values, and all 320 slots
complete. Unblinding maps A to Marker 1.10.2, B to Mistral OCR 4.1, C to
Datalab Convert accurate, and D to Docling 2.122.0.

| Dimension | Marker 1 | Mistral | Datalab | Docling |
| --- | ---: | ---: | ---: | ---: |
| Text | 4.625 | **5.000** | **5.000** | 4.625 |
| Inline formatting | **5.000** | **5.000** | **5.000** | 3.000 |
| Reading order | 5.000 | 5.000 | 5.000 | 5.000 |
| Hierarchy | 4.000 | 4.000 | 4.000 | 4.000 |
| Lists | **5.000** | 4.750 | 4.500 | **5.000** |
| Assets | 4.143 | **4.857** | 3.714 | 4.714 |
| Source mapping | 5.000 | 5.000 | 5.000 | 5.000 |
| Wiki utility | 4.000 | **5.000** | 3.000 | 4.000 |

Tables and references were absent and scored N/A. The equal source-mapping
scores describe page association, not geometry precision.

Mistral is the quality leader for this canary. It alone recovered the title
cleanly, decoded ornamental RPG glyphs usefully, tied for best text and inline
formatting, and led assets and wiki utility. Its output still includes repeated
page-number/title footers and one page used non-Markdown list symbols, so it
needs header/footer suppression and context-aware list normalization before a
production default.

Marker remains a strong, zero-marginal-cost local candidate. It produced valid
Markdown lists throughout and preserved formatting, but left some ornamental
glyphs as `Y`, emitted one rogue list marker, and had inconsistent asset crops.
Docling also produced valid lists and good assets, but lost meaningful inline
formatting and had lower-resolution logo extraction. Datalab's high-resolution
assets and descriptions do not compensate for descriptions bleeding into body
text, nonstandard `❖` list markers, and a useless full-page raster asset. Its
current recipe is not suitable for wiki ingestion without isolating descriptions
as asset metadata and filtering page screenshots.

## London Falling table adjudication

The blinded table/stat-block campaign did not yield a numeric export because
both outputs failed the acceptance purpose: neither represented the tables in
a usable structured form. After recording that verdict, unblinding mapped A to
Marker 1.10.2 and B to Docling 2.122.0. Docling embeds the tables as images,
which preserves some human readability but no queryable/editable cell
semantics; Marker's attempted conversion is also unusable. Neither is an
acceptable table-heavy wiki recipe as currently configured. A table-focused
hosted challenger trial is therefore required before routing rules can be
selected.

## London Falling hosted table challenger

The follow-up fixture contains original PDF pages 12, 23, 31, 38, 64, 78, 90,
and 92 as fixture pages 1-8. Poppler `pdfseparate`/`pdfunite` 25.03.0 produced
the 1,992,639-byte PDF at BLAKE3
`a897bae6f0af77a9379d3acc17e232032f1131e00024f572410ed0e783948f39`.
Each fixture/source page pair rendered byte-identically as a 72-DPI PNG. The
ignored fixture provenance record retains the exact source digest, fixture
SHA-256/BLAKE3, construction tools, and page map.

| Engine | Elapsed | Text bytes | Words | Headings | Table rows | Assets |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Marker 1.10.2 | 736.1 s | 133,915 | 6,096 | 47 | 303 | 4 |
| Docling 2.122.0 | 192.8 s | 391,233 | 21,404 | 46 | 235 | 18 |
| Mistral OCR 4.1 | 7.5 s | 45,750 | 5,867 | 50 | 299 | 6 |
| Datalab Convert accurate | 25.2 s | 96,977 | 6,554 | 47 | 299 | 8 |

All four MDAFs pass independent Vulcan validation with 8/8 page mappings.
Mistral cost $0.032 at published list price; its response does not expose the
account charge. Datalab reported an exact $0.06 final charge. Both successful
responses were cached before packaging and replay without credentials. The
first Datalab package exposed native-response JSON key-order drift between the
live HTTP object and the sorted cache. Canonical key-sorted native
serialization now makes replays byte-identical; the unreviewed first package
was superseded without another provider request.

Counts are especially unsafe as a verdict here. Mistral and Datalab each emit
299 apparent table rows while Marker emits 303, yet the prior human review
already established that Marker's table structure is unusable. Docling emits
fewer rows but over eight times Mistral's Markdown bytes. The blinded campaign
must judge actual header/cell associations, duplication, omissions, description
bleed, and editability.

Fresh review-v2 campaign
`blake3:9a366ab22d1557b1f665b7c76f08ab90db14b670ad0c4d823ed043a8a6b0d3a1`
contains all four candidates on fixture pages 1-8. Its public bundle passed an
engine-name scan and its private key is mode `0600`. Do not inspect the key
until a complete export is available.

The reviewer completed pages 1-3 and reported that pages 4-8 repeated the same
result. The partial export was strictly validated before unblinding: 3/8 pages,
80 numeric ratings, 36 N/A values, and 116/320 completed slots. No numeric score
is extrapolated onto the five unrated pages. A=Marker 1.10.2, B=Mistral OCR 4.1,
C=Datalab Convert accurate, and D=Docling 2.122.0.

| Dimension | Marker 1 | Mistral | Datalab | Docling |
| --- | ---: | ---: | ---: | ---: |
| Text | 5.000 | 5.000 | 5.000 | 5.000 |
| Reading order | 5.000 | 5.000 | 5.000 | 5.000 |
| Hierarchy | 4.000 | 3.667 | **5.000** | 3.000 |
| Tables | 1.000 | **5.000** | 4.000 | 1.000 |
| Assets | **5.000** | 4.000 | 2.000 | 3.000 |
| Source mapping | 5.000 | 5.000 | 5.000 | 5.000 |
| Wiki utility | 1.000 | **4.000** | **4.000** | 1.000 |

Asset means cover pages 2-3; page 1 was left blank because its irrelevant logo
and table screenshots made a single asset score ambiguous. Inline formatting,
lists, and references were N/A on all three pages.

Mistral produced the best tables, with Datalab close behind. Marker and Docling
are rejected for this table class: rewriting is easier than repairing their
table structures. Mistral still embeds a repeated page header. Datalab again
bleeds image descriptions into body text. Mistral, Datalab, and Docling extract
irrelevant footer logos, while Docling additionally embeds redundant table
screenshots. Marker extracted only the relevant image on the rated asset pages.

The reviewer identified a representation gap rather than merely an extraction
gap: merged headers need `colspan`, which pipe-table Markdown cannot express.
The provisional output decision is sanitized semantic HTML tables inside
Markdown for grids requiring row/column spans, after a Vulcan/renderer
compatibility fixture. See `table_output_strategy.md`.

### Wiki-normalized table challenger

The same cached responses were replayed through composite wiki-v1 profiles with
both provider keys removed. Mistral removed 8 typed headers, 13 typed footers,
and 5 bottom-furniture images while converting 34 tables. Datalab isolated 8
exact duplicated descriptions and removed 7 recurring small footer images while
converting 18 parseable tables. Both now package one referenced asset, retain
8/8 page mappings, pass independent Vulcan validation/import, and reproduce
byte-for-byte on repeat cache replay.

The normalized identities are
`blake3:1a6b3dad11b78eb1c2912bab9f87b6c23aeb77dde383a33c011bc183f8866534`
for Mistral and
`blake3:bbed7f449c82e4c53f7aa552f1431ab434fe840f5c8b9d5c5159b6effc4b3fab`
for Datalab. Structural counts now treat HTML `<tr>` as rows and exclude table
tag names from word counts; removing one Markdown separator row per converted
table explains the logical-row totals of 265 and 281 versus 299 raw pipe lines.
These automated checks do not replace the new blinded human campaign.

The follow-up campaign was stopped after two fully rated pages because the same
structural result repeated. Strict validation accepted 26 ratings and 14 N/A
values (40/160 slots); pages 3-8 were not assigned synthetic scores. A is
Mistral-wiki and B is Datalab-wiki.

| Dimension | Mistral wiki v1 | Datalab wiki v1 |
| --- | ---: | ---: |
| Text | 5.0 | 5.0 |
| Reading order | 5.0 | 5.0 |
| Hierarchy | 3.0 | **5.0** |
| Tables | **5.0** | 3.0 |
| Assets | 5.0 | 5.0 |
| Source mapping | 5.0 | 5.0 |
| Wiki utility | **5.0** | 4.0 |

Mistral is the selected complex-table backend: all relevant tables used the
reviewed HTML representation, whereas Datalab had only one correct HTML table
with spans and retained its other inconsistent pipe grids. Datalab's stronger
heading hierarchy does not outweigh that gap for this routing class. Its image
alt text also contains an unexplained `阴森` token in an otherwise English
description; this comes from the provider response rather than normalization.

### Storypath wiki-profile regression

Keyless cache replay produced Mistral-wiki
`blake3:5b1074c707e16069c8ea0172cd90557f57c4eee32c77ff4c886c0d96bca35568`
and Datalab-wiki
`blake3:646bb02b391704d6f27af4e52eb0bc8ba01efc3c7c9d78a879bbd2d821ba36ea`.
Both pass Vulcan validation/import and deterministic replay.

Mistral's Markdown shrank from 26,262 to 26,096 bytes and from 4,266 to 4,239
measured words. The exact diff contains only 16 provider-typed running-footer
blocks (166 bytes); headings, outline titles/levels, image links, assets, lists,
inline formatting, and all body prose are unchanged. This directly repairs the
footer defect recorded in the original blinded review without creating a new
quality tradeoff.

Datalab remains exactly 27,484 Markdown bytes and 4,465 words because none of
its Storypath defects met the conservative structural cleanup rules. A new
blinded review was not generated: it would compare unchanged Datalab content
and a Mistral candidate differing only by already-adjudicated running footers.
For this prose canary, Mistral-wiki strictly supersedes raw Mistral and remains
the hosted quality-tier leader; Marker 1 remains the local/privacy fallback and
still leads standards-compliant list syntax.

## Next evaluation gates

### Storypath list-normalization canary

Mistral wiki-v2 freezes context-aware list normalization at recipe
`blake3:bdd3e060e88f64277834245a42528a54b6b077774123c3806bdd827cf8ea3026`.
A keyless replay of the same eight-page response produced validated MDAF
`blake3:aedfe70488c3a376371e64e368dd51b2c3e224d1cf8aa4cea8ad1a23e30e4f0d`.
It removed 20 decorative glyphs that followed existing Markdown list markers
and recovered 10 items from two-or-more consecutive glyph-leading provider
`text` blocks. Inspection confirmed that inline `At ♦` mechanics and headings
containing `• TO ••` remain. This repairs the hosted Storypath list defects
without a global glyph or font-character substitution.

The first routing policy now selects this exact recipe only for explicitly
authorized, sufficiently funded, born-digital English/undetermined-language
rulebook canaries. It remains advisory until hidden-holdout and coordinator
production canaries pass.

- select and label representative hard pages and a hidden holdout;
- capture peak RAM, artifact bytes, failures, and cancellation/resume behavior;
- freeze local model directories and hashes;
- import canaries into a disposable Vulcan vault for blinded wiki review;
- validate the sanitized-HTML table representation through Vulcan and the
  intended renderer, then add table/header/footer/asset cleanup fixtures;
- add shared cleanup stages for repeated headers/footers, context-aware list
  glyphs, and asset-description isolation;
- then decide whether MinerU, PP-StructureV3, or other controls add enough
  independent value to close the remaining table gap.
