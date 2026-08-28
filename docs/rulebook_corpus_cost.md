# Priority Rulebook Corpus Conversion Cost

Inventory date: 2026-08-27  
Corpus: `/home/eric/rulebooks/rulebooks`  
Method: read-only `pdfinfo`, file-size, encryption, and SHA-256 duplicate checks

## Summary

- 43 readable, exact-byte-distinct PDF paths;
- 9,465 pages and 1,234.58 MiB;
- the two redundant Rigger 5.0 variants identified in the preceding inventory
  have been removed, leaving `Shadowrun_5E_Rigger_5.0_with_bookmarks.pdf` as the
  current selected source;
- one encrypted PDF, `Cthulhu_7_Grundregelwerk.pdf`, which permits local copy
  and print extraction but still requires API preflight;
- every individual PDF is below the reviewed Mistral 512 MiB and Datalab 200
  MiB request/file limits.

The first inventory contained 17 PDFs and 3,060 pages. The current directory
retains 15 of those, adds 28 paths / 6,952 pages, and no longer contains the two
Trinity Continuum books (547 pages). This is therefore a changed corpus, not
strictly the old corpus plus additions.

## Deduplication and scope

The preceding 45-path inventory found that these two paths had the same SHA-256:

- `Shadowrun_5E_Rigger_5.0_in_Tits-o-Vision.pdf`;
- `Shadowrun_5E_Rigger_5.0_with_bookmarks.pdf`.

The byte-distinct `Shadowrun_5E_Rigger_5.0.pdf` was also another 194-page
variant. Both redundant variants are now absent, and only the bookmarked source
remains. Reconfirm this selection in the frozen corpus manifest rather than
depending on directory contents alone.

BlobForge's canonical BLAKE3 ingestion will replace the temporary SHA-256
duplicate check. API adapters must cache by `(source_digest, recipe_digest)` so
retries, aliases, and renamed files do not create accidental charges.

## Inventory by family

This table uses exact-byte-deduplicated pages. Mistral columns use the published
standard $4/1,000-page and conservative annotated $5/1,000-page rates. Google
uses Layout Parser at $10/1,000 pages and AWS uses the published US West
Layout+Tables example at $0.015/page.

| Family | Unique PDFs | Pages | Mistral standard | Mistral annotated | Google Layout | AWS Layout+Tables |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Call of Cthulhu | 1 | 436 | $1.74 | $2.18 | $4.36 | $6.54 |
| Chronicles of Darkness | 19 | 5,093 | $20.37 | $25.47 | $50.93 | $76.40 |
| Cortex Prime | 1 | 256 | $1.02 | $1.28 | $2.56 | $3.84 |
| Shadowrun 5 | 13 | 2,918 | $11.67 | $14.59 | $29.18 | $43.77 |
| Storypath Ultra | 9 | 762 | $3.05 | $3.81 | $7.62 | $11.43 |
| **Total** | **43** | **9,465** | **$37.86** | **$47.33** | **$94.65** | **$141.98** |

Prices exclude tax, currency conversion, storage/egress, engineering time, and
provider-specific minimums. AWS is region-dependent. Google and AWS return
structured document data, not a ready MDAF or necessarily Markdown, so their
adapter work is greater than Mistral's.

## Mistral OCR 4.1

OCR 4.1 (`mistral-ocr-4-1`) is now the pinned Mistral candidate. Its OCR response
supports page Markdown, paragraph blocks and bounding boxes, structural labels,
block confidence, images, page dimensions, and returned model/usage metadata.
Those ordinary OCR fields appear sufficient for the initial MDAF rendition and
source map; custom structured annotations are not required merely to obtain
blocks or block confidence.

Current official pages state:

- $4 per 1,000 standard pages;
- $5 per 1,000 annotated pages;
- a general pricing-table Batch rate of $0.40 per 1,000 OCR pages;
- elsewhere, Batch documentation describes a 50% discount.

The two Batch statements conflict, and no separate annotated Batch rate is
published in the reviewed material. Do not use the apparent $0.40 rate as a
budget assumption until a small metered OCR 4.1 batch confirms the account's
invoice. Preserve the returned usage and compare it with the provider dashboard.

Official references:

- <https://docs.mistral.ai/models/ocr-4-1>
- <https://docs.mistral.ai/inference/pricing>
- <https://docs.mistral.ai/studio/batch-processing>
- <https://docs.mistral.ai/api/endpoint/ocr>
- <https://docs.mistral.ai/resources/known-limitations>

## Budget scenarios

All scenarios use the current 9,465 exact-byte-distinct pages.

| Scenario | Calculation | Expected charge / cap |
| --- | ---: | ---: |
| One Mistral standard pass | 9,465 × $0.004 | $37.86 |
| Standard plus 10% retry margin | $37.86 × 1.10 | $41.65 |
| One conservative annotated pass | 9,465 × $0.005 | $47.33 |
| Annotated plus 10% retry margin | $47.33 × 1.10 | $52.06 |
| Two standard stability passes plus 10% | $37.86 × 2 × 1.10 | $83.29 |
| Two annotated stability passes plus 10% | $47.33 × 2 × 1.10 | $104.12 |
| Mistral standard + Google Layout, 10% | ($37.86 + $94.65) × 1.10 | $145.76 |
| Mistral annotated + Google Layout, 10% | ($47.33 + $94.65) × 1.10 | $156.17 |
| Google Layout only | 9,465 × $0.010 | $94.65 |
| AWS Layout+Tables only | 9,465 × $0.015 | $141.98 |
| Advertised Mistral $0.40 Batch rate | 9,465 × $0.0004 | $3.79, unverified |
| Generic documented 50% Batch discount | $37.86 × 0.50 | $18.93, conservative |

If the two absent Trinity books are restored, add 547 pages: the corpus becomes
10,012 pages and costs $40.05 for Mistral standard, $50.06 annotated, $100.12
for Google Layout, or $150.18 for AWS Layout+Tables.

The operator has a low-cost Mistral subscription with promotional API credits.
Those credits can fund adjudication and whole-book batches over successive
quota periods, reducing immediate cash expenditure. They do not change the
normalized list-price figures above. Evaluation records must keep list-price
cost, actual billed amount, and credits applied as separate fields, and retain
hard page/spend ceilings even when a batch is expected to be fully credited.
Account balances, expiry details, and credentials remain private runtime
configuration rather than repository or MDAF metadata.

## Other provider controls

Datalab/Chandra pricing remains dashboard-dependent. Official documentation
confirms per-page billing, a $5 trial, 200 MiB / 7,000-page request limits, and
one-hour result retention. Use its trial on the adjudication set, capture every
response immediately, and require a dashboard-observed per-page rate or quote
before approving all 9,465 pages.

Google Enterprise Document OCR alone is much cheaper than Layout Parser, but it
is not an equivalent Markdown/layout conversion. Google lists the first 1,000
monthly pages at no charge and $1.50/1,000 pages thereafter at the reviewed
tier. Treat it as an OCR baseline, not a replacement for the $96.59 Layout
control.

Official references:

- <https://documentation.datalab.to/docs/recipes/conversion/conversion-api-overview>
- <https://cloud.google.com/products/document-ai/pricing>
- <https://aws.amazon.com/textract/pricing/>

## Recommended spend plan

1. Freeze the current corpus manifest, confirm the bookmarked Rigger source, and
   decide whether to restore Trinity.
2. Run free local/deterministic MDAF baselines first.
3. Use 5-10 adjudication pages per unique book/edition for API preflight. At
   215-430 pages, ordinary Mistral costs only $0.86-$1.72 before retries.
4. Submit a small Mistral OCR 4.1 Batch job and verify whether the account is
   billed at $0.40/1,000, 50% of standard, or another rate.
5. If response fields and data-processing terms pass review, run one complete
   standard Mistral pass with a hard $55 cap. This also covers the conservative
   annotated estimate plus retry margin.
6. Do not pay for a second complete Mistral pass until the adjudication set shows
   that nondeterminism needs measurement; raise the cumulative cap to $110 if
   it does.
7. Keep Google Layout behind a separate $110 cap and AWS behind a $160 cap.
   Their extra engineering cost means they should first demonstrate unique
   quality on difficult pages.
8. Keep Datalab within its $5 trial until exact billing is observed.

Every successful API response should be sanitized and packaged into a validated
MDAF immediately so it is never purchased twice.
