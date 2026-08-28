# Enrichment Backfill Operations

Status: `pdf-enrichment/v1` frozen and ready for resumable local backfill  
Date: 2026-08-28  
Recipe: `blake3:0e7e6c1ba4bb6a8920a58cd08fe3c957bd48b729cbccc5733ffec3d47876a569`

## Scope and safety

The frozen recipe targets born-digital illustrated PnP rulebooks with usable
embedded text. It never overwrites a source PDF or conservative legacy MDAF.
Every result is a recipe-keyed derived artifact, and interrupted or failed work
remains resumable. Image-only and scan-heavy PDFs are outside this recipe's
applicability rather than silently routed through OCR.

The canonical recipe is packaged at
`blobforge/recipes/pdf-enrichment-v1.json`. Runtime startup fails closed unless
the installed `pdftotext` version is exactly 25.03.0. Read-only status and
verification remain available on a mismatched host. Performance-only process
count and large-document thresholds do not affect artifact identity.

## Size-aware execution

Concurrent enrichment uses isolated processes so peak memory can be attributed
to one document. A PDF is classified as large when it has at least 300 pages or
is at least 64 MiB. At most one large document runs at once; an ordinary
document may use the other slot. The 32-GiB CPU host should use two processes:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --no-sync blobforge migrate enrich \
  --all --jobs 2 --fail-fast
```

`--large-pages` and `--large-mib` are operational overrides. Increasing
`--jobs` above two on this host is not approved by the canary evidence.

Page classification uses `pdfinfo` once and caches the count in
`legacy_pdf_metadata`; source objects are immutable, so no TTL is needed.

Poppler can copy raw C0 control glyphs from a PDF into its otherwise valid
XHTML. XML 1.0 forbids those bytes, so BlobForge removes only
`00-08`, `0B`, `0C`, and `0E-1F` before parsing while retaining TAB, LF, CR,
and every other byte. The removed count is included in native PDF evidence as
`normalization.xhtml_forbidden_c0_bytes_removed`; the field is absent on the
ordinary zero-removal path. This is a compatibility correction under the same
frozen recipe: it is byte-for-byte inert for every previously successful
input, and formerly invalid inputs had no artifact identity to conflict with.

## Attempt telemetry

`legacy_enrichment_attempts` is append-only and records each attempt's status,
start/finish time, elapsed seconds, sampled process-tree peak RSS, PDF pages,
output bytes, and failure. Sampling includes the isolated Python process and
its `pdftotext` children. When `psutil` is unavailable, Linux/macOS process
high-water RSS is retained as a conservative fallback.

`enrich-status` reports measured documents/pages, summed process-seconds,
maximum per-document peak RSS, output bytes, and pages per process-hour.
`enrich-verify` cross-checks successful telemetry against the archive size and
retained Poppler page evidence. Artifacts created before instrumentation remain
valid and show as unmeasured until explicitly rerun.

The real three-document telemetry canary measured:

| Source class | Pages | Source | Elapsed | Peak RSS | Output |
| --- | ---: | ---: | ---: | ---: | ---: |
| Small rulebook | 8 | 9.4 MiB | 6.2 s | 51.8 MiB | 0.4 MiB |
| Ordinary omnibus | 70 | 17.9 MiB | 45.9 s | 138.0 MiB | 3.5 MiB |
| Large-bytes Cortex book | 256 | 104.7 MiB | 138.5 s | 354.2 MiB | 34.8 MiB |

All three retained their prior MDAF identities. The scheduler ran the large
book alongside only ordinary work, and all 15 reviewed canary artifacts passed
the post-run verifier.

## Monitoring and recovery

```bash
uv run --no-sync blobforge migrate enrich-status
uv run --no-sync blobforge migrate enrich-verify
```

Stopping the runner does not damage completed outputs. A killed attempt may
leave a `processing` row and partial destination; the next run selects every
non-converted row, creates a new attempt, and atomically replaces the partial
file. Do not delete prior recipe rows or conservative artifacts during
recovery. After the run, require zero processing/failed rows and a completely
valid read-only verification before publication is considered.

The first complete-corpus pass exposed this Poppler control-glyph condition.
Failures remain append-only evidence. Restart the runner after deploying the
normalizer: it selects failed and interrupted rows, preserves completed
artifacts, and appends a successful retry attempt rather than erasing history.
