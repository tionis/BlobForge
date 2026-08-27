# Local legacy ZIP to MDAF migration

## Safety boundary

The migration is deliberately split into a local build phase and a future
publication phase. The implemented commands never contact or mutate S3.
`rclone copy` created an immutable local input mirror; generated MDAFs, model
caches, the SQLite catalog, and reports live below the git-ignored
`.blobforge-migration/` directory. There is no upload or deletion command yet.

The 2026-08-27 snapshot was copied with:

```bash
rclone copy blobforge:pdf .blobforge-migration/remote/pdf \
  --metadata --fast-list --transfers 8 --checkers 16 --progress --immutable
```

`copy`, rather than `sync`, is intentional: it cannot delete remote objects.
The completed mirror contains 3,634 objects / 31.98 GiB. Its relevant payload
is 1,808 SHA-256-addressed raw PDFs and 1,377 legacy output ZIPs; all 1,377 ZIPs
have a matching PDF.

The local migration completed with 1,377 converted and zero failed artifacts.
The verified stage contains 1,377 source objects and 1,377 MDAFs. Its canonical
legacy recipe digest is
`blake3:8822289b4860301f73b64a2139a3559f2026793a48135fc13b83bc84a67b0c39`;
the staged run-manifest digest is
`blake3:8cb0de0459044c53c2038192af2f8a8e438d9a33c4c4c9502d81f930140fd213`.

The conservative migration is now also an immutable input to the separately
versioned pipeline in `pdf_enrichment_pipeline.md`. Enrichment creates derived
MDAFs below each source's `generated/.../enriched/` directory and never rewrites
the 1,377 baseline artifacts. The first automated canary has 10 valid derivatives
under recipe
`blake3:cf33db6438b2a2fbe1e44538bf05cb64a40bf9d88e3f211b1276933c580e1598`;
manual review rejected that recipe for unsupported region precision and
repeated/wrong-page matches. The remaining 1,367 are intentionally pending a
corrected recipe and successful repeat review. The ten derivatives remain
immutable experimental evidence.

## Workspace

```text
.blobforge-migration/
  remote/pdf/                    read-only rclone mirror
    store/raw/<sha256>.pdf
    store/out/<sha256>.zip
  catalog.sqlite3                resumable source/artifact state
  generated/<bb>/<blake3>.legacy.mdaf
  migration-manifest.json        checksummed publication input
  corpora/                       frozen evaluation manifests
  evaluations/                  local converter artifacts
  models/                        local model downloads
```

The catalog uses WAL mode. Each legacy artifact is independently `pending`,
`converted`, or `failed`, with its verified source identity, output path, MDAF
identity, and last error. Re-running inventory is an idempotent upsert;
re-running conversion skips completed rows. The default remains one conversion
at a time. `--jobs` is capped at four, and the tested bulk setting is two
because each task may hold expanded legacy assets while constructing and
validating its deterministic archive.

## Commands

```bash
# Refresh local inventory after a resumed rclone copy.
uv run blobforge migrate inventory

# Canary one legacy SHA-256 or a bounded batch.
uv run blobforge migrate legacy <sha256> --fail-fast
uv run blobforge migrate legacy --limit 20

# Resume every remaining pair locally.
uv run blobforge migrate legacy
uv run blobforge migrate legacy --jobs 2  # tested on the 32-GiB host

# Read every generated artifact back and cross-check its MDAF identity, source
# BLAKE3, and retained legacy SHA-256 alias against the SQLite catalog.
uv run blobforge migrate verify

# Export the catalog used by a future publication/cutover tool.
uv run blobforge migrate report

# Materialize the proposed v2 key tree locally after full verification.
uv run blobforge migrate stage

# Build a bounded enrichment canary, inspect aggregate coverage, and read every
# derivative back against source/base/recipe/catalog identities.
uv run blobforge migrate enrich --limit 10
uv run blobforge migrate enrich-status
uv run blobforge migrate enrich-verify
```

Every conversion verifies that the raw bytes match the SHA-256 object name,
calculates canonical BLAKE3, builds the artifact atomically, reads it back, and
validates schemas, member hashes, UTF-8 byte spans, activity output ownership,
and logical identity. A representative real artifact was also accepted by the
Vulcan source checkout's independent `vulcan artifact validate` implementation.
The separate `verify` pass is read-only, so it is safe to run while conversion
is in progress and cannot accidentally rewrite resumability state.

## What is preserved

The MDAF primary `text.md` is based on the old Marker Markdown. The original
`content.md` and `info.json` remain as opaque members below
`renditions/dev.tionis.blobforge.legacy/`; extracted images remain declared
`assets/`. Source records contain canonical `blake3:` and the verified old
`sha256:` alias. The source PDF is not duplicated inside each artifact because
the immutable local/remote source object remains addressable.

Historical conversion provenance is honest:

- `marker-pdf` version is `unavailable`, not inferred from current packages;
- the historical model is `unavailable` and therefore yields the expected
  reproducibility warning;
- the packaging activity records the exact current BlobForge version and
  migration parameters;
- secret-like legacy metadata stops conversion for manual redaction.

## Page evidence and outlines

Some Marker outputs can contain `<span id="page-N-M"></span>` boundaries. The
migrator removes those from final Markdown and records exact, zero-based,
half-open page selectors against final UTF-8 byte spans.

The production legacy ZIPs inspected so far were rendered without that Marker
option. They do contain Marker TOC entries with `page_id` and polygons. The
migrator aligns only normalized exact TOC-title/Markdown-heading matches and
emits page plus point-coordinate polygon selectors for the heading span. It
does not claim that an entire page was aligned. Unmatched text remains
unmapped. This is intentionally more conservative than fabricating page spans
from `page_stats`, which contain counts but no Markdown positions.

An alternative outline is derived from every final Markdown heading. TOC source
locators are attached only to exact matches. Native Marker page statistics and
all unmatched evidence remain in the legacy rendition for later alignment work.
Headings whose visible title becomes empty after stripping embedded HTML are
retained in `text.md` but omitted from `outline.json`, whose schema requires a
non-empty title.

## Publication and S3 v2

Publication is a separate, not-yet-enabled operation. Its inputs will be only
`converted` rows from the checksummed migration manifest. Before any write it
must create and verify a coordinator backup, confirm bucket versioning/retention,
and obtain a lease- or migration-run-bound upload URL. After upload it must read
the object back, validate it again, and only then add the coordinator artifact
row. Old objects remain untouched throughout the dual-read rollback window.

The selected relative v2 key layout is:

```text
store/v2/sources/blake3/ab/<source-hex>
store/v2/recipes/blake3/ab/<recipe-hex>.json
store/v2/artifacts/mdaf/v1/blake3/ab/<artifact-hex>/<attempt-id>.mdaf
store/v2/checkpoints/<attempt-id>/<stage>/blake3/<digest-hex>
store/v2/migrations/<run-id>/manifest.json
```

The configured bucket prefix (currently `pdf/`) remains outside these relative
keys. The database is authoritative for media types and source-to-artifact
relationships; key parsing is never the index. Attempt-qualified artifact keys
prevent an unverified upload from overwriting a previously validated artifact.

`migrate stage` requires every paired legacy artifact to be converted and all
converted outputs to pass the independent verifier. It then hard-links (or,
across filesystems, copies) sources and artifacts into `staged-v2/` using these
exact relative keys, writes one canonical legacy migration recipe, and writes a
checksummed run manifest. This creates an inspectable input for a future
coordinator-aware publisher; it is deliberately not an invitation to run a raw
`rclone sync`, because object upload and coordinator rows must be committed as a
validated, recoverable workflow.
