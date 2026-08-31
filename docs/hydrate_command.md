# Hydrate Command Design

## Objective

`blobforge hydrate` materializes completed conversion outputs back into the local filesystem next to source PDFs.  
It is intended for libraries that have already been ingested and converted in BlobForge.

## Behavior

Given one or more input paths (PDF files and/or directories), the command:

1. Recursively discovers PDF files (`.pdf`, case-insensitive).
2. Runs local preflight:
   - Skips files where `<stem>.md` already exists unless `--force` is set.
   - Computes SHA256 using a persistent local SQLite index first: files are
     keyed by `(path, size, mtime_ns)`, so unchanged files are reused without
     re-reading them on any filesystem. Only index misses fall through to the
     existing xattr-aware hash path (`compute_sha256_with_cache`).
3. Runs artifact-aware remote preflight:
   - The coordinator's bulk status response includes every retained artifact
     for each source, independently of the job's current mutable state. A
     retained artifact can therefore be hydrated while the source is queued,
     failed, or processing under another recipe.
   - `--recipe-digest` chooses an exact retained recipe. Without it, hydration
     uses the job's selected recipe when that artifact exists, or the sole
     retained artifact. Multiple unmatched artifacts fail closed instead of
     selecting newest-by-time.
   - Older coordinators retain their done-watermark compatibility path;
     `--refresh-status` only affects that legacy path.
   - If a coordinator is not configured, falls back to the legacy S3 done-hash
     index scan or per-hash existence checks.
4. Downloads the exact selected artifact through a coordinator-issued signed
   URL, deduplicated by source hash and recipe during one run. `mdaf/v1`
   packages are staged with a `.mdaf` suffix, fully validated, and read from
   `text.md`; historical archives use their legacy `content.md` member.
5. By default, writes:
   - `<stem>.md`
   - `<stem>.assets/` (sibling directory)

   With `--format textpack`, writes one `<stem>.textpack` directly instead.
   This is a deliberately lossy projection of MDAF into TextBundle v2: the
   Markdown and assets are retained, while the source artifact identity,
   recipe digest, and artifact type are recorded in the
   `dev.tionis.blobforge` metadata extension. The MDAF remains the canonical
   provenance-bearing artifact in BlobForge.

The command requires `BLOBFORGE_COORDINATOR_URL` and
`BLOBFORGE_COORDINATOR_TOKEN` (an admin token created in the management UI) or
the equivalent `--coordinator-url` / `--token` flags. No S3 credentials are
needed.

## Local Persistent Index

Hydration maintains a SQLite database (WAL mode) that makes repeat runs fast:

- **File hashes** — keyed by `(path, size, mtime_ns)` with nanosecond
  precision. This removes the dependency on filesystem extended-attribute
  support (the xattr cache silently misses on mounts without `user_xattr`),
  so unchanged files are never re-read.
- **Legacy done-set mirrors** — retained only for compatibility with old
  coordinators that cannot return artifact-aware bulk status. Current
  coordinators do not use this mirror because job completion is mutable while
  retained artifacts are immutable and recipe-scoped.

Location is `~/.cache/blobforge/hash_index.sqlite3`, overridable with
`BLOBFORGE_CACHE_DIR` (directory) or `BLOBFORGE_HASH_INDEX_PATH` (file path).

Bulk status is chunked at 5,000 source hashes per request. This transfers the
recipe-scoped artifact catalog needed for deterministic selection; a one-bit
done mirror cannot represent that information safely.

## Asset Path Rewriting

Worker output markdown references images under `assets/...`.  
During hydration, those references are rewritten to `<stem>.assets/...` so multiple hydrated PDFs in the same directory do not collide on a shared `assets/` folder.

## Safety and Idempotency

- Markdown is written via atomic file replacement.
- Asset extraction uses a staging directory before final placement.
- `--dry-run` reports intended writes without changing local files.
- Archive download is cached by source hash and recipe to avoid repeated
  network fetches for duplicate files without conflating recipe variants.
- Remote checks are deduplicated by hash. A completed output whose signed
  download is rejected definitively (coordinator 404/409, i.e. the output is
  gone) is dropped from the local done-set mirror so it is not retried as
  available on every run; transient download failures keep the mirror entry so
  the next run retries.

## Examples

```bash
# Current selected recipe, or the sole retained artifact
blobforge hydrate ./library

# One exact immutable recipe
blobforge hydrate ./library --recipe-digest blake3:<digest>

# Direct TextBundle v2 output without intermediate Markdown/assets
blobforge hydrate ./library --format textpack
```

## Exit Semantics

- Returns `0` when no runtime errors occur (even if some PDFs are missing conversion output).
- Returns `1` if one or more hydration operations fail due to I/O/archive errors.

## Hydrated Output Maintenance

Large recursive libraries can replace the two-file/folder hydrated layout with
one portable archive, or remove the hydrated outputs entirely. Both maintenance
operations are anchored to discovered PDFs and only consider a sibling
`<stem>.md`; unrelated Markdown and `.assets` directories are left alone.

```bash
# Preview deletion of hydrated Markdown/assets under a tree
blobforge hydrated clean ./library

# Apply the deletion
blobforge hydrated clean ./library --execute

# Preview replacement by one <stem>.textpack per hydrated PDF
blobforge hydrated textpack ./library

# Create each TextPack, validate it, then remove its source Markdown/assets
blobforge hydrated textpack ./library --execute

# Replace existing TextPacks; without --force they are skipped safely
blobforge hydrated textpack ./library --execute --force

# Preview or restore TextPacks to <stem>.md and <stem>.assets/
blobforge hydrated unpack ./library
blobforge hydrated unpack ./library --execute

# Replace existing unpacked outputs
blobforge hydrated unpack ./library --execute --force

# Preview or remove generated TextPacks
blobforge hydrated clean-textpacks ./library
blobforge hydrated clean-textpacks ./library --execute
```

The TextPack operation implements the TextBundle v2 compressed format. Each
ZIP-based `.textpack` contains lowercase `text.md`, `info.json`, and `assets/`;
Markdown references are changed from `<stem>.assets/...` back to
`assets/...`. Creation uses a temporary file in the destination directory and
validates ZIP CRCs and required metadata before atomic replacement. Only then
are the sibling Markdown and assets removed. Symbolic links anywhere in the
asset tree are rejected so packaging cannot read outside that tree.

`unpack` performs the reverse operation. It accepts a TextBundle v2 Markdown
body named `text.*`, validates metadata and ZIP CRCs, rejects duplicate,
unexpected, traversal, and non-regular asset members, stages extracted assets,
and rewrites `assets/...` links to `<stem>.assets/...`. Existing Markdown or
assets cause a safe skip unless `--force` is supplied. The `.textpack` is
removed only after restoration succeeds, so a bad archive remains available
for diagnosis or recovery. `clean-textpacks` removes PDF-anchored TextPacks
without touching PDFs or unrelated archives.

The `blobforge hydrated` command is deliberately a command group so additional
local maintenance operations can be added without expanding the top-level CLI.
