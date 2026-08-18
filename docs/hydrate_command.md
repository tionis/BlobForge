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
3. Runs remote preflight as incremental reconciliation:
   - When a coordinator is configured, the local done-set mirror is reconciled
     against the coordinator's `GET /api/v1/jobs/done-since` watermark
     endpoint: the client stores `(since_ms, cursor)` and each run pulls only
     hashes that completed after the last sync (keyset-paginated over
     `(completed_at, file_hash)`), merges them into the local mirror, then
     answers membership entirely locally. Content-addressed outputs are
     immutable, so known-done hashes never need re-querying and there is no
     status TTL.
   - `--refresh-status` discards the local mirror and watermark and re-syncs
     the done-set from scratch.
   - If a coordinator is not configured, falls back to the legacy S3 done-hash
     index scan or per-hash existence checks.
4. When conversion output exists, downloads `<hash>.zip` through a
   coordinator-issued signed URL (deduplicated per hash during one run).
5. Writes:
   - `<stem>.md`
   - `<stem>.assets/` (sibling directory)

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
- **Done-set mirror** — the full set of content hashes known to have completed
  conversions (`done_hashes`) plus a `(since_ms, cursor)` watermark in a
  `meta` table. The mirror is append-only: content-addressed outputs never
  expire, and entries are dropped only when a signed download proves the
  output is gone. The pre-watermark `hash_status` table is dropped on open.

Location is `~/.cache/blobforge/hash_index.sqlite3`, overridable with
`BLOBFORGE_CACHE_DIR` (directory) or `BLOBFORGE_HASH_INDEX_PATH` (file path).

This is a practical form of set reconciliation: instead of re-transferring the
whole candidate set each run, the client holds a local snapshot of the
done-set and pulls only the coordinator-side delta since the last watermark.
A full range-based reconciliation protocol (IBLT/Merkle-style) was considered
and rejected as overkill — the candidate payload is only ~2 MB for tens of
thousands of hashes, and the dominant costs on repeat runs are re-reading
unchanged files and re-querying the full set, both of which the local index
eliminates. The watermark sync keeps the coordinator exchange proportional to
new completions rather than to the candidate set size.

## Asset Path Rewriting

Worker output markdown references images under `assets/...`.  
During hydration, those references are rewritten to `<stem>.assets/...` so multiple hydrated PDFs in the same directory do not collide on a shared `assets/` folder.

## Safety and Idempotency

- Markdown is written via atomic file replacement.
- Asset extraction uses a staging directory before final placement.
- `--dry-run` reports intended writes without changing local files.
- Archive download is cached by hash to avoid repeated network fetches for duplicate files.
- Remote checks are deduplicated by hash; a completed output whose signed
  download fails is dropped from the local done-set mirror so it is not
  retried as available on every run.

## Exit Semantics

- Returns `0` when no runtime errors occur (even if some PDFs are missing conversion output).
- Returns `1` if one or more hydration operations fail due to I/O/archive errors.
