# Hydrate Command Design

## Objective

`blobforge hydrate` materializes completed conversion outputs back into the local filesystem next to source PDFs.  
It is intended for libraries that have already been ingested and converted in BlobForge.

## Behavior

Given one or more input paths (PDF files and/or directories), the command:

1. Recursively discovers PDF files (`.pdf`, case-insensitive).
2. Runs local preflight:
   - Skips files where `<stem>.md` already exists unless `--force` is set.
   - Computes SHA256 using the existing xattr-aware hash path (`compute_sha256_with_cache`).
3. Runs remote preflight:
   - Loads the manifest once and uses manifest hashes to prefilter candidates.
   - Resolves done availability per unique candidate hash.
   - For large candidate sets, uses a done-hash index scan to avoid one-by-one HEAD checks.
4. When conversion output exists, downloads `<hash>.zip` (deduplicated per hash during one run).
5. Writes:
   - `<stem>.md`
   - `<stem>.assets/` (sibling directory)

## Asset Path Rewriting

Worker output markdown references images under `assets/...`.  
During hydration, those references are rewritten to `<stem>.assets/...` so multiple hydrated PDFs in the same directory do not collide on a shared `assets/` folder.

## Safety and Idempotency

- Markdown is written via atomic file replacement.
- Asset extraction uses a staging directory before final placement.
- `--dry-run` reports intended writes without changing local files.
- Archive download is cached by hash to avoid repeated network fetches for duplicate files.
- Remote checks are deduplicated by hash and prefiltered with manifest data.

## Exit Semantics

- Returns `0` when no runtime errors occur (even if some PDFs are missing conversion output).
- Returns `1` if one or more hydration operations fail due to I/O/archive errors.
