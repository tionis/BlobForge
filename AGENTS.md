# Agent Instructions

All LLM agents operating in this repository MUST adhere to the following protocols:

- **Documentation:** Document all architectural decisions, system components, and significant logic in the `docs/` directory.
- **Task Management:** Use `TODO.md` in the root directory to store, track, and look up all pending and completed todo items.
- **Activity Logging:** Log all actions, tool executions, and progress in `docs/WORK_LOG.md`.
- **Knowledge Sharing:** Note all significant findings, codebase insights, or updated protocols directly in this file (`AGENTS.md`) under the "Findings" section.

## Development Environment

This project uses **uv** as the Python package manager. Always use `uv` commands instead of `pip` or `python -m pip`:

```bash
# Install the project in development mode
uv pip install -e .

# Install with optional dependencies
uv pip install -e ".[metrics]"      # For psutil system metrics
uv pip install -e ".[convert]"      # For marker-pdf conversion
uv pip install -e ".[all]"          # All optional dependencies

# Install as a CLI tool
uv tool install .

# Run commands in the virtual environment
uv run blobforge --help
uv run python -m pytest tests/

# Add a new dependency
uv add <package>

# Sync dependencies
uv sync
```

The virtual environment is located at `.venv/` and should be activated automatically by uv or can be activated manually with `source .venv/bin/activate`.

## Findings

- **2026-07-16:** Replaced the Cloudflare coordination implementation with a Bunny-native design after the platform decision changed. A standalone Bunny Edge Script now provides the stable HTTP API, IndieAuth + PKCE management UI, sessions, and authorization; Bunny Database (managed libSQL/SQLite, currently public preview) stores files, queue state, fenced leases, retries, workers, config, logs, sessions, and audit records. Atomic `UPDATE ... RETURNING` claims prevent double assignment across globally distributed stateless script instances. Because Bunny Edge Scripting has no background alarm equivalent, expired leases are recovered lazily and atomically before claims/snapshots or through the UI, which is sufficient for polling external workers and preserves scale-to-zero operation. Python coordinator environment variables and API paths remain unchanged, so clients need only point `BLOBFORGE_COORDINATOR_URL` at the Bunny script. The old `cloudflare/` project and deployment documentation were removed and replaced by `bunny/` plus `docs/bunny_coordination_backend.md`.

- **2026-07-15:** Implemented an optional Cloudflare coordination plane for BlobForge: one SQLite-backed Durable Object owns persistent file/job state, priority claims, fenced expiring leases, retries/dead-letter state, worker registry, configuration, alarms, sessions, and audit records; Bunny/S3 remains the raw/output blob store. The Worker also serves an IndieAuth + PKCE management UI restricted to `https://eric.wendland.dev/`. Python ingestion, workers, status, config, and CLI reads select this backend when `BLOBFORGE_COORDINATOR_URL` and `BLOBFORGE_COORDINATOR_TOKEN` are set, while retaining legacy S3 fallback. A separately authenticated, repeatable `coordinator-migrate` command preserves the existing S3 backlog and terminal states; old processing locks are safely requeued. Workers detect output-upload/completion-call ambiguity and finalize an existing content-addressed ZIP without reconversion.

- **2026-07-09:** Hardened worker scheduled aborts after a marker native crash (`corrupted double-linked list`) killed the whole worker and left job `f829c114cc29...` in `PROCESSING`. `--abort-outside-window` now automatically enables isolated marker conversion in a child process, and the new `--isolate-conversion` worker flag can be used independently to contain native marker/PyTorch crashes. The parent worker owns the S3 lock/heartbeat, enforces child-process timeouts, requeues on schedule boundary, and records ordinary child failures without dying.
- **2026-07-08:** Added local worker run windows. `blobforge worker --run-window HH:MM-HH:MM` gates job acquisition by the worker machine's local time; the flag may be repeated or comma-separated and supports midnight-crossing windows. By default, active jobs finish after a window closes. `--abort-outside-window` interrupts active conversion at the schedule boundary, requeues the job with `recovered_from: schedule_window_closed`, and releases the processing lock. Documentation added in `docs/worker_schedule.md`, README, and DESIGN.
- **2026-06-25:** Requeued all current retry-candidate problem jobs. `blobforge janitor --verbose` restored stale processing lock `0237641f74fd...` at retry `3/3` and retried failed timeout job `792ac29bd6b6...` at retry `2/3`. `blobforge retry-all --dead --reset-retries --priority 3_normal` requeued the four dead-letter jobs (`0857d1183713...`, `3c7ccc748fb4...`, `a96530cb7011...`, `f829c114cc29...`). Verification showed failed `0`, dead-letter `0`, processing `0`, `3_normal` `9`, and `4_low` `431`.
- **2026-06-25:** Follow-up failed-job investigation after reruns found 1 failed job, 4 dead-letter jobs, and 1 stale processing lock. `atlantis` and `citadel` were confirmed retired and their worker registry entries should be ignored as stale. Failed job `792ac29bd6b6...` (`Changeling The Lost - Core Book.pdf`) exceeded the 86400s conversion timeout at retry 1. Stale processing lock `0237641f74fd...` (`Cthulhu_7_Grundregelwerk.pdf`) had a ~4h50m-old heartbeat and would be restored by janitor at retry 3/3. Dead-letter jobs are `0857d1183713...` (`7910 - Rigger 3.pdf`), `3c7ccc748fb4...` (`Trinity Continuum Aberrant (Rasterized).pdf`), `a96530cb7011...` (`Cthulhu-Edition-7-Grundregelwerk-2017.pdf`), and `f829c114cc29...` (`Geist - The Sin-Eaters.pdf`); no structured error logs were available for those markers.
- **2026-06-09:** Removed the two remaining PDFium data-format failure jobs from BlobForge: `3f094b24b162...` (`4th Edition/Shadowrun 4E - Mil Spec Tech.pdf`) and `5be2a0426593...` (`Scion 1st/Scion - Seeds of Tomorrow.pdf`). `blobforge remove` deleted each dead-letter marker, raw PDF object, manifest entry, and error log; verification showed dead-letter count `0` and both hashes absent from the manifest.
- **2026-06-09:** Selectively requeued 38 dead-letter jobs whose errors were not `PDFium: Data format error`. Requeued jobs were moved to `queue/todo/3_normal/` with retry counters reset to `0` and `recovered_from: manual_bulk_retry_dead_excluding_pdfium`; the two PDFium data-format failures (`3f094b24b162...`, `5be2a0426593...`) remain in dead-letter.
- **2026-06-09:** Investigated the current failed/dead-letter backlog. The failed queue was empty, while dead-letter had 40 jobs at retry count 4 (`max_retries: 3`). Error grouping: 34 `Worker restarted while job was processing`, 4 process-pool abrupt terminations, and 2 PDFium data-format errors. A local Marker probe of the smallest restart-failed PDF (`1f71f4699dbe...`, 19.8 MiB) loaded successfully and began layout recognition, indicating at least some dead-letter jobs are retry candidates, but local conversion caused high memory pressure. Avoid bulk retry; retry one job at a time under managed worker control.
- **2026-03-27:** Added `blobforge repair-metadata` to restore missing BlobForge raw-object metadata (`original-name`, `tags`, `size`) from manifest entries after S3 migrations. The repair uses same-key server-side copy with `MetadataDirective=REPLACE`, merges in unrelated existing metadata (for example `src_last_modified_millis`), and reconstructs `original-name` from the basename of the first manifest path to match historical ingest behavior.
- **2026-03-27:** Investigated `blobforge dashboard` showing `unknown.pdf` for live processing jobs. The dashboard prints the worker heartbeat's `progress.original_filename`, and `worker.py` falls back to `s3_meta.get("original-name", "unknown.pdf")`. Live Backblaze raw objects currently retain only `src_last_modified_millis` metadata for sampled/in-flight PDFs, while the manifest still contains correct `paths`, so this symptom indicates missing raw-object filename metadata rather than lost PDF/manifest data. The user confirmed the cause: migrating raw objects to a new S3 provider with `rclone sync` did not preserve the BlobForge metadata.
- **2026-02-03:** Added `S3_PREFIX` support to `config.py` to allow namespacing in the S3 bucket. All queue and storage paths now respect this prefix.
- **2026-02-03:** Standardized `janitor.py` and `status.py` to use the central `config.py` for path resolution.
- **2026-02-03:** Major refactor - consolidated all S3 operations into single `s3_client.py` module. All components now use this unified client.
- **2026-02-03:** Added dead-letter queue (`queue/dead/`) for jobs exceeding MAX_RETRIES. Jobs can be manually retried via CLI.
- **2026-02-03:** Worker now uses heartbeat mechanism (60s interval) with 15-minute stale timeout instead of 2-hour fixed timeout.
- **2026-02-03:** Fixed race condition: todo markers are now kept until job completion (not deleted on lock acquisition).
- **2026-02-03:** Improved sharding from 16 to 256 shards (2-char hex prefix) for better worker distribution.
- **2026-02-03:** Worker ID is now persistent based on machine fingerprint (hostname + /etc/machine-id) instead of random per session.
- **2026-02-03:** Restructured as Python package with `pyproject.toml` entry point. Install via `uv tool install .`
- **2026-02-03:** All env vars now use `BLOBFORGE_` prefix. S3 credentials use `BLOBFORGE_S3_ACCESS_KEY_ID`, etc.
- **2026-02-03:** Operational config (max_retries, timeouts) now stored in S3 at `{prefix}registry/config.json` with 1-hour TTL cache.
- **2026-02-03:** Worker registration: workers push metadata to `{prefix}registry/workers/{id}.json` on startup/shutdown.
- **2026-02-03:** New CLI commands: `blobforge config` (view/update remote config), `blobforge workers` (list registered workers).
- **2026-02-04:** Optimized worker job polling: replaced random shard scanning (5 priorities × 256 shards = 1280 potential requests) with broad priority scans (max 5 LIST requests). Added adaptive exponential backoff with jitter when queue is empty. Added priority cache to skip empty queues for 30s.
- **2026-02-04:** Added `blobforge test-s3` CLI command to test S3 endpoint capabilities (conditional writes, metadata, etc.).
- **2026-02-04:** Implemented timestamp-based soft locking for S3 providers without conditional write support (e.g., Hetzner Object Storage). Set `s3_supports_conditional_writes: false` in remote config to use this mode.
- **2026-02-04:** Enhanced heartbeat metadata: now tracks CPU/RAM usage (via psutil), elapsed time, file size, original filename. Install psutil for full metrics: `pip install psutil` or `pip install blobforge[metrics]`.
- **2026-02-04:** Added job throughput metrics: workers track jobs_completed, jobs_failed, bytes_processed, avg_processing_time, jobs_per_hour. Metrics stored in worker registry and displayed in `blobforge workers` command.
- **2026-02-04:** Improved status dashboard: shows detailed processing job info including filename, elapsed time, stage, CPU/RAM usage. Progress bar for overall completion.
- **2026-02-04:** Added job logging: errors now saved to `{prefix}registry/logs/{hash}/error.json` with full traceback and context. View with `blobforge logs <hash>`.
- **2026-02-04:** New CLI commands: `blobforge logs` (view job logs), `blobforge watch` (auto-refresh dashboard), `blobforge download` (get results), `blobforge preview` (peek at output), `blobforge retry-all` (bulk retry), `blobforge clear-dead` (purge dead-letter), `blobforge search-queue` (find by filename), `blobforge cancel` (cancel running job).
- **2026-02-05:** Added Telegram bot integration (`blobforge telegram`). Features: interactive dashboard, PDF upload for ingestion, queue stats, job status lookup, workers/config views, janitor trigger, retry/cancel/download actions. Uses inline keyboards for navigation. Requires `BLOBFORGE_TELEGRAM_TOKEN` and `BLOBFORGE_TELEGRAM_ALLOWED_USERS` (comma-separated user IDs). Install with `uv pip install -e '.[telegram]'`.
- **2026-02-05:** Fixed S3 metadata Unicode handling. S3 only supports ASCII in metadata values, but filenames may contain Unicode characters (e.g., curly apostrophes like ' U+2019). Added `blobforge/utils.py` with `sanitize_metadata_value()` / `decode_metadata_value()` functions that URL-encode non-ASCII characters. The S3 client now automatically encodes metadata on upload and decodes on retrieval. Also added `original_name` parameter to `ingestor.ingest()` for telegram bot compatibility.
- **2026-02-05:** Implemented file hash caching via extended attributes (xattrs) as specified in `docs/file_hashing_via_xattrs.md`. The ingestor now caches SHA256 hashes in `user.checksum.sha256` and validates using `user.checksum.mtime`. This significantly speeds up re-ingestion of unchanged files. The xattr package is optional - install with `uv pip install -e '.[xattr]'` or `uv pip install -e '.[all]'`. Works on Linux/macOS with ext4, btrfs, xfs, zfs filesystems.
- **2026-02-05:** Added `blobforge remove` CLI command to completely remove jobs from the system. Removes from all queues (todo/failed/dead), raw store, manifest, and logs. Throws error if job is currently processing or already completed (use `--force` to remove completed jobs). Supports `--dry-run` to preview changes.
- **2026-02-08:** Worker startup recovery now treats recovered processing locks as failed attempts. On startup, recovered jobs increment retry count from the processing lock; jobs beyond retry budget move directly to dead-letter queue, and only within-budget jobs are requeued to todo. This prevents crash/restart loops from repeatedly running the same job at `retry=0`.
- **2026-02-08:** Worker runtime now handles catchable shutdown signals (`SIGINT`, `SIGTERM`, and platform-available `SIGHUP`/`SIGQUIT`) and performs graceful shutdown by requeueing the active job and releasing its processing lock before deregistration. This avoids waiting for stale-lock janitor recovery during normal restarts.
- **2026-02-10:** Hardened graceful shutdown ordering in `worker.py`: signal handlers remain active until cleanup finishes, active job requeue happens before heartbeat join wait, and unexpected loop exceptions now still trigger graceful shutdown with requeue intent.
- **2026-02-10:** Startup recovery retry reconciliation now uses `max(lock_retries, todo_retries)` before incrementing to avoid undercounting retries when lock and todo metadata diverge.
- **2026-02-10:** Conversion timeout is now enforced in the worker conversion path via `SIGALRM`/`ITIMER_REAL` when available. Added fallback behavior/logging for platforms/contexts where signal timers are unavailable.
- **2026-02-10:** Updated `README.md` to document graceful worker shutdown semantics (signal handling, immediate active-job requeue, janitor fallback for ungraceful termination) and conversion-timeout behavior/caveats so user-facing docs match current runtime implementation.
- **2026-02-26:** Added `blobforge hydrate` command to walk local PDFs, hash with xattr-aware caching, and materialize completed conversion outputs as `<stem>.md` plus `<stem>.assets/`. Hydration deduplicates zip downloads by hash in a single run, rewrites markdown image links from `assets/` to `<stem>.assets/`, and supports `--dry-run` / `--force`.
- **2026-02-26:** Optimized `blobforge hydrate` remote checks with a two-phase preflight: local hash indexing + single manifest fetch for hash prefiltering, then done-availability checks per unique candidate hash. Added bulk done-hash listing support in `S3Client.list_done_hashes()` for large hydration runs.
- **2026-04-28:** Investigated and fixed `blobforge dashboard` slowness. Root causes were (1) sequential S3 `count_prefix` calls for 5 todo priorities + done/failed/dead queues, (2) `scan_processing_detailed` doing sequential `get_object_json` for every active job (N+1 query problem), and (3) `list_workers` sequentially fetching each worker JSON. Fixed by parallelizing all independent I/O with `concurrent.futures.ThreadPoolExecutor`: `status.py` now fetches all counts and processing details concurrently; `s3_client.py` `scan_processing_detailed` fetches lock contents in parallel (max 16 workers); `list_workers` fetches worker metadata in parallel (max 8 workers). Also added optional `limit` parameter to `count_prefix` for future capping of huge prefixes.
