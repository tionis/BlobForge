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

- **2026-07-15:** Implementation-complexity comparison: Bunny is marginally simpler for a proof of concept because one Edge Script can access libSQL directly. Cloudflare adds a public Worker router, a Durable Object class/binding, and migration configuration. For a production-quality implementation, Cloudflare is simpler overall because one Durable Object serializes coordination and supplies strong transactions, persisted alarms, and PITR, avoiding Bunny replica-consistency and failover-reconciliation logic. Both designs still require fencing leases because external workers can crash after claiming jobs.
- **2026-07-15:** Objective platform comparison now favors Cloudflare Workers with a single SQLite-backed Durable Object—not Cloudflare Queues—for BlobForge's production coordination backend. One Durable Object provides a non-expiring SQLite job table, serialized coordination, strongly consistent transactional storage, alarms, 30-day point-in-time recovery, GA status, and ample Free-plan capacity for hundreds of jobs. Bunny remains attractive for experimentation and a lower paid floor, but its Database is Public Preview and documents possible loss of recently committed writes during failover, making it the weaker production choice today.
- **2026-07-15:** Managed broker queues are unsuitable as BlobForge's authoritative backlog. The queue is expected to contain hundreds of jobs while workers finish only a few per day, so jobs may legitimately wait for months. Cloudflare Queue retention is 24 hours on Free and at most 14 days on Paid. The target must use a persistent database `jobs` table whose pending rows do not expire, with atomic priority claims, fencing-token leases, heartbeat renewal, inline expired-lease recovery, and policy-controlled terminal-state retention.
- **2026-07-15:** Pricing comparison: Bunny Database is free during Public Preview; published future rates are $0.30/billion rows read, $0.30/million rows written, and $0.10/GB per active region-month. Bunny Edge Scripting is $0.20/million requests plus $0.02/1000 CPU-seconds, with Bunny advertising a general $1 monthly minimum. Cloudflare can also be free at small scale, but free Queue retention is fixed at 24 hours. Workers Paid has a $5 monthly floor, includes 10 million Worker requests, one million Queue operations, and large D1 row allowances; extra Queue operations are $0.40/million. For BlobForge's sporadic workload, price is unlikely to drive the decision, though Bunny has the lower paid floor and indefinite database-row retention.
- **2026-07-15:** Compared Bunny-only coordination with Cloudflare's current coordination primitives. Cloudflare offers managed at-least-once Queues with external HTTP pull consumers, visibility leases, retry/delay/DLQ behavior, Cron Triggers, Durable Object alarms/strong per-object consistency, and durable Workflows. Bunny can reproduce BlobForge's functional queue state with database leases, but those semantics and observability remain application-owned. More importantly, Bunny Database currently documents that committed writes not yet uploaded from the primary WAL may be lost on failover, with a maximum exposure of 10 seconds or 4096 frames, and replica reads do not guarantee read-your-writes. This risk must be explicitly tested and reconciled if Bunny remains the only coordination backend.
- **2026-07-15:** User clarified the backend scope: Bunny Edge Scripting and Bunny Database should be the only coordination services. Python conversion workers remain external and the existing object store remains the data plane. Cloudflare, hosted PostgreSQL, Bunny Magic Containers, and S3 queue markers are out of scope for the target coordination architecture. Long-running behavior is represented by durable database job leases and external worker heartbeats, not by a long-lived Edge Script invocation.
- **2026-07-15:** Evaluated a Bunny-based backend for BlobForge. Bunny Edge Scripting is a good fit for an authenticated API/control plane but not long-running PDF conversion (currently 30 seconds CPU and 128 MB per request). Recommended keeping conversion in Python workers, moving authoritative metadata/job state to Bunny Database behind lease-based atomic transitions, and retaining object storage for PDFs/results. Bunny Database is currently Public Preview with a documented 1 GB default limit and serialized primary writes, so those constraints require validation during the proof of concept. The existing uncommitted PostgreSQL work is only partially integrated: worker/dashboard paths use it while ingestion, janitor, and multiple operator paths remain S3-authoritative.
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
