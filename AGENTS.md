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
