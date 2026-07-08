# Work Log

## 2026-07-09 (Worker Conversion Isolation)
- **Objective:** Fix scheduled-abort worker mode after marker crashed natively with `corrupted double-linked list` and left an active processing lock behind.
- **Actions:**
    1. Investigated live queue state:
       - Job `f829c114cc2982b16472c22110b21f662cf32001d3a76f77582eb212c6fa7b98` remained in `PROCESSING` under worker `f8a500d06011`.
       - The active PDF was `Geist - The Sin-Eaters.pdf`.
    2. Updated worker runtime:
       - Added isolated marker conversion supervision via a child Python process.
       - Made `--abort-outside-window` automatically enable isolated conversion.
       - Added `--isolate-conversion` for crash containment without a run schedule.
       - Parent worker now keeps S3 lock ownership, enforces child timeout, and requeues on schedule boundary.
    3. Added `blobforge/conversion_child.py`:
       - Runs marker conversion in the child process.
       - Writes `content.md`, assets, marker metadata, and a small handoff result.
    4. Updated tests:
       - Covered successful isolated conversion handoff.
       - Covered schedule-boundary timeout killing the child and raising the requeue path.
    5. Updated documentation and tracking:
       - Updated `docs/worker_schedule.md`, `README.md`, `DESIGN.md`, `TODO.md`, and `AGENTS.md`.
- **Validation:**
    - `UV_CACHE_DIR=/tmp/uv-cache uv run --no-project --with pytest python -m pytest tests/test_worker_runtime.py -q` -> `14 passed`.
    - `UV_CACHE_DIR=/tmp/uv-cache uv run --no-project --with pytest --with xattr python -m pytest tests -q` -> `86 passed, 5 subtests passed`.
    - `UV_CACHE_DIR=/tmp/uv-cache uv run --no-project --with ruff ruff check blobforge/worker.py blobforge/conversion_child.py tests/test_worker_runtime.py` -> passed.
    - Targeted Ruff including `blobforge/cli.py` is still blocked by pre-existing lint issues in `cli.py`.
- **Recovery:**
    - Network-approved `blobforge janitor --verbose` recovered stale processing lock `f829c114cc29...` at retry `1/3`.
    - Final queue check: `PROCESSING: 0`, `FAILED: 0`, `DEAD-LETTER: 0`, `TODO: 439`, `DONE: 1371`.
- **Status:** Implementation validated and stale failed-run lock recovered.

## 2026-03-27 (Raw Metadata Repair Command)
- **Objective:** Add an operator command to restore stripped raw-object metadata after S3 provider migration.
- **Actions:**
    1. Added metadata rewrite support to `blobforge/s3_client.py`:
       - Implemented same-key server-side copy with `MetadataDirective='REPLACE'`.
       - Preserved unrelated existing metadata keys such as `src_last_modified_millis`.
       - Preserved common object headers such as `ContentType` during metadata rewrite.
    2. Added CLI command `blobforge repair-metadata` in `blobforge/cli.py`:
       - Uses manifest entries as the source of truth.
       - Restores `original-name`, `tags`, and `size` onto `store/raw/<hash>.pdf`.
       - Defaults to filling only missing keys.
       - Supports `--force` to overwrite mismatched BlobForge metadata.
       - Supports `--dry-run` and optional per-hash targeting.
    3. Added test coverage in `tests/test_blobforge.py`:
       - Verifies metadata merge/preservation in the S3 client.
       - Verifies CLI repair behavior for missing metadata, forced overwrite, and dry-run.
    4. Updated documentation:
       - Added `docs/raw_metadata_repair.md`.
       - Updated `README.md` and `DESIGN.md` command examples.
- **Status:** Repair command implemented and documented.

## 2026-03-27 (Dashboard Filename Investigation)
- **Objective:** Determine why `blobforge dashboard` shows `unknown.pdf` for active jobs and verify whether data was lost.
- **Actions:**
    1. Inspected dashboard and worker code paths:
       - Confirmed `blobforge/status.py` renders `progress.original_filename`.
       - Confirmed `blobforge/worker.py` sets that field from raw-object metadata via `s3_meta.get("original-name", "unknown.pdf")`.
    2. Queried live S3 state for the two active jobs shown in the dashboard:
       - Verified both raw PDFs still exist in `store/raw/`.
       - Verified neither job is in `done`, `failed`, or `dead`.
       - Verified manifest entries still contain the original paths/filenames.
       - Verified raw object metadata for the live jobs contains only `src_last_modified_millis`, not `original-name`, `tags`, or `size`.
    3. Sampled additional raw objects and observed the same metadata shape (`src_last_modified_millis` only) in the sample.
    4. Ran `blobforge test-s3` against the configured Backblaze endpoint:
       - Confirmed custom metadata support works for new test writes.
- **Conclusion:** `unknown.pdf` in the dashboard is a display fallback caused by missing raw-object filename metadata, not by loss of the underlying PDF or manifest entry. The user later confirmed the operational cause: `rclone sync` was used during S3-provider migration and did not preserve BlobForge's custom metadata on the raw objects.

## 2026-02-03
- **Objective:** Add S3 namespacing support and establish agent protocols.
- **Actions:**
    - Modified `config.py` to include `S3_PREFIX`.
    - Updated `janitor.py`, `status.py`, and `ingestor.py` to use standardized config paths.
    - Verified prefixing logic with a dry-run test.
    - Created `AGENTS.md`, `TODO.md`, and `docs/WORK_LOG.md` to define repository standards for AI agents.
- **Status:** Namespacing complete. Agent environment initialized.

## 2026-02-03 (Design Review & Refactor)
- **Objective:** Address design document issues and implement architectural improvements.
- **Issues Identified:**
    1. Race condition in job acquisition (TOCTOU vulnerability)
    2. Duplicate S3Client implementations across 4 files
    3. Weak sharding strategy (16 shards with single char)
    4. No heartbeat mechanism (2-hour stale timeout too long)
    5. No retry limit or dead-letter queue
    6. Copy-paste errors in DESIGN.md
    7. Duplicate `Body=` parameter bug in worker.py
    8. Ingestor didn't check all queue states
    9. No CLI command to retry failed jobs
    10. Random worker IDs broke cross-restart cleanup
- **Actions:**
    - Created consolidated `s3_client.py` with unified S3Client class
    - Updated `config.py` with new settings: `S3_PREFIX_DEAD`, `MAX_RETRIES`, `HEARTBEAT_INTERVAL_SECONDS`, `STALE_TIMEOUT_MINUTES`, persistent worker ID generation
    - Rewrote `worker.py`:
        - Fixed race condition: todo marker kept until completion
        - Improved sharding: 2-char prefix (256 shards)
        - Added heartbeat thread (60s interval)
        - Added retry tracking and dead-letter queue support
    - Rewrote `ingestor.py`:
        - Now checks all queue states (done, processing, failed, dead, todo) before queueing
        - Added summary statistics
    - Rewrote `janitor.py`:
        - Uses heartbeat-based stale detection (15 min default)
        - Processes both stale processing jobs and failed queue
        - Respects MAX_RETRIES, moves to dead-letter when exceeded
    - Rewrote `status.py` to use consolidated S3Client
    - Rewrote `cli.py`:
        - Added `retry` command for failed/dead jobs
        - Added `dashboard` command
        - All commands use consolidated S3Client
    - Completely rewrote `DESIGN.md`:
        - Fixed copy-paste errors
        - Documented all new features
        - Added S3 provider compatibility section
        - Added state transition diagram
        - Added configuration reference
- **Status:** All changes complete. Code compiles successfully.

## 2026-02-03 (Quality Improvements)
- **Objective:** Address remaining issues from code review.
- **Actions:**
    - Added comprehensive unit test suite (`tests/test_blobforge.py`)
        - 35 tests covering S3 operations, locking, heartbeat, retry logic
        - Tests skip gracefully when botocore not available
    - Added logging framework:
        - Replaced `print()` with `logging` module in worker.py
        - Added `LOG_LEVEL` environment variable
        - Structured log format with timestamps
    - Made conversion timeout configurable:
        - Added `CONVERSION_TIMEOUT_SECONDS` to config.py
        - Updated worker to use configurable timeout
    - Fixed graceful shutdown:
        - `worker.shutdown()` now waits for heartbeat thread to finish
    - Updated README.md with complete documentation:
        - All CLI commands with examples
        - Full configuration reference
        - S3 provider compatibility matrix
        - State transition diagram
    - Fixed `datetime.utcnow()` deprecation warnings
- **Status:** All improvements complete. 35 tests pass.

## 2026-02-04 (Quality of Life Improvements)
- **Objective:** Implement comprehensive QoL improvements for monitoring and management.
- **Actions:**
    1. **Enhanced Heartbeat Metadata:**
       - Added CPU/RAM/disk usage tracking via psutil (optional dependency)
       - Track elapsed time, file size, original filename per job
       - System metrics included in heartbeat updates
    
    2. **Richer Status Dashboard:**
       - Redesigned `blobforge dashboard` with visual progress bars
       - Shows filename, elapsed time, stage, CPU/RAM for processing jobs
       - Added worker summary section with aggregate metrics
       - Visual indicators (emojis) for job status
    
    3. **Job Throughput Metrics:**
       - Workers now track: jobs_completed, jobs_failed, bytes_processed
       - Calculate avg_processing_time and jobs_per_hour
       - Metrics persisted in worker registry JSON
       - `blobforge workers` command shows aggregate metrics
    
    4. **Job Logs / Error Details:**
       - Errors saved to `registry/logs/{hash}/error.json`
       - Includes full traceback and context (stage, filename, etc.)
       - New `blobforge logs <hash>` command to view logs
    
    5. **New CLI Commands:**
       - `blobforge logs <hash>` - View error details and logs
       - `blobforge watch` - Auto-refreshing dashboard
       - `blobforge download <hash>` - Download completed results
       - `blobforge preview <hash>` - Preview markdown output
       - `blobforge retry-all` - Bulk retry failed/dead jobs
       - `blobforge clear-dead` - Purge dead-letter queue
       - `blobforge search-queue <query>` - Find jobs by filename
       - `blobforge cancel <hash>` - Cancel running job
    
    6. **Worker Management:**
       - Enhanced `blobforge workers` output with metrics display
       - Shows CPU/RAM, jobs completed, throughput rate
       - Aggregate statistics across all workers
    
    7. **Dependencies:**
       - Added psutil as optional dependency: `pip install blobforge[metrics]`
       - Added `[all]` extra for full install
    
- **Status:** All QoL improvements complete. CLI tested and working.
## 2026-02-05 (Progress Tracking & ETA Display)
- **Objective:** Add rich progress tracking for marker PDF conversion stages and ETA display.
- **Actions:**
    1. **tqdm Progress Interception:**
       - Implemented monkey-patch for `tqdm.tqdm` class to intercept progress bars
       - Captures stage name, current/total items, rate, elapsed time, and ETA
       - Rate-limited callbacks (2s interval) to avoid overhead
       - Progress stored in HeartbeatThread memory, included in scheduled heartbeats
       - Heartbeat frequency unchanged (follows `get_heartbeat_interval()` config)
    
    2. **Dashboard ETA Column:**
       - Added ETA column to processing jobs display
       - Shows estimated time remaining for current stage (e.g., "~45s", "~2m30s")
       - Stage column shows marker progress: "Recognizing Text: 5/12"
    
    3. **Containerfile Update:**
       - Changed to install `.[all]` to include psutil for system metrics
    
- **Status:** Progress tracking and ETA display complete. Tests passing.
    

    
## 2026-02-05 (File Hash Caching via Xattrs)
    
- **Objective:** Implement persistent file hash caching using filesystem extended attributes.
    
- **Actions:**
    
    1. **Specification Alignment:**
    
       - Reviewed `docs/file_hashing_via_xattrs.md` and Go reference implementation.
    
       - Implemented logic in `blobforge/utils.py` following the standard.
    
    2. **Xattr Cache Implementation:**
    
       - Added `get_cached_hash` and `set_cached_hash` with mtime validation.
    
       - Updated `compute_sha256_with_cache` to use atomicity checks (pre/post stat comparison).
    
       - Stored mtime as integer seconds string for cross-language compatibility.
    
    3. **Ingestor Optimization:**
    
       - Updated `blobforge/ingestor.py` to check xattr cache before computing hashes.
    
       - Optimized UI logging: "Computing hash..." is now only shown on cache miss.
    
       - Verified that redundant cache checks are safe and maintain correct logging.
    
    4. **Verification:**
    
       - Created `tests/test_xattr_hashing.py` to verify caching logic, mtime validation, and modification detection.
    
       - Created `tests/test_ingest_logging.py` to verify UI logging behavior.
    
       - Both tests passing with `uv run`.
    
- **Status:** Xattr caching implemented and verified. Ingestor performance significantly improved for large directories.

## 2026-02-08 (Worker Startup Recovery Retry Semantics)
- **Objective:** Prevent infinite crash/restart loops from reprocessing the same job with `retry=0`.
- **Actions:**
    1. Updated `blobforge/worker.py` startup recovery logic (`cleanup_previous_session`):
       - Recovered processing locks now increment retry count from lock metadata.
       - Recovered jobs are requeued with structured todo marker metadata (`retries`, `queued_at`, `recovered_from`).
       - Jobs exceeding retry budget during recovery are moved directly to dead-letter queue.
       - Added explicit cleanup of todo markers for dead-lettered recovered jobs.
    2. Added unit tests in `tests/test_worker_recovery.py`:
       - Verifies recovered jobs are requeued with incremented retries.
       - Verifies over-budget recovered jobs are dead-lettered and not requeued.
       - Verifies locks owned by other workers are ignored.
    3. Updated architecture documentation:
       - `DESIGN.md` section `4.2.1`
       - `docs/worker_startup_recovery.md` design note for startup recovery retry semantics
    4. Updated `TODO.md` completed items and added a repository finding in `AGENTS.md`.
- **Status:** Recovery logic now treats startup-recovered locks as failed attempts, closing the observed retry reset loop.

## 2026-02-08 (Signal-Aware Graceful Worker Shutdown)
- **Objective:** Ensure worker shutdown catches normal termination signals and requeues in-flight jobs immediately.
- **Actions:**
    1. Updated `blobforge/worker.py`:
       - Added catchable signal registration/restoration helpers for worker loops.
       - Added `run_worker_loop(...)` shared runtime with signal-aware graceful exit.
       - Added `_requeue_active_job(...)` used by shutdown to move active job back to todo and release lock.
       - Extended `shutdown(...)` with `requeue_current_job` behavior.
    2. Updated `blobforge/cli.py`:
       - `cmd_worker` now delegates to `worker.run_worker_loop(...)` so CLI worker path uses the same graceful signal handling.
    3. Added tests in `tests/test_worker_shutdown.py`:
       - Verifies shutdown requeues active job with preserved retries and recovery metadata.
       - Verifies no-op requeue when no current job is active.
       - Verifies loop interruption triggers shutdown with requeue intent.
    4. Added/updated documentation:
       - `docs/worker_graceful_shutdown.md`
       - `DESIGN.md` worker section (`4.2.7`) for signal-aware graceful shutdown semantics.
       - Updated `TODO.md` and `AGENTS.md` findings.
- **Status:** Worker shutdown now handles catchable signals and requeues active jobs without waiting for stale-lock recovery.

## 2026-02-10 (Worker Runtime Hardening Follow-Up)
- **Objective:** Address remaining worker robustness gaps after initial shutdown/recovery changes.
- **Actions:**
    1. `blobforge/worker.py` runtime/shutdown hardening:
       - Added `_safe_int()` utility for resilient numeric parsing from lock/marker metadata.
       - Updated startup recovery to reconcile retries using `max(lock_retries, todo_retries)` before incrementing.
       - Updated heartbeat thread stop behavior to use an event (`wait`) for prompt shutdown wake-up.
       - Reordered shutdown flow: stop heartbeat -> requeue active job -> join heartbeat -> deregister.
       - Kept custom signal handlers active until shutdown cleanup completes.
       - Added explicit handling for unexpected run loop exceptions (`exit_code=1`) while still forcing graceful cleanup/requeue.
    2. Conversion timeout enforcement:
       - Added `_run_conversion_with_timeout()` wrapper.
       - Wired `process()` conversion path to enforce `conversion_timeout` using `SIGALRM`/`ITIMER_REAL` when available.
       - Added fallback warnings for unsupported timer/signal environments.
    3. Test coverage updates:
       - Extended `tests/test_worker_shutdown.py` with shutdown ordering and handler-restore ordering checks.
       - Extended `tests/test_worker_recovery.py` with lock/todo retry reconciliation coverage.
       - Added `tests/test_worker_runtime.py`:
         - Timeout wrapper behavior tests.
         - Subprocess integration test sending real `SIGTERM` to verify graceful shutdown path invocation.
    4. Documentation updates:
       - Updated `DESIGN.md` worker sections (`4.2.1`, `4.2.4`, `4.2.7`).
       - Updated `docs/worker_startup_recovery.md` and `docs/worker_graceful_shutdown.md`.
       - Added `docs/worker_conversion_timeout.md`.
       - Updated `TODO.md` and `AGENTS.md` findings.
    5. Validation:
       - `uv run python -m pytest tests/test_worker_shutdown.py tests/test_worker_recovery.py tests/test_worker_runtime.py -q` -> `15 passed`.
       - `uv run python -m pytest tests/test_blobforge.py -q` -> `49 passed, 5 subtests passed`.
- **Status:** All identified follow-up issues from worker robustness review have been addressed and validated.

## 2026-07-08 (Worker Run Windows)
- **Objective:** Add a worker runtime option that limits CPU-intensive conversion work to configured local-time windows and can optionally abort/requeue active jobs when a window closes.
- **Actions:**
    1. Inspected worker polling, graceful shutdown, active-job requeue, and conversion-timeout paths.
    2. Implemented local run-window parsing and gating in `blobforge/worker.py`.
       - Supports repeated or comma-separated `HH:MM-HH:MM` windows.
       - Supports windows crossing midnight.
       - Prevents job acquisition outside configured windows.
    3. Added optional active conversion abort/requeue.
       - `--abort-outside-window` uses the existing signal timer path.
       - Window-boundary aborts raise a schedule-specific exception and requeue the active job with `recovered_from: schedule_window_closed`.
       - Default behavior still lets active jobs finish after a window closes.
    4. Wired CLI flags in both `blobforge worker` and standalone `blobforge.worker` parser.
    5. Added focused unit coverage for schedule parsing, run-loop acquisition gating, process schedule propagation, and schedule-derived conversion timeout.
    6. Added documentation:
       - `docs/worker_schedule.md`
       - README worker examples and behavior notes.
       - DESIGN worker lifecycle notes.
    7. Validation:
       - `uv run --no-project --with pytest python -m pytest tests/test_worker_runtime.py tests/test_worker_shutdown.py tests/test_worker_recovery.py -q` -> `23 passed`.
       - `uv run --no-project --with pytest --with xattr python -m pytest tests -q` -> `84 passed, 5 subtests passed`.
       - `uv run --no-project --with ruff ruff check blobforge/worker.py tests/test_worker_runtime.py` -> passed.
       - `uv run --no-project --with ruff ruff check .` -> failed on pre-existing unrelated lint issues across the repo; touched worker files pass targeted Ruff.
       - `uv run blobforge worker --help` -> confirmed `--run-window` and `--abort-outside-window` are registered.
    8. Refined outside-window idle behavior:
       - Worker now sleeps until the next configured opening window instead of waking on `idle_sleep`/short polling intervals.
       - Existing signal handling still interrupts the sleep for graceful shutdown.
       - Added regression coverage for a 10-hour outside-window sleep despite `idle_sleep=10`.
- **Status:** Worker run-window scheduling implemented, documented, and validated.

## 2026-06-25 (Failed Jobs Follow-Up Investigation)
- **Objective:** Inspect jobs that failed after the previous selective dead-letter retry, identify current failure causes, and recommend next handling.
- **Actions:**
    1. Started follow-up investigation and reviewed prior failed/dead-letter workflow notes.
    2. Queried live queue, worker registry, remote config, and object-level queue records.
       - Todo: 436 total (`3_normal`: 5, `4_low`: 431).
       - Processing locks: 1 actual lock (`0237641f74fd...`, worker `f8a500d06011`).
       - Failed queue: 1 job.
       - Dead-letter queue: 4 jobs.
       - Remote config: `max_retries: 3`, `conversion_timeout: 86400`.
    3. Noted that `atlantis` and `citadel` are retired hosts per user confirmation; their worker registry records are stale and should not be treated as live workers.
    4. Triaged current problem records:
       - Failed: `792ac29bd6b6...` (`Changeling The Lost - Core Book.pdf`, 126.7 MiB), `Conversion exceeded timeout (86400s)`, retries `1`.
       - Dead: `0857d1183713...` (`7910 - Rigger 3.pdf`, 104.4 MiB), `Worker restarted while job was processing`, retries `4`.
       - Dead: `3c7ccc748fb4...` (`Trinity Continuum Aberrant (Rasterized).pdf`, 92.9 MiB), `Exceeded max retries (4)`, retries `4`.
       - Dead: `a96530cb7011...` (`Cthulhu-Edition-7-Grundregelwerk-2017.pdf`, 48.0 MiB), `Worker restarted while job was processing`, retries `4`.
       - Dead: `f829c114cc29...` (`Geist - The Sin-Eaters.pdf`, 57.4 MiB), `Worker restarted while job was processing`, retries `4`.
       - Processing/stale: `0237641f74fd...` (`Cthulhu_7_Grundregelwerk.pdf`, 46.0 MiB), last heartbeat about 4h50m old, retries `2`.
    5. Ran `blobforge janitor --dry-run --verbose`.
       - Would restore stale `0237641f74fd...` to `3_normal` with retry `3/3`.
       - Would retry failed `792ac29bd6b6...` to `3_normal` with retry `2/3`.
       - Would not move any additional jobs to dead-letter.
    6. Checked available job logs.
       - Dead-letter jobs and stale processing lock had no structured error logs available.
       - Timeout job had structured error detail.
- **Status:** Investigation complete. Current failed/stale jobs are mostly resource/runtime failures, not PDFium data-format corruption. Suggested next operational step is to run janitor for the stale/failed queue, then decide separately whether to manually reset/requeue the four dead-letter jobs.

## 2026-06-25 (Failed/Dead/Stale Requeue)
- **Objective:** Requeue all current failed, dead-letter, and stale processing jobs after determining they were retry candidates.
- **Actions:**
    1. Ran `blobforge janitor --verbose`.
       - Restored stale processing lock `0237641f74fd...` to `3_normal` at retry `3/3`.
       - Retried failed timeout job `792ac29bd6b6...` to `3_normal` at retry `2/3`.
       - Moved no jobs to dead-letter.
    2. Ran `blobforge retry-all --dead --reset-retries --priority 3_normal`.
       - Requeued `0857d1183713...`.
       - Requeued `3c7ccc748fb4...`.
       - Requeued `a96530cb7011...`.
       - Requeued `f829c114cc29...`.
    3. Verified post-requeue state:
       - Failed queue: `0`.
       - Dead-letter queue: `0`.
       - Processing locks: `0`.
       - `3_normal`: `9` jobs.
       - `4_low`: `431` jobs.
- **Status:** All current failed/dead/stale jobs were requeued successfully.

## 2026-02-10 (README Documentation Sync)
- **Objective:** Update user-facing docs to reflect finalized worker shutdown and timeout semantics.
- **Actions:**
    1. Updated `README.md`:
       - Added key-feature bullets for graceful shutdown and conversion-timeout behavior.
       - Expanded worker section with explicit signal-handling/shutdown behavior.
       - Clarified janitor role as recovery path for crash/ungraceful-stop scenarios.
       - Updated `conversion_timeout` config description with platform caveats.
       - Added conversion-timeout notes describing hard-timeout support and fallback behavior.
    2. Updated repository tracking files:
       - Added completed documentation item in `TODO.md`.
       - Added findings entry in `AGENTS.md`.
- **Status:** README now matches current worker runtime behavior and operational expectations.

## 2026-02-26 (Hydrate Converted Outputs Feature)
- **Objective:** Implement a local hydration workflow that materializes completed conversion outputs next to source PDFs.
- **Actions:**
    1. Implemented new hydration component in `blobforge/hydrator.py`:
       - Added recursive PDF discovery for files/directories.
       - Reused xattr-aware SHA256 path (`compute_sha256_with_cache`) for hash resolution.
       - Added done-zip existence checks at `{prefix}store/done/<hash>.zip`.
       - Added local materialization to `<stem>.md` and `<stem>.assets/`.
       - Added per-run archive download deduplication for duplicate hashes.
       - Added markdown asset path rewriting (`assets/` -> `<stem>.assets/`) to prevent folder collisions.
       - Added staging/atomic write behavior for markdown and staged asset directory replacement.
       - Added `--dry-run` and `--force` support via function parameters.
    2. Wired CLI in `blobforge/cli.py`:
       - Added `cmd_hydrate(...)`.
       - Added `hydrate` subcommand with positional `paths` and flags `--force`, `--dry-run`.
    3. Added automated tests in `tests/test_hydrator.py`:
       - Hydrates markdown/assets from a mocked conversion archive.
       - Skips when local markdown exists and `--force` is not set.
       - Verifies one archive download is reused for multiple PDFs with identical hash.
    4. Added documentation:
       - New design note: `docs/hydrate_command.md`.
       - Updated `README.md` usage section with `blobforge hydrate` examples.
    5. Updated repository tracking/protocol files:
       - `TODO.md` completed item added.
       - `AGENTS.md` findings updated.
- **Tooling / Verification Commands:**
    - `rg`, `sed` used to inspect command surfaces and existing hash/download logic.
    - `uv run python -m pytest tests/test_hydrator.py -q` -> `3 passed`.
    - `uv run python -m pytest tests/test_blobforge.py -q` -> `49 passed, 5 subtests passed`.
    - `uv run python -m pytest tests -q` -> `69 passed, 5 subtests passed`.
    - `uv run blobforge --help` -> confirmed `hydrate` command registered.
    - `uv run blobforge hydrate --help` -> confirmed `paths`, `--force`, `--dry-run`.
- **Status:** Feature implemented, documented, and validated.

## 2026-06-09 (Failed Queue Investigation)
- **Objective:** Inspect the current failed PDF queue, identify likely failure causes, and determine whether to retry jobs on this machine.
- **Actions:**
    1. Reviewed CLI/S3 queue support for failed jobs, retries, janitor recovery, and job logs.
    2. Confirmed `search-queue` does not currently include failed/dead queues, so failed-job triage will use direct failed-queue listing and `blobforge logs`.
    3. Queried live queue state with `uv run blobforge list --verbose`:
       - Todo: 411 jobs.
       - Processing: 2 jobs.
       - Failed queue: 0 jobs.
       - Dead-letter queue: 40 jobs.
    4. Queried remote config and worker state:
       - `max_retries: 3`; dead-letter entries are at retry count 4.
       - Active workers: `atlantis` and `citadel`; `atlantis` was already memory-saturated while processing a large rasterized PDF.
    5. Triaged dead-letter records plus structured error logs:
       - 34 jobs: `Worker restarted while job was processing`.
       - 4 jobs: `A process in the process pool was terminated abruptly while the future was running or pending.`
       - 2 jobs: `Failed to load document (PDFium: Data format error).`
    6. Installed optional local conversion dependencies with `uv pip install -e ".[convert,metrics]"`.
    7. Downloaded the smallest restart-failed PDF (`1f71f4699dbe...`, 19.8 MiB) to `/tmp` and ran a local offline conversion probe.
       - Marker model downloads completed successfully.
       - The PDF loaded and began layout recognition, reaching `8/184` batches before the probe was stopped.
       - Local memory pressure rose substantially during the probe, then returned to normal after termination.
- **Status:** The live failed queue is empty; the real backlog is 40 dead-letter jobs. Most look like worker restart/resource casualties rather than corrupt PDFs. Do not bulk retry. Prefer one managed retry at a time, starting with a small restart-failed job, using `blobforge retry <hash> --priority 1_critical --reset-retries` followed by `blobforge worker --run-once` on a machine with enough RAM.

## 2026-06-09 (Selective Dead-Letter Requeue)
- **Objective:** Requeue all dead-letter jobs except PDFs that failed with `PDFium: Data format error`.
- **Actions:**
    1. Ran a dry-run S3 scan over `queue/dead/` and structured error logs.
       - Selected for requeue: 38 jobs.
       - Skipped: 2 jobs with `Failed to load document (PDFium: Data format error).`
    2. Requeued the selected 38 jobs to `queue/todo/3_normal/` with retry counters reset to `0`.
       - Todo marker `recovered_from`: `manual_bulk_retry_dead_excluding_pdfium`.
       - Preserved each prior error in marker metadata as `previous_error`.
    3. Removed the corresponding 38 dead-letter markers.
    4. Verified queue state after the update:
       - `3_normal`: 39 jobs.
       - `dead-letter`: 2 jobs.
       - Remaining dead-letter hashes are `3f094b24b162...` and `5be2a0426593...`, both PDFium data-format failures.
- **Status:** Selective requeue completed successfully.

## 2026-06-09 (Broken PDF Removal)
- **Objective:** Remove the two PDFs that failed with `PDFium: Data format error`.
- **Actions:**
    1. Ran dry-run removals for both broken PDF hashes:
       - `3f094b24b162ccb468c5f941eeb439d8ba5c9c59834ad6cdc16e9b6614f4e4b4` (`4th Edition/Shadowrun 4E - Mil Spec Tech.pdf`)
       - `5be2a04265933289f7ec557f66732e37f9049811296db2deb86867e84279ed94` (`Scion 1st/Scion - Seeds of Tomorrow.pdf`)
    2. Applied `blobforge remove` for both jobs.
       - Removed dead-letter markers.
       - Removed raw PDF objects.
       - Removed manifest entries.
       - Removed one error log per job.
    3. Verified queue and manifest state:
       - Dead-letter queue: `0`.
       - Both hashes are no longer present in the manifest.
- **Status:** Broken PDF jobs removed successfully.

## 2026-02-26 (Hydrate Preflight Performance Optimization)
- **Objective:** Reduce per-file remote checks during hydration by precomputing local/remote hash sets.
- **Actions:**
    1. Updated `blobforge/hydrator.py`:
       - Added local preflight pass to compute hashes once for all hydration candidates.
       - Added manifest prefilter (`get_manifest`) to eliminate remote checks for hashes not present in manifest.
       - Added done-availability resolver keyed by unique hashes.
       - Added bulk done-index path for large runs (`DONE_INDEX_THRESHOLD`) with helper fallback paths.
    2. Updated `blobforge/s3_client.py`:
       - Added `list_done_hashes()` to paginate done objects and return parsed `<hash>.zip` identifiers.
    3. Updated tests:
       - `tests/test_hydrator.py`: added manifest-prefilter test asserting reduced `exists` calls.
       - `tests/test_blobforge.py`: added `list_done_hashes` parsing test.
    4. Updated docs/tracking:
       - Updated `docs/hydrate_command.md` with preflight and remote-check strategy.
       - Updated `TODO.md` and `AGENTS.md` findings.
- **Validation:**
    - `uv run python -m pytest tests/test_hydrator.py tests/test_blobforge.py -q` -> `54 passed, 5 subtests passed`.
    - `uv run python -m pytest tests -q` -> `71 passed, 5 subtests passed`.
- **Status:** Optimization implemented and validated.
    
