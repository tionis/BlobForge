# Work Log

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
    