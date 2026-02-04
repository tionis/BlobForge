# Work Log

## 2026-02-04 (Shutdown Fix, LFS Ingest, Priority Index, CI Pipeline)
- **Objective:** Fix various issues and improve tooling
- **Features Implemented:**
    1. **Server Shutdown Fix** (`server/sse/sse.go`)
        - Fixed SSE hub blocking on shutdown - Run() goroutine exited but clients still tried to unregister
        - Added `done` channel to Hub struct for clean shutdown signaling
        - Made unregister non-blocking in ServeHTTP when hub is closing
    2. **LFS Support in Go CLI** (`server/cmd/blobforge/main.go`)
        - Enhanced ingest command with Git LFS pointer detection
        - Regex-based LFS hash extraction from pointer files
        - Auto-materialize LFS files before upload, revert after
        - Derive tags from file path (books/sci-fi/dune.pdf -> ["books", "sci-fi", "dune"])
        - New flags: --lfs (default true), --tags (default true)
    3. **Job Claim Index** (`server/db/migrations.go`)
        - Migration 6: Added covering index for efficient job claiming
        - `idx_jobs_claim ON jobs(status, type, priority, created_at)`
        - Optimizes 50k+ jobs with priority ordering
    4. **GitHub CI Pipeline** (`.github/workflows/container.yml`)
        - Updated to build separate images for server and pdf-worker
        - Images: `ghcr.io/tionis/blobforge/server`, `ghcr.io/tionis/blobforge/pdf-worker`
        - Added webserver branch to trigger builds
        - Parallel builds with separate cache scopes
    5. **Worker Auth Update** (`workers/pdf/worker.py`, `workers/pdf/README.md`)
        - Updated PDF worker to use X-Worker-ID and X-Worker-Secret headers
        - Added BLOBFORGE_WORKER_ID and BLOBFORGE_WORKER_SECRET config
        - Updated documentation with new worker credential flow
- **Status:** Complete. All tests pass.

## 2026-02-03 (CLI, Migrations, Tests, Dark Mode)
- **Objective:** Add Go admin CLI, migration system, comprehensive tests, and dark mode UI
- **Features Implemented:**
    1. **Go Admin CLI** (`server/cmd/blobforge/main.go`)
        - Cobra-based CLI replacing Python CLI
        - Commands: submit, ingest, stats, jobs (list/retry/cancel), workers (list/drain/remove), tokens (list/create/delete)
        - PDF ingestor with batch processing, SHA256 hashing, S3 upload via presigned URLs
        - Progress reporting for batch operations
    2. **SQLite Migration System** (`server/db/migrations.go`)
        - Uses user_version pragma to track schema version
        - Versioned migrations with up SQL
        - Auto-runs pending migrations on startup
        - Initial migrations: V1 (schema), V2 (auth tables), V3 (output_path)
    3. **Comprehensive API Tests** (`server/api/handlers_test.go`)
        - ~600 lines of tests covering all API endpoints
        - Worker API: register, heartbeat, list, drain, remove
        - Job API: create, list, get, retry, cancel, priority
        - Scheduling logic: priority order, type matching, busy workers
        - Failure scenarios: stale worker cleanup, concurrent claims, draining workers
    4. **Discord-inspired Dark Mode** (`server/web/static/style.css`)
        - CSS custom properties for theming
        - Discord color palette (#313338, #2b2d31, #1e1f22)
        - 3-way toggle: system/light/dark with localStorage persistence
        - JavaScript theme manager (`server/web/static/app.js`)
    5. **WebUI Enhancements**
        - All templates use base.html with navbar, theme toggle, footer
        - Worker management: add, drain, remove buttons
        - Embedded static assets in binary (no external CDN)
        - Status badges with proper colors for all states
- **Status:** Complete. Server compiles, all tests pass.

## 2026-02-03 (Production Features Implementation)
- **Objective:** Add authentication, real-time updates, and management features
- **Features Implemented:**
    1. **OIDC Authentication** (`server/auth/auth.go`)
        - Full OIDC flow with coreos/go-oidc library
        - Group-based access control (allowed groups, admin groups)
        - Session management with cookies
        - API token authentication for workers/CLI
    2. **Server-Sent Events** (`server/sse/sse.go`)
        - Live dashboard updates without polling
        - Event types: job_created, job_updated, worker_online, stats_updated
        - Hub architecture with client management
    3. **Worker Management**
        - Drain mode (stop accepting new jobs)
        - Remove worker from dashboard
        - Real-time status updates
    4. **Job Management**
        - Retry/cancel from dashboard
        - Priority updates (1-5)
        - HTMX partial updates
    5. **Database Enhancements** (`server/db/db.go`)
        - Users, sessions, api_tokens tables
        - Fixed NULL JSON scanning with custom nullableJSON type
        - UTC time comparison for stale workers
    6. **Tests** (`server/db/db_test.go`)
        - 12 comprehensive tests for all DB operations
        - All tests passing
    7. **Litestream Config** (`server/litestream.yml`)
        - SQLite replication to S3
        - 10s sync interval, 7 day retention
    8. **Documentation Updates**
        - README.md with auth, SSE, GPU acceleration docs
        - DESIGN.md with auth, SSE, priority sections
        - TODO.md updated with completed items
- **Status:** Complete. Server compiles, all tests pass.

## 2026-02-03 (Major Architecture Redesign)
- **Objective:** Migrate from S3-only coordination to centralized Go server with SQLite.
- **Motivation:**
    - S3-only coordination had polling inefficiency, complex locking, no ACID transactions
    - Hard to debug without centralized view
    - High S3 API costs from constant coordination calls
- **Actions:**
    - Designed new architecture with Go server as central coordinator
    - Created `server/` directory with Go implementation:
        - `main.go` - HTTP server with Chi router, graceful shutdown
        - `db/db.go` - SQLite database layer with migrations
        - `s3/s3.go` - S3 client with presigned URL generation
        - `api/handlers.go` - REST API handlers for workers and jobs
        - `web/web.go` - HTMX dashboard handlers and templates
    - Created `workers/pdf/` with Python HTTP client worker:
        - Simple loop: register → claim → process → complete/fail
        - Uses presigned URLs for file transfers
    - Created `cli/` with Python CLI for job submission
    - Removed old Python files (worker.py, janitor.py, ingestor.py, etc.)
    - Updated README.md with new architecture
    - Updated DESIGN.md with comprehensive documentation
    - Added docker-compose.yml for easy deployment
- **Status:** Complete. Server compiles and starts successfully.

## 2026-02-03 (Previous S3-only work - now replaced)
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

## 2026-02-04 (Per-Worker Authentication)
- **Objective:** Implement per-worker secrets for better security and individual revocation
- **Motivation:** 
    - Workers previously used shared API tokens
    - Compromised token affected all workers
    - No way to know which worker performed actions
    - No individual worker revocation
- **Features Implemented:**
    1. **Database Migration 5** (`server/db/migrations.go`)
        - Added `secret_hash TEXT` column to workers table
        - Added `enabled BOOLEAN DEFAULT TRUE` column
        - Index on enabled column for efficient queries
    2. **Worker Struct Updates** (`server/db/db.go`)
        - New fields: `SecretHash *string`, `Enabled bool`
        - `SecretHash` excluded from JSON (never exposed in API)
        - Updated all worker queries to include new columns
        - New functions: `UpdateWorkerSecret`, `SetWorkerEnabled`, `ValidateWorkerSecret`
    3. **Worker Auth Middleware** (`server/auth/auth.go`)
        - `WorkerAuthMiddleware` validates `X-Worker-ID` + `X-Worker-Secret` headers
        - Hashes provided secret, compares with stored hash
        - Checks worker is enabled before allowing access
        - `GenerateWorkerSecret()` creates secrets with `bfw_` prefix
    4. **Admin API Endpoints** (`server/api/handlers.go`)
        - `POST /api/admin/workers` - Create worker, returns one-time secret
        - `POST /api/admin/workers/{id}/regenerate` - Generate new secret
        - `POST /api/admin/workers/{id}/enable` - Re-enable worker
        - `POST /api/admin/workers/{id}/disable` - Disable worker (revoke access)
    5. **Web UI Updates** (`server/web/templates/workers.html`, `workers_table.html`)
        - Worker creation modal with ID, name, type inputs
        - Secret display modal (one-time view) with copy button
        - Environment variables display for easy copy
        - Enable/Disable/Regenerate/Delete buttons per worker
        - Disabled workers shown with reduced opacity
        - Admin-only actions (hidden for non-admins)
    6. **Route Updates** (`server/main.go`)
        - Worker-specific endpoints use `WorkerAuthMiddleware`
        - General API endpoints use standard token/session auth
        - Admin endpoints for worker management
    7. **Tests** (`server/api/handlers_test.go`)
        - `TestAdminCreateWorker` - creation flow, secret format
        - `TestAdminRegenerateWorkerSecret` - regeneration, hash changes
        - `TestAdminEnableDisableWorker` - enable/disable flow
        - `TestWorkerSecretValidation` - DB validation logic
        - `TestWorkerAuthMiddleware` - middleware integration
- **Status:** Complete. All tests pass.

