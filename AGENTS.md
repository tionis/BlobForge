# Agent Instructions

All LLM agents operating in this repository MUST adhere to the following protocols:

- **Documentation:** Document all architectural decisions, system components, and significant logic in the `docs/` directory.
- **Task Management:** Use `TODO.md` in the root directory to store, track, and look up all pending and completed todo items.
- **Activity Logging:** Log all actions, tool executions, and progress in `docs/WORK_LOG.md`.
- **Knowledge Sharing:** Note all significant findings, codebase insights, or updated protocols directly in this file (`AGENTS.md`) under the "Findings" section.

## Findings

- **2026-02-03:** Major architecture redesign - migrated from S3-only coordination to centralized Go server with SQLite.
- **2026-02-03:** New Go server in `server/` with Chi router, SQLite database, HTMX dashboard.
- **2026-02-03:** Workers are now simple HTTP clients that poll the server API for jobs.
- **2026-02-03:** S3 is now used only for file storage (sources/, outputs/), not coordination.
- **2026-02-03:** Added presigned URL generation for secure worker file transfers.
- **2026-02-03:** PDF worker refactored to `workers/pdf/` as HTTP client using marker-pdf.
- **2026-02-03:** Created Python CLI tool in `cli/` for job submission and status checking.
- **2026-02-03:** Added docker-compose.yml for easy deployment of server + workers.
- **2026-02-03:** Created Go admin CLI in `server/cmd/blobforge/` with Cobra - supports submit, ingest, stats, jobs, workers, tokens commands.
- **2026-02-03:** Implemented SQLite migration system using `user_version` pragma in `server/db/migrations.go`.
- **2026-02-03:** Added comprehensive API tests in `server/api/handlers_test.go` (~600 lines covering all endpoints and failure scenarios).
- **2026-02-03:** Added Discord-inspired dark mode with 3-way toggle (system/light/dark) via CSS custom properties.
- **2026-02-03:** All web assets (CSS, JS, templates) embedded in binary using Go's `embed.FS`.
- **2026-02-03:** Introduced `S3Client` interface in `server/api/handlers.go` for dependency injection in tests.
- **2026-02-03:** Added proper Litestream integration to server container with exec pattern.
    - New `server/run.sh` - handles restore and execs Litestream
    - Updated `server/Containerfile` - includes Litestream v0.3.13
    - Reuses same `BLOBFORGE_S3_*` credentials (backups go to `litestream/` prefix)
    - Optional Age encryption via `BLOBFORGE_LITESTREAM_AGE_*` env vars
- **2026-02-04:** Implemented per-worker authentication with secrets.
    - Migration 5 adds `secret_hash` and `enabled` columns to workers table
    - Worker struct updated with `SecretHash` and `Enabled` fields
    - New `WorkerAuthMiddleware` validates `X-Worker-ID` + `X-Worker-Secret` headers
    - Admin API endpoints: create worker (returns one-time secret), regenerate secret, enable/disable
    - Web UI updated with worker creation modal, secret display, enable/disable/regenerate buttons
    - Worker secrets use `bfw_` prefix (vs `bf_` for API tokens)
    - Tests added for all new functionality
- **2026-02-04:** Fixed server shutdown timeout issue.
    - SSE hub Run() goroutine exited but clients blocked on unregister channel
    - Added `done` channel to Hub struct for clean shutdown signaling
    - Made unregister non-blocking when hub is closing
- **2026-02-04:** Enhanced Go CLI ingest command with Git LFS support.
    - Detects LFS pointer files via regex (`oid sha256:...`)
    - Auto-materializes LFS files before upload, reverts after
    - Derives tags from file path structure
    - New flags: --lfs (default true), --tags (default true)
- **2026-02-04:** Added migration 6 with covering index for efficient job claims.
    - `idx_jobs_claim ON jobs(status, type, priority, created_at)`
    - Handles 50k+ jobs with priority ordering efficiently
- **2026-02-04:** Updated GitHub CI pipeline for multi-image builds.
    - Builds server and pdf-worker images separately
    - Images: `ghcr.io/{owner}/blobforge/server`, `ghcr.io/{owner}/blobforge/pdf-worker`
    - Triggers on both main and webserver branches
- **2026-02-04:** Updated PDF worker to use worker auth headers (X-Worker-ID, X-Worker-Secret).
