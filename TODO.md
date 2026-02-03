# TODO List

## High Priority
- [ ] Test full end-to-end flow with real OIDC provider
- [ ] Add worker GPU health monitoring

## Normal Priority
- [ ] Add metrics/monitoring hooks (job duration, success rate, Prometheus)
- [ ] Add progress reporting during conversion (page count)
- [ ] Add batch job operations (retry all failed, cancel all pending)
- [ ] Add job dependencies (job A waits for job B)

## Low Priority
- [ ] Add webhooks for job completion notifications
- [ ] Add multi-tenancy support

## Done
- [x] Redesign architecture from S3-only to centralized Go server
- [x] Create Go server with SQLite database
- [x] Implement REST API for workers and jobs
- [x] Create HTMX dashboard
- [x] Refactor PDF worker to use HTTP API
- [x] Add presigned URL generation for S3
- [x] Add worker heartbeat and stale detection
- [x] Create CLI for job submission
- [x] Write comprehensive README
- [x] Add docker-compose for easy deployment
- [x] Add OIDC authentication with group-based access control
- [x] Add API token authentication for workers/CLI
- [x] Add SSE for real-time dashboard updates (replaces polling)
- [x] Add worker management (drain, remove) from dashboard
- [x] Add job management (retry, cancel, priority) from dashboard
- [x] Add flexible priority system (1-5)
- [x] Create Litestream configuration for SQLite backup
- [x] Add comprehensive database tests
- [x] Document GPU acceleration for marker-pdf workers
- [x] Update DESIGN.md with auth and SSE sections
- [x] Implement Go admin CLI with cobra (submit, ingest, stats, jobs, workers, tokens)
- [x] Add SQLite migration system using user_version pragma
- [x] Add comprehensive API and scheduling tests
- [x] Add Discord-inspired dark mode with 3-way toggle (system/light/dark)
- [x] Worker management features in WebUI (add/drain/remove)
- [x] Embed all web assets (CSS, JS) in the binary
