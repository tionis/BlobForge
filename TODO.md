# TODO List

## High Priority
- [ ] Add unit tests for S3 operations and queue state transitions.
- [ ] Test heartbeat mechanism under load.

## Normal Priority
- [ ] Update README.md with full configuration reference.
- [ ] Add a separate dynamic-registration token flow for intentionally reusable bootstrap credentials, issuing distinct incremented worker IDs and per-worker credentials at registration time.
- [ ] Consider adding optional SQS/SNS integration for larger scale.

## Low Priority

## Done
- [x] Add a public, CDN-cacheable Edge Script documentation landing page.
- [x] Reduce coordinator heartbeat traffic with suspension-aware workers, dynamic intervals, and lease-only mode.
- [x] Hide revoked workers from normal fleet views and expose them separately.
- [x] Add CPU/CUDA worker images plus a no-clone Linux systemd installation workflow.
- [x] Remediate the 33 Dependabot alerts with patched universal-lock constraints, Python 3.10+, conversion-tested Pillow override, and documented non-applicable Transformer/Torch advisories.
- [x] Derive worker IDs directly from label slugs and reject duplicate/colliding enrollment labels.
- [x] Make coordinator progress updates prompt and add persistent per-attempt failure diagnostics with a Web UI history viewer.
- [x] Replace the preview's handwritten Markdown subset with Marked + DOMPurify and add a sticky/collapsible active-section ToC.
- [x] Add Web UI PDF ingestion, paginated library search/filtering, PDF/ZIP downloads, and client-side result previews.
- [x] Add application-level Bunny Database backups to S3 and a management UI trigger.
- [x] Remove manifest/log/Telegram dependencies and add dry-run-first legacy S3 cleanup.
- [x] Add per-worker UI enrollment/revocation, coordinator-issued S3 transfer URLs, and coordinator-backed worker/dashboard CLI views.
- [x] Replace unavailable Bunny admin session cookies with a fragment-bootstrap, browser-stored signed session and authenticated API header.
- [x] Make Bunny admin session cookies scheme-independent, disable CDN auth caching, and add an auth transport diagnostic endpoint.
- [x] Preserve strict CSP while allowing the IndieAuth form redirect through script-driven top-level navigation.
- [x] Fix Bunny IndieAuth cross-edge login sessions and add normalized profile input with a multi-admin allowlist.
- [x] Replace S3 queue coordination with Bunny Edge Scripting + Bunny Database, IndieAuth management UI, fenced leases, and legacy-state migration.
- [x] Isolate scheduled-abort worker conversions in a child process so native marker crashes do not kill the worker.
- [x] Add worker run-window scheduling with optional active-job abort/requeue.
- [x] Requeue all current failed, dead-letter, and stale processing jobs after follow-up investigation.
- [x] Investigate current failed/dead-letter jobs after rerun.
- [x] Remove the two PDFium data-format failure jobs from dead-letter, raw store, manifest, and logs.
- [x] Requeue non-PDFium dead-letter jobs for retry while leaving PDFium data-format failures in dead-letter.
- [x] Investigate current failed/dead-letter PDF queue and assess local retry viability.
- [x] Add `blobforge repair-metadata` to restore stripped raw-object metadata from manifest entries after S3 provider migrations.
- [x] Investigate `blobforge dashboard` `unknown.pdf` display and confirm whether it reflects missing data or only missing raw-object metadata.
- [x] Optimize `blobforge hydrate` with local hash preflight + single manifest prefilter to reduce per-file remote checks.
- [x] Add `blobforge hydrate` command to materialize `<stem>.md` and `<stem>.assets/` from completed conversions.
- [x] Document worker graceful shutdown and conversion timeout behavior in `README.md`.
- [x] Keep shutdown signal handlers active until cleanup completes and route unexpected loop exceptions through graceful shutdown.
- [x] Requeue active jobs before heartbeat join wait to release processing locks promptly on shutdown.
- [x] Resolve startup recovery retry undercount by reconciling lock + todo retry metadata.
- [x] Enforce conversion timeout in worker conversion path (platform-supported hard timeout with signal timers).
- [x] Add subprocess-level SIGTERM integration test for worker loop graceful shutdown path.
- [x] Add signal-aware graceful worker shutdown that requeues the in-flight job before exiting.
- [x] Count recovered processing locks as failed attempts during worker startup (increment retry, dead-letter when exceeded).
- [x] Enhanced Heartbeat Metadata (CPU/RAM, page progress, elapsed time)
- [x] Richer Dashboard / Status Display (filenames, sizes, progress)
- [x] Job Throughput Metrics (jobs_completed, avg_time, bytes_processed)
- [x] Job Logs / Error Details (store logs, `blobforge logs <hash>`)
- [x] Better CLI Experience (`blobforge watch`, cancel, download, preview)
- [x] Worker Management (health status, active filter, metrics display)
- [x] Queue Management (bulk retry, clear dead, search by filename)
- [x] Output/Results Features (`blobforge download`, `blobforge preview`)
- [x] Add S3 namespacing support via `S3_PREFIX`.
- [x] Refactor scripts to use central config.
- [x] Initialize agent protocols and documentation structure.
- [x] Consolidate S3Client into single module.
- [x] Implement sharding logic (256 shards with 2-char prefix).
- [x] Fix race condition in job acquisition.
- [x] Add heartbeat mechanism with configurable timeout.
- [x] Add retry limits and dead-letter queue.
- [x] Make ingestor state-aware (check all queues).
- [x] Add CLI retry command.
- [x] Generate persistent worker ID from machine fingerprint.
- [x] Fix DESIGN.md and document all features.

## Done
- [x] Add S3 namespacing support via `S3_PREFIX`.
- [x] Refactor scripts to use central config.
- [x] Initialize agent protocols and documentation structure.
- [x] Consolidate S3Client into single module.
- [x] Implement sharding logic (256 shards with 2-char prefix).
- [x] Fix race condition in job acquisition.
- [x] Add heartbeat mechanism with configurable timeout.
- [x] Add retry limits and dead-letter queue.
- [x] Make ingestor state-aware (check all queues).
- [x] Add CLI retry command.
- [x] Generate persistent worker ID from machine fingerprint.
- [x] Fix DESIGN.md and document all features.
