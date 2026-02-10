# TODO List

## High Priority
- [ ] Add unit tests for S3 operations and queue state transitions.
- [ ] Test heartbeat mechanism under load.

## Normal Priority
- [ ] Update README.md with full configuration reference.
- [ ] Consider adding optional SQS/SNS integration for larger scale.

## Low Priority
- [ ] Implement optional SQLite + Litestream for manifest storage.

## Done
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
