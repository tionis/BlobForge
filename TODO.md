# TODO List

## High Priority
- [ ] Add unit tests for S3 operations and queue state transitions.
- [ ] Test heartbeat mechanism under load.

## Normal Priority
- [ ] Update README.md with full configuration reference.
- [ ] Add metrics/monitoring hooks (job duration, success rate).
- [ ] Consider adding optional SQS/SNS integration for larger scale.

## Low Priority
- [ ] Add progress reporting during conversion (page count).
- [ ] Implement optional SQLite + Litestream for manifest storage.

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
