# TODO List

## High Priority
- [ ] Add unit tests for S3 path prefixing logic.
- [ ] Implement a CLI entry point to wrap ingestor, worker, and janitor.

## Normal Priority
- [ ] Update README.md with configuration instructions for `S3_PREFIX`.
- [ ] Implement sharding logic in the worker lock acquisition.

## Done
- [x] Add S3 namespacing support via `S3_PREFIX`.
- [x] Refactor scripts to use central config.
- [x] Initialize agent protocols and documentation structure.
