# Agent Instructions

All LLM agents operating in this repository MUST adhere to the following protocols:

- **Documentation:** Document all architectural decisions, system components, and significant logic in the `docs/` directory.
- **Task Management:** Use `TODO.md` in the root directory to store, track, and look up all pending and completed todo items.
- **Activity Logging:** Log all actions, tool executions, and progress in `docs/WORK_LOG.md`.
- **Knowledge Sharing:** Note all significant findings, codebase insights, or updated protocols directly in this file (`AGENTS.md`) under the "Findings" section.

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
