# Worker Startup Recovery Policy

## Problem

If a worker process dies abruptly (for example OOM kill or forced restart), the in-process failure handler is not executed. Before this change, startup recovery moved old processing locks back to todo without incrementing retries, which could create an infinite restart loop at `retry=0`.

## Decision

Treat startup-recovered processing locks as failed attempts.

## Recovery Algorithm

When a worker starts and finds `queue/processing/<hash>` entries owned by its own `WORKER_ID`:

1. Read retry count from the lock (`retries`, default `0`).
2. Increment retry count by 1.
3. If incremented retries are within budget (`<= max_retries`):
   - Requeue job to todo with metadata:
     - `retries`
     - `queued_at`
     - `recovered_from=worker_restart`
4. If incremented retries exceed budget (`> max_retries`):
   - Move job directly to dead-letter queue.
   - Remove stale todo markers for all priorities.
5. Release processing lock.

## Why This Works

- Crash/restart cycles now consume retry budget instead of resetting attempts.
- Jobs that repeatedly crash eventually move to dead-letter for manual intervention.
- Workers no longer loop indefinitely on the same problematic file after restarts.
