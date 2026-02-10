# Worker Graceful Shutdown

## Goal

Ensure that normal termination signals do not leave in-flight jobs stuck in `processing` until stale timeout.

## Behavior

Worker runtime now installs handlers for catchable shutdown signals:

- `SIGINT`
- `SIGTERM`
- `SIGHUP` (if available on platform)
- `SIGQUIT` (if available on platform)

On signal reception, the worker:

1. Breaks out of the polling/processing loop.
2. Runs shutdown logic.
3. If a job is currently active:
   - Reads lock metadata for priority/retries.
   - Rewrites or recreates the todo marker with:
     - `retries` (preserved, not incremented)
     - `queued_at`
     - `recovered_from: graceful_shutdown`
   - Releases processing lock.
4. Keeps signal handlers installed until shutdown cleanup finishes.
5. Stops heartbeat thread and deregisters worker.

## Ordering Guarantees

- Heartbeat thread is asked to stop first.
- Active job requeue + lock release runs before heartbeat join wait.
- Signal handlers are restored only after shutdown work completes.
- Unexpected exceptions in the worker loop still trigger graceful shutdown with requeue intent.

## Non-Catchable Kill Paths

Some terminations cannot be intercepted in-process:

- `SIGKILL`
- Linux OOM killer hard kill

Those cases are handled by startup recovery and janitor stale-lock recovery.
