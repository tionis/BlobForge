# Worker Run Windows

## Goal

Allow a worker to limit CPU-intensive conversion work to local-time windows, for example overnight.

## CLI Options

```bash
blobforge worker --run-window 22:00-06:00
blobforge worker --run-window 22:00-06:00 --abort-outside-window
blobforge worker --isolate-conversion
blobforge worker --run-window 22:00-23:59 --run-window 00:00-06:00
blobforge worker --run-window 06:00-08:00,22:00-23:30
```

`--run-window` uses the machine's local time and accepts `HH:MM-HH:MM`.
The option can be repeated, and each value can contain comma-separated windows.
Windows may cross midnight.

## Behavior

- Outside the run window, the worker stays idle and does not acquire new jobs.
- When outside all windows, the worker sleeps until the next configured window opens rather than polling periodically.
- Inside the run window, normal polling and processing behavior applies.
- With no extra flags, an already-running job is allowed to finish even if the window closes.
- With `--abort-outside-window`, active conversion is interrupted when the window closes.
- Scheduled aborts requeue the active job and release the processing lock immediately.
- `--abort-outside-window` automatically runs marker conversion in an isolated child process.
- `--isolate-conversion` can be used without a schedule to keep native marker/PyTorch crashes from killing the worker process.

## Abort Mechanism

Scheduled aborts use subprocess supervision:

1. The worker computes the remaining seconds in the active run window.
2. If the schedule boundary is sooner than `conversion_timeout`, the child process timeout is set to that boundary.
3. When the timeout fires, the worker kills the conversion child and raises a schedule-specific abort instead of a conversion failure.
4. The active job is requeued with `recovered_from: schedule_window_closed`.

Normal conversion timeout still applies. In the default in-process mode it uses `SIGALRM` / `ITIMER_REAL` when supported. In isolated mode the parent process enforces the timeout around the child process.

## Limitations

- The schedule is local to the worker process; it is not stored in remote S3 config.
- Isolated conversion reloads marker models per job, so it is more robust but slower than the default in-process model cache.
- Non-conversion work such as final packaging may continue briefly past a window boundary.
- Shutdown signals still interrupt the outside-window sleep and trigger normal graceful shutdown.
