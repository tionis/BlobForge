# Worker Conversion Timeout

## Goal

Enforce a hard ceiling for long-running conversion calls so a single file cannot block a worker indefinitely.

## Mechanism

- Worker reads `conversion_timeout` from remote config.
- Conversion is executed through a timeout wrapper.
- On supported platforms, wrapper uses:
  - `SIGALRM`
  - `signal.setitimer(signal.ITIMER_REAL, timeout)`
- If timeout fires, conversion raises `TimeoutError` and follows normal failure handling.

## Fallback Behavior

Hard timeout is skipped when timer-based signals are unavailable:

- No `SIGALRM`/`setitimer` support
- Worker not running on main thread

In those cases, conversion still runs, and a warning is logged.

## Notes

- This is best-effort process-local enforcement.
- Uncatchable termination (e.g. OOM hard kill) still relies on startup and janitor recovery logic.
