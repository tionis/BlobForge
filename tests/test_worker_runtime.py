"""
Runtime robustness tests for worker loop and conversion timeout behavior.
"""
import signal
import subprocess
import sys
import textwrap
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

from blobforge.worker import Worker


class TestConversionTimeout(unittest.TestCase):
    """Validate conversion timeout enforcement code paths."""

    def _build_worker(self):
        s3 = MagicMock()
        s3.list_processing.return_value = []
        heartbeat = MagicMock()

        with patch("blobforge.worker.WORKER_ID", "atlantis"), \
             patch("blobforge.worker.HeartbeatThread", return_value=heartbeat), \
             patch("blobforge.worker.get_max_retries", return_value=3):
            worker = Worker(s3)

        return worker

    def test_conversion_timeout_uses_alarm_timer(self):
        worker = self._build_worker()

        with patch.object(worker, "_run_marker_conversion", return_value=("md", {}, {})) as run_conv, \
             patch("blobforge.worker.signal.getitimer", return_value=(0.0, 0.0)) as getit, \
             patch("blobforge.worker.signal.getsignal", return_value=signal.SIG_DFL) as getsig, \
             patch("blobforge.worker.signal.signal") as set_handler, \
             patch("blobforge.worker.signal.setitimer") as setit, \
             patch("blobforge.worker.threading.current_thread", return_value=threading.main_thread()):
            result = worker._run_conversion_with_timeout("source.pdf", timeout_seconds=9)

        self.assertEqual(result, ("md", {}, {}))
        run_conv.assert_called_once_with("source.pdf")
        getit.assert_called_once_with(signal.ITIMER_REAL)
        getsig.assert_called_once_with(signal.SIGALRM)
        setit.assert_any_call(signal.ITIMER_REAL, 9)
        setit.assert_any_call(signal.ITIMER_REAL, 0.0, 0.0)
        self.assertGreaterEqual(set_handler.call_count, 2)

    def test_conversion_timeout_disabled_runs_directly(self):
        worker = self._build_worker()
        with patch.object(worker, "_run_marker_conversion", return_value=("md", {}, {})) as run_conv, \
             patch("blobforge.worker.signal.setitimer") as setit:
            result = worker._run_conversion_with_timeout("source.pdf", timeout_seconds=0)

        self.assertEqual(result, ("md", {}, {}))
        run_conv.assert_called_once_with("source.pdf")
        setit.assert_not_called()

    def test_conversion_timeout_skips_timer_outside_main_thread(self):
        worker = self._build_worker()
        with patch.object(worker, "_run_marker_conversion", return_value=("md", {}, {})) as run_conv, \
             patch("blobforge.worker.threading.main_thread", return_value=object()), \
             patch("blobforge.worker.signal.setitimer") as setit:
            result = worker._run_conversion_with_timeout("source.pdf", timeout_seconds=5)

        self.assertEqual(result, ("md", {}, {}))
        run_conv.assert_called_once_with("source.pdf")
        setit.assert_not_called()


@unittest.skipUnless(hasattr(signal, "SIGTERM"), "SIGTERM unavailable on this platform")
class TestSignalSubprocessIntegration(unittest.TestCase):
    """End-to-end signal handling test using a real subprocess."""

    def test_sigterm_triggers_graceful_shutdown_path(self):
        script = textwrap.dedent(
            """
            import time
            from blobforge.worker import run_worker_loop

            class FakeWorker:
                def __init__(self):
                    self.id = "sigtest"
                    self._empty_poll_count = 0

                def acquire_job(self):
                    print("loop_started", flush=True)
                    time.sleep(60)
                    return None

                def process(self, job):
                    pass

                def get_poll_backoff(self):
                    return 60

                def shutdown(self, requeue_current_job=False, reason="graceful_shutdown"):
                    print(f"shutdown={requeue_current_job},reason={reason}", flush=True)

            run_worker_loop(FakeWorker(), run_once=False, idle_sleep=60)
            """
        )

        proc = subprocess.Popen(
            [sys.executable, "-u", "-c", script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            ready = proc.stdout.readline().strip()
            self.assertEqual(ready, "loop_started")
            proc.send_signal(signal.SIGTERM)
            stdout, stderr = proc.communicate(timeout=15)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.communicate(timeout=5)

        self.assertEqual(proc.returncode, 0, msg=f"stderr:\n{stderr}")
        self.assertIn("shutdown=True,reason=graceful_shutdown", stdout)


if __name__ == "__main__":
    unittest.main()
