"""
Runtime robustness tests for worker loop and conversion timeout behavior.
"""
import json
import os
import signal
import subprocess
import sys
import tempfile
import textwrap
import threading
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from blobforge.worker import ScheduleWindowClosed, Worker, WorkerSchedule, run_worker_loop


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

    def test_schedule_abort_shortens_conversion_timeout(self):
        worker = self._build_worker()
        schedule = WorkerSchedule(
            [(22 * 3600, 6 * 3600)],
            abort_running=True,
            now_fn=lambda: datetime(2026, 1, 1, 23, 30),
        )

        timeout, schedule_abort = worker._resolve_conversion_timeout(86400, schedule)

        self.assertEqual(timeout, 6 * 3600 + 30 * 60)
        self.assertTrue(schedule_abort)

    def test_schedule_without_abort_does_not_change_conversion_timeout(self):
        worker = self._build_worker()
        schedule = WorkerSchedule(
            [(22 * 3600, 6 * 3600)],
            abort_running=False,
            now_fn=lambda: datetime(2026, 1, 1, 23, 30),
        )

        timeout, schedule_abort = worker._resolve_conversion_timeout(86400, schedule)

        self.assertEqual(timeout, 86400)
        self.assertFalse(schedule_abort)

    def test_isolated_conversion_reads_child_output(self):
        worker = self._build_worker()

        class FakeProcess:
            returncode = 0

            def communicate(self, timeout=None):
                return ('{"output_chars": 2, "image_count": 3}\n', "")

        with tempfile.TemporaryDirectory() as tmp_dir:
            out_dir = os.path.join(tmp_dir, "output")
            os.makedirs(out_dir)
            with open(os.path.join(out_dir, "content.md"), "w", encoding="utf-8") as f:
                f.write("md")
            with open(os.path.join(out_dir, "marker_meta.json"), "w", encoding="utf-8") as f:
                json.dump({"page_stats": [{}, {}]}, f)
            with open(os.path.join(out_dir, "conversion_result.json"), "w", encoding="utf-8") as f:
                json.dump({"image_count": 3}, f)

            with patch("blobforge.worker.subprocess.Popen", return_value=FakeProcess()) as popen:
                md_text, image_count, marker_meta = worker._run_conversion_subprocess(
                    "source.pdf",
                    out_dir,
                    timeout_seconds=5,
                )

            self.assertEqual(md_text, "md")
            self.assertEqual(image_count, 3)
            self.assertEqual(marker_meta, {"page_stats": [{}, {}]})
            self.assertFalse(os.path.exists(os.path.join(out_dir, "marker_meta.json")))
            self.assertFalse(os.path.exists(os.path.join(out_dir, "conversion_result.json")))
            popen.assert_called_once()

    def test_isolated_schedule_timeout_kills_child_and_requeues(self):
        worker = self._build_worker()

        class FakeProcess:
            returncode = None
            killed = False

            def communicate(self, timeout=None):
                if not self.killed:
                    raise subprocess.TimeoutExpired(["worker"], timeout)
                return "", "timed out"

            def kill(self):
                self.killed = True

        fake_proc = FakeProcess()

        with tempfile.TemporaryDirectory() as tmp_dir, \
             patch("blobforge.worker.subprocess.Popen", return_value=fake_proc):
            with self.assertRaises(ScheduleWindowClosed):
                worker._run_conversion_subprocess(
                    "source.pdf",
                    tmp_dir,
                    timeout_seconds=1,
                    timeout_reason="schedule_window_closed",
                )

        self.assertTrue(fake_proc.killed)


class TestCoordinatorObjectTransfers(unittest.TestCase):
    def test_worker_uses_enrollment_identity_and_signed_transfers(self):
        job_hash = "a" * 64
        s3 = MagicMock()
        s3.mock = True
        coordinator = MagicMock()
        coordinator.available = True
        coordinator.worker_identity.return_value = "enrolled-worker"
        coordinator.claim_job.return_value = {
            "hash": job_hash,
            "priority": "3_normal",
            "lease_token": "lease-1",
            "retry_count": 0,
            "original_name": "source.pdf",
            "size_bytes": 123,
            "tags": ["test"],
            "input": {"url": "https://s3.example/signed-input"},
            "output_exists": False,
        }

        def download(_job, path):
            with open(path, "wb") as target:
                target.write(b"%PDF-1.4 mock")

        coordinator.download_job_input.side_effect = download
        heartbeat = MagicMock()
        heartbeat._format_duration.return_value = "1s"

        with patch("blobforge.worker.HeartbeatThread", return_value=heartbeat), patch("blobforge.worker.get_pdf_page_count", return_value=1):
            worker = Worker(s3, coordinator_client=coordinator)
            self.assertEqual(worker.id, "enrolled-worker")
            self.assertEqual(worker.acquire_job(), job_hash)
            worker.process(job_hash)

        coordinator.download_job_input.assert_called_once()
        coordinator.upload_job_output.assert_called_once()
        coordinator.complete.assert_called_once()
        s3.download_file.assert_not_called()
        s3.get_object_metadata.assert_not_called()
        s3.upload_file.assert_not_called()
        s3.exists.assert_not_called()


class TestWorkerSchedule(unittest.TestCase):
    """Validate local-time worker run window parsing and gating."""

    def test_cross_midnight_window(self):
        schedule = WorkerSchedule.from_specs(["22:00-06:00"])

        self.assertTrue(schedule.is_allowed(datetime(2026, 1, 1, 23, 0)))
        self.assertTrue(schedule.is_allowed(datetime(2026, 1, 2, 5, 59)))
        self.assertFalse(schedule.is_allowed(datetime(2026, 1, 2, 12, 0)))

    def test_comma_separated_windows(self):
        schedule = WorkerSchedule.from_specs(["06:00-08:00,22:00-23:00"])

        self.assertTrue(schedule.is_allowed(datetime(2026, 1, 1, 7, 0)))
        self.assertTrue(schedule.is_allowed(datetime(2026, 1, 1, 22, 30)))
        self.assertFalse(schedule.is_allowed(datetime(2026, 1, 1, 12, 0)))

    def test_invalid_window_raises(self):
        with self.assertRaises(ValueError):
            WorkerSchedule.from_specs(["25:00-06:00"])


class TestScheduledWorkerRunLoop(unittest.TestCase):
    """Validate schedule-aware polling loop behavior."""

    def test_run_once_outside_window_does_not_acquire_job(self):
        worker = MagicMock()
        worker.id = "scheduled"
        schedule = WorkerSchedule(
            [(22 * 3600, 23 * 3600)],
            now_fn=lambda: datetime(2026, 1, 1, 12, 0),
        )

        with patch("blobforge.worker._install_shutdown_handlers", return_value={}), \
             patch("blobforge.worker._restore_shutdown_handlers"):
            rc = run_worker_loop(worker, run_once=True, idle_sleep=0, run_schedule=schedule)

        self.assertEqual(rc, 0)
        worker.acquire_job.assert_not_called()
        worker.shutdown.assert_called_once_with(requeue_current_job=False)

    def test_outside_window_sleeps_until_next_window(self):
        worker = MagicMock()
        worker.id = "scheduled"
        worker.acquire_job.side_effect = KeyboardInterrupt()
        current_time = {"value": datetime(2026, 1, 1, 12, 0)}
        schedule = WorkerSchedule(
            [(22 * 3600, 23 * 3600)],
            now_fn=lambda: current_time["value"],
        )

        def advance_to_window(_seconds):
            current_time["value"] = datetime(2026, 1, 1, 22, 30)

        with patch("blobforge.worker._install_shutdown_handlers", return_value={}), \
             patch("blobforge.worker._restore_shutdown_handlers"), \
             patch("blobforge.worker.time.sleep", side_effect=advance_to_window) as sleep:
            rc = run_worker_loop(worker, run_once=False, idle_sleep=10, run_schedule=schedule)

        self.assertEqual(rc, 0)
        sleep.assert_called_once_with(10 * 3600)
        worker.acquire_job.assert_called_once()
        worker.shutdown.assert_called_once_with(requeue_current_job=True)

    def test_run_loop_passes_schedule_to_process(self):
        worker = MagicMock()
        worker.id = "scheduled"
        worker.acquire_job.return_value = "job123"
        schedule = WorkerSchedule(
            [(22 * 3600, 23 * 3600)],
            now_fn=lambda: datetime(2026, 1, 1, 22, 30),
        )

        with patch("blobforge.worker._install_shutdown_handlers", return_value={}), \
             patch("blobforge.worker._restore_shutdown_handlers"):
            rc = run_worker_loop(worker, run_once=True, idle_sleep=0, run_schedule=schedule)

        self.assertEqual(rc, 0)
        worker.process.assert_called_once_with("job123", run_schedule=schedule)
        worker.shutdown.assert_called_once_with(requeue_current_job=False)


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
