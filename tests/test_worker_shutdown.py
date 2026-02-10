"""
Unit tests for graceful worker shutdown behavior.
"""
import json
import unittest
from unittest.mock import ANY, MagicMock, call, patch

from blobforge.config import S3_PREFIX_TODO
from blobforge.worker import Worker, run_worker_loop


class TestWorkerGracefulShutdown(unittest.TestCase):
    """Validate graceful shutdown and active-job requeue behavior."""

    def _build_worker(self):
        s3 = MagicMock()
        s3.list_processing.return_value = []
        heartbeat = MagicMock()

        with patch("blobforge.worker.WORKER_ID", "atlantis"), \
             patch("blobforge.worker.HeartbeatThread", return_value=heartbeat), \
             patch("blobforge.worker.get_max_retries", return_value=3):
            worker = Worker(s3)

        return worker, s3, heartbeat

    def test_shutdown_requeues_active_job(self):
        worker, s3, heartbeat = self._build_worker()
        job_hash = "job123"
        priority = "2_high"
        todo_key = f"{S3_PREFIX_TODO}/{priority}/{job_hash}"

        worker.current_job = job_hash
        worker.current_priority = priority

        s3.get_lock_info.return_value = {"priority": priority, "retries": 2}
        s3.get_object_json.return_value = {"original_name": "sample.pdf", "retries": 1}

        worker.shutdown(requeue_current_job=True)

        todo_write = None
        for write_call in s3.put_object.call_args_list:
            if write_call.args and write_call.args[0] == todo_key:
                todo_write = write_call
                break

        self.assertIsNotNone(todo_write, "Expected active job to be written back to todo queue")
        payload = json.loads(todo_write.args[1])
        self.assertEqual(payload["retries"], 2)
        self.assertEqual(payload["recovered_from"], "graceful_shutdown")
        self.assertEqual(payload["original_name"], "sample.pdf")

        s3.release_lock.assert_called_once_with(job_hash)
        s3.deregister_worker.assert_called_once()
        heartbeat.stop.assert_called_once()
        heartbeat.join.assert_called_once()
        self.assertIsNone(worker.current_job)
        self.assertIsNone(worker.current_priority)
    
    def test_shutdown_requeues_before_heartbeat_join(self):
        worker, s3, heartbeat = self._build_worker()
        tracker = MagicMock()
        tracker.attach_mock(heartbeat, "heartbeat")
        tracker.attach_mock(s3, "s3")
        
        job_hash = "order123"
        priority = "3_normal"
        worker.current_job = job_hash
        worker.current_priority = priority
        s3.get_lock_info.return_value = {"priority": priority, "retries": 0}
        s3.get_object_json.return_value = {}
        
        worker.shutdown(requeue_current_job=True)
        
        calls = tracker.mock_calls
        stop_idx = calls.index(call.heartbeat.stop())
        put_idx = next(i for i, c in enumerate(calls) if c == call.s3.put_object(f"{S3_PREFIX_TODO}/{priority}/{job_hash}", ANY))
        join_idx = calls.index(call.heartbeat.join(timeout=ANY))
        self.assertLess(stop_idx, put_idx)
        self.assertLess(put_idx, join_idx)

    def test_shutdown_with_no_active_job_does_not_requeue(self):
        worker, s3, heartbeat = self._build_worker()

        worker.shutdown(requeue_current_job=True)

        s3.put_object.assert_not_called()
        s3.release_lock.assert_not_called()
        s3.deregister_worker.assert_called_once()
        heartbeat.stop.assert_called_once()
        heartbeat.join.assert_called_once()


class TestWorkerRunLoop(unittest.TestCase):
    """Validate run loop shutdown semantics."""

    def test_keyboard_interrupt_requeues_current_job(self):
        worker = MagicMock()
        worker.id = "atlantis"
        worker.acquire_job.side_effect = KeyboardInterrupt()

        with patch("blobforge.worker._install_shutdown_handlers", return_value={}), \
             patch("blobforge.worker._restore_shutdown_handlers"):
            rc = run_worker_loop(worker, run_once=False, idle_sleep=0)

        self.assertEqual(rc, 0)
        worker.shutdown.assert_called_once_with(requeue_current_job=True)
    
    def test_run_loop_restores_handlers_after_shutdown(self):
        worker = MagicMock()
        worker.id = "atlantis"
        worker.acquire_job.side_effect = KeyboardInterrupt()
        events = []
        
        def mock_shutdown(*args, **kwargs):
            events.append("shutdown")
        
        def mock_restore(*args, **kwargs):
            events.append("restore")
        
        worker.shutdown.side_effect = mock_shutdown
        
        with patch("blobforge.worker._install_shutdown_handlers", return_value={}), \
             patch("blobforge.worker._restore_shutdown_handlers", side_effect=mock_restore):
            run_worker_loop(worker, run_once=False, idle_sleep=0)
        
        self.assertEqual(events, ["shutdown", "restore"])

    def test_run_once_without_jobs_exits_cleanly(self):
        worker = MagicMock()
        worker.id = "atlantis"
        worker.acquire_job.return_value = None

        with patch("blobforge.worker._install_shutdown_handlers", return_value={}), \
             patch("blobforge.worker._restore_shutdown_handlers"):
            rc = run_worker_loop(worker, run_once=True, idle_sleep=0)

        self.assertEqual(rc, 0)
        worker.shutdown.assert_called_once_with(requeue_current_job=False)
        worker.process.assert_not_called()
    
    def test_unhandled_exception_still_attempts_graceful_shutdown(self):
        worker = MagicMock()
        worker.id = "atlantis"
        worker.acquire_job.side_effect = RuntimeError("boom")
        
        with patch("blobforge.worker._install_shutdown_handlers", return_value={}), \
             patch("blobforge.worker._restore_shutdown_handlers"):
            rc = run_worker_loop(worker, run_once=False, idle_sleep=0)
        
        self.assertEqual(rc, 1)
        worker.shutdown.assert_called_once_with(requeue_current_job=True)


if __name__ == "__main__":
    unittest.main()
