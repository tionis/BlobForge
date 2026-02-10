"""
Unit tests for worker startup recovery behavior.
"""
import json
import unittest
from unittest.mock import MagicMock, call, patch

from blobforge.config import PRIORITIES, S3_PREFIX_PROCESSING, S3_PREFIX_TODO
from blobforge.worker import Worker


class TestWorkerStartupRecovery(unittest.TestCase):
    """Validate startup recovery retry/dead-letter handling."""

    def test_recovered_job_increments_retry_and_requeues(self):
        s3 = MagicMock()
        job_hash = "abc123"
        processing_key = f"{S3_PREFIX_PROCESSING}/{job_hash}"
        todo_key = f"{S3_PREFIX_TODO}/3_normal/{job_hash}"

        s3.list_processing.return_value = [{"Key": processing_key}]

        def mock_get_object_json(key):
            if key == processing_key:
                return {
                    "worker": "atlantis",
                    "priority": "3_normal",
                    "retries": 0,
                }
            if key == todo_key:
                return {"original_name": "sample.pdf"}
            return None

        s3.get_object_json.side_effect = mock_get_object_json

        with patch("blobforge.worker.WORKER_ID", "atlantis"), \
             patch("blobforge.worker.get_max_retries", return_value=3), \
             patch("blobforge.worker.HeartbeatThread"):
            Worker(s3)

        todo_write = None
        for write_call in s3.put_object.call_args_list:
            if write_call.args and write_call.args[0] == todo_key:
                todo_write = write_call
                break

        self.assertIsNotNone(todo_write, "Expected recovered job to be requeued in todo queue")
        payload = json.loads(todo_write.args[1])
        self.assertEqual(payload["retries"], 1)
        self.assertEqual(payload["recovered_from"], "worker_restart")
        self.assertEqual(payload["original_name"], "sample.pdf")

        s3.mark_dead.assert_not_called()
        s3.release_lock.assert_called_once_with(job_hash)

    def test_recovered_job_exceeding_retry_limit_moves_to_dead_letter(self):
        s3 = MagicMock()
        job_hash = "def456"
        processing_key = f"{S3_PREFIX_PROCESSING}/{job_hash}"

        s3.list_processing.return_value = [{"Key": processing_key}]
        s3.get_object_json.return_value = {
            "worker": "atlantis",
            "priority": "3_normal",
            "retries": 3,
        }

        with patch("blobforge.worker.WORKER_ID", "atlantis"), \
             patch("blobforge.worker.get_max_retries", return_value=3), \
             patch("blobforge.worker.HeartbeatThread"):
            Worker(s3)

        s3.mark_dead.assert_called_once()
        dead_call = s3.mark_dead.call_args
        self.assertEqual(dead_call.args[0], job_hash)
        self.assertEqual(dead_call.args[2], 4)

        expected_delete_calls = [call(f"{S3_PREFIX_TODO}/{priority}/{job_hash}") for priority in PRIORITIES]
        for expected_call in expected_delete_calls:
            self.assertIn(expected_call, s3.delete_object.call_args_list)

        self.assertFalse(
            any(
                c.args and c.args[0] == f"{S3_PREFIX_TODO}/3_normal/{job_hash}"
                for c in s3.put_object.call_args_list
            ),
            "Did not expect todo requeue for dead-lettered recovered job",
        )
        s3.release_lock.assert_called_once_with(job_hash)

    def test_recovery_ignores_locks_from_other_workers(self):
        s3 = MagicMock()
        job_hash = "ghi789"
        processing_key = f"{S3_PREFIX_PROCESSING}/{job_hash}"

        s3.list_processing.return_value = [{"Key": processing_key}]
        s3.get_object_json.return_value = {
            "worker": "other-worker",
            "priority": "3_normal",
            "retries": 1,
        }

        with patch("blobforge.worker.WORKER_ID", "atlantis"), \
             patch("blobforge.worker.get_max_retries", return_value=3), \
             patch("blobforge.worker.HeartbeatThread"):
            Worker(s3)

        s3.put_object.assert_not_called()
        s3.mark_dead.assert_not_called()
        s3.delete_object.assert_not_called()
        s3.release_lock.assert_not_called()

    def test_recovery_uses_max_retry_from_lock_and_todo_marker(self):
        s3 = MagicMock()
        job_hash = "maxretry"
        processing_key = f"{S3_PREFIX_PROCESSING}/{job_hash}"
        todo_key = f"{S3_PREFIX_TODO}/3_normal/{job_hash}"

        s3.list_processing.return_value = [{"Key": processing_key}]

        def mock_get_object_json(key):
            if key == processing_key:
                return {
                    "worker": "atlantis",
                    "priority": "3_normal",
                    "retries": 1,
                }
            if key == todo_key:
                return {"retries": 3}
            return None

        s3.get_object_json.side_effect = mock_get_object_json

        with patch("blobforge.worker.WORKER_ID", "atlantis"), \
             patch("blobforge.worker.get_max_retries", return_value=10), \
             patch("blobforge.worker.HeartbeatThread"):
            Worker(s3)

        todo_write = None
        for write_call in s3.put_object.call_args_list:
            if write_call.args and write_call.args[0] == todo_key:
                todo_write = write_call
                break

        self.assertIsNotNone(todo_write)
        payload = json.loads(todo_write.args[1])
        # max(1, 3) + 1
        self.assertEqual(payload["retries"], 4)


if __name__ == "__main__":
    unittest.main()
