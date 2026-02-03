"""
Unit tests for BlobForge.

Run with: python -m pytest tests/ -v
Or: python -m unittest tests.test_blobforge -v
"""
import unittest
import json
import time
import tempfile
import os
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime, timedelta

# Import modules under test
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import PRIORITIES, DEFAULT_PRIORITY, MAX_RETRIES, STALE_TIMEOUT_MINUTES
from config import S3_PREFIX_DONE, S3_PREFIX_PROCESSING, S3_PREFIX_FAILED, S3_PREFIX_DEAD, S3_PREFIX_TODO
from s3_client import S3Client


class TestS3ClientMock(unittest.TestCase):
    """Test S3Client in mock mode (no boto3)."""
    
    def setUp(self):
        """Set up mock S3 client."""
        self.client = S3Client(dry_run=True)
        self.client.mock = True
    
    def test_exists_returns_false_in_mock(self):
        """Mock mode should return False for exists checks."""
        self.assertFalse(self.client.exists("any/key"))
    
    def test_list_todo_returns_mock_hash(self):
        """list_todo should return mock data in mock mode."""
        result = self.client.list_todo("3_normal", "ab")
        self.assertEqual(len(result), 1)
        self.assertTrue(result[0].startswith("ab"))
    
    def test_get_object_metadata_returns_mock(self):
        """Mock mode should return mock metadata."""
        meta = self.client.get_object_metadata("any/key")
        self.assertIn("original-name", meta)
        self.assertEqual(meta["original-name"], "mock.pdf")


class TestS3ClientWithBoto3(unittest.TestCase):
    """Test S3Client with mocked boto3."""
    
    def setUp(self):
        """Set up S3 client with mocked boto3."""
        self.mock_s3 = MagicMock()
        self.client = S3Client(dry_run=False)
        self.client.mock = False
        self.client.s3 = self.mock_s3
    
    def test_exists_returns_true_when_object_exists(self):
        """exists() should return True when head_object succeeds."""
        self.mock_s3.head_object.return_value = {}
        self.assertTrue(self.client.exists("test/key"))
        self.mock_s3.head_object.assert_called_once()
    
    def test_exists_returns_false_when_object_missing(self):
        """exists() should return False when head_object raises."""
        self.mock_s3.head_object.side_effect = Exception("Not found")
        self.assertFalse(self.client.exists("test/key"))
    
    def test_put_object_with_if_none_match(self):
        """put_object should pass IfNoneMatch when requested."""
        self.client.put_object("test/key", "content", if_none_match=True)
        self.mock_s3.put_object.assert_called_once()
        call_kwargs = self.mock_s3.put_object.call_args[1]
        self.assertEqual(call_kwargs['IfNoneMatch'], '*')
    
    def test_put_object_without_if_none_match(self):
        """put_object should not pass IfNoneMatch by default."""
        self.client.put_object("test/key", "content", if_none_match=False)
        call_kwargs = self.mock_s3.put_object.call_args[1]
        self.assertNotIn('IfNoneMatch', call_kwargs)
    
    def test_get_object_returns_content(self):
        """get_object should return decoded content."""
        mock_body = MagicMock()
        mock_body.read.return_value = b'{"test": "data"}'
        self.mock_s3.get_object.return_value = {'Body': mock_body}
        
        result = self.client.get_object("test/key")
        self.assertEqual(result, '{"test": "data"}')
    
    def test_get_object_json_parses_json(self):
        """get_object_json should return parsed JSON."""
        mock_body = MagicMock()
        mock_body.read.return_value = b'{"test": "data"}'
        self.mock_s3.get_object.return_value = {'Body': mock_body}
        
        result = self.client.get_object_json("test/key")
        self.assertEqual(result, {"test": "data"})
    
    def test_get_object_json_returns_none_on_invalid_json(self):
        """get_object_json should return None for invalid JSON."""
        mock_body = MagicMock()
        mock_body.read.return_value = b'not json'
        self.mock_s3.get_object.return_value = {'Body': mock_body}
        
        result = self.client.get_object_json("test/key")
        self.assertIsNone(result)


class TestJobStateChecks(unittest.TestCase):
    """Test job state checking logic."""
    
    def setUp(self):
        """Set up S3 client with controlled exists behavior."""
        self.client = S3Client(dry_run=True)
        self.client.mock = False
        self.client.s3 = MagicMock()
        self.existing_keys = set()
        
        def mock_head_object(Bucket, Key):
            if Key in self.existing_keys:
                return {}
            raise Exception("Not found")
        
        self.client.s3.head_object.side_effect = mock_head_object
    
    def test_job_exists_done(self):
        """Should detect job in done state."""
        self.existing_keys.add(f"{S3_PREFIX_DONE}/abc123.zip")
        result = self.client.job_exists_anywhere("abc123", PRIORITIES)
        self.assertTrue(result['done'])
        self.assertFalse(result['processing'])
    
    def test_job_exists_processing(self):
        """Should detect job in processing state."""
        self.existing_keys.add(f"{S3_PREFIX_PROCESSING}/abc123")
        result = self.client.job_exists_anywhere("abc123", PRIORITIES)
        self.assertTrue(result['processing'])
        self.assertFalse(result['done'])
    
    def test_job_exists_failed(self):
        """Should detect job in failed state."""
        self.existing_keys.add(f"{S3_PREFIX_FAILED}/abc123")
        result = self.client.job_exists_anywhere("abc123", PRIORITIES)
        self.assertTrue(result['failed'])
    
    def test_job_exists_dead(self):
        """Should detect job in dead-letter state."""
        self.existing_keys.add(f"{S3_PREFIX_DEAD}/abc123")
        result = self.client.job_exists_anywhere("abc123", PRIORITIES)
        self.assertTrue(result['dead'])
    
    def test_job_exists_todo(self):
        """Should detect job in todo state."""
        self.existing_keys.add(f"{S3_PREFIX_TODO}/3_normal/abc123")
        result = self.client.job_exists_anywhere("abc123", PRIORITIES)
        self.assertTrue(result['todo'])
    
    def test_job_not_found(self):
        """Should return all False when job doesn't exist."""
        result = self.client.job_exists_anywhere("nonexistent", PRIORITIES)
        self.assertFalse(any(result.values()))


class TestLockingLogic(unittest.TestCase):
    """Test atomic locking behavior."""
    
    def setUp(self):
        """Set up S3 client for locking tests."""
        try:
            import botocore.exceptions
            self.botocore = botocore
        except ImportError:
            self.skipTest("botocore not installed")
        
        self.client = S3Client(dry_run=False)
        self.client.mock = False
        self.client.s3 = MagicMock()
        self.client.ClientError = botocore.exceptions.ClientError
    
    def test_acquire_lock_success(self):
        """Should return True when lock acquired."""
        self.client.s3.put_object.return_value = {}
        
        result = self.client.acquire_lock("abc123", "worker-1", "3_normal")
        self.assertTrue(result)
        
        # Verify IfNoneMatch was used
        call_kwargs = self.client.s3.put_object.call_args[1]
        self.assertEqual(call_kwargs['IfNoneMatch'], '*')
    
    def test_acquire_lock_fails_on_precondition(self):
        """Should return False when lock already exists (412)."""
        error_response = {'Error': {'Code': 'PreconditionFailed'}}
        self.client.s3.put_object.side_effect = self.botocore.exceptions.ClientError(
            error_response, 'PutObject'
        )
        
        result = self.client.acquire_lock("abc123", "worker-1", "3_normal")
        self.assertFalse(result)
    
    def test_lock_content_format(self):
        """Lock file should contain required fields."""
        self.client.s3.put_object.return_value = {}
        self.client.acquire_lock("abc123", "worker-1", "3_normal")
        
        call_kwargs = self.client.s3.put_object.call_args[1]
        body = json.loads(call_kwargs['Body'])
        
        self.assertEqual(body['worker'], "worker-1")
        self.assertEqual(body['priority'], "3_normal")
        self.assertIn('started', body)
        self.assertIn('last_heartbeat', body)


class TestHeartbeatLogic(unittest.TestCase):
    """Test heartbeat update behavior."""
    
    def setUp(self):
        """Set up S3 client for heartbeat tests."""
        self.client = S3Client(dry_run=False)
        self.client.mock = False
        self.client.s3 = MagicMock()
    
    def test_update_heartbeat_success(self):
        """Should update heartbeat when worker owns lock."""
        # Mock reading current lock
        current_lock = {
            "worker": "worker-1",
            "started": 1000,
            "last_heartbeat": 1000,
            "priority": "3_normal"
        }
        mock_body = MagicMock()
        mock_body.read.return_value = json.dumps(current_lock).encode()
        self.client.s3.get_object.return_value = {'Body': mock_body}
        
        result = self.client.update_heartbeat("abc123", "worker-1", {"stage": "converting"})
        self.assertTrue(result)
        
        # Verify new content was written
        self.client.s3.put_object.assert_called_once()
        call_kwargs = self.client.s3.put_object.call_args[1]
        body = json.loads(call_kwargs['Body'])
        self.assertIn('progress', body)
        self.assertEqual(body['progress']['stage'], 'converting')
    
    def test_update_heartbeat_fails_wrong_worker(self):
        """Should fail if another worker owns the lock."""
        current_lock = {
            "worker": "worker-2",  # Different worker
            "started": 1000,
            "last_heartbeat": 1000,
            "priority": "3_normal"
        }
        mock_body = MagicMock()
        mock_body.read.return_value = json.dumps(current_lock).encode()
        self.client.s3.get_object.return_value = {'Body': mock_body}
        
        result = self.client.update_heartbeat("abc123", "worker-1", None)
        self.assertFalse(result)
        
        # Should not write anything
        self.client.s3.put_object.assert_not_called()


class TestRetryLogic(unittest.TestCase):
    """Test retry counting and dead-letter queue logic."""
    
    def setUp(self):
        """Set up S3 client for retry tests."""
        self.client = S3Client(dry_run=False)
        self.client.mock = False
        self.client.s3 = MagicMock()
    
    def test_get_retry_count_from_failed(self):
        """Should read retry count from failed queue."""
        failed_data = {"error": "test", "retries": 2}
        mock_body = MagicMock()
        mock_body.read.return_value = json.dumps(failed_data).encode()
        self.client.s3.get_object.return_value = {'Body': mock_body}
        
        count = self.client.get_retry_count("abc123")
        self.assertEqual(count, 2)
    
    def test_get_retry_count_returns_zero_when_missing(self):
        """Should return 0 when no failure record exists."""
        self.client.s3.get_object.side_effect = Exception("Not found")
        
        count = self.client.get_retry_count("abc123")
        self.assertEqual(count, 0)
    
    def test_mark_failed_includes_details(self):
        """mark_failed should include error details."""
        self.client.mark_failed("abc123", "Connection error", "worker-1", retry_count=1)
        
        # Check failed marker content
        calls = self.client.s3.put_object.call_args_list
        failed_call = [c for c in calls if 'failed' in c[1]['Key']][0]
        body = json.loads(failed_call[1]['Body'])
        
        self.assertEqual(body['error'], "Connection error")
        self.assertEqual(body['worker'], "worker-1")
        self.assertEqual(body['retries'], 1)
        self.assertIn('failed_at', body)
    
    def test_mark_dead_includes_reason(self):
        """mark_dead should include exceeded retries reason."""
        self.client.mark_dead("abc123", "Persistent failure", total_retries=3)
        
        call_kwargs = self.client.s3.put_object.call_args[1]
        body = json.loads(call_kwargs['Body'])
        
        self.assertEqual(body['reason'], "exceeded_max_retries")
        self.assertEqual(body['total_retries'], 3)


class TestStaleDetection(unittest.TestCase):
    """Test stale job detection logic."""
    
    def test_stale_timeout_config(self):
        """Verify stale timeout is configured correctly."""
        # Default should be 15 minutes
        self.assertEqual(STALE_TIMEOUT_MINUTES, 15)
    
    def test_max_retries_config(self):
        """Verify max retries is configured correctly."""
        # Default should be 3
        self.assertEqual(MAX_RETRIES, 3)


class TestWorkerIdGeneration(unittest.TestCase):
    """Test persistent worker ID generation."""
    
    def test_worker_id_is_consistent(self):
        """Worker ID should be consistent across imports."""
        from config import WORKER_ID, _generate_worker_id
        
        # Generate multiple times
        id1 = _generate_worker_id()
        id2 = _generate_worker_id()
        
        # Should be the same (deterministic based on machine)
        self.assertEqual(id1, id2)
        
        # Should be 12 characters (hex)
        self.assertEqual(len(id1), 12)
    
    def test_worker_id_is_hex(self):
        """Worker ID should be valid hex string."""
        from config import _generate_worker_id
        
        worker_id = _generate_worker_id()
        # Should not raise
        int(worker_id, 16)


class TestIngestorTagGeneration(unittest.TestCase):
    """Test tag generation from file paths."""
    
    def test_get_tags_simple_path(self):
        """Should extract tags from simple path."""
        from ingestor import get_tags
        
        tags = get_tags("books/fantasy/dragon.pdf")
        self.assertEqual(tags, ["books", "fantasy", "dragon"])
    
    def test_get_tags_nested_path(self):
        """Should handle deeply nested paths."""
        from ingestor import get_tags
        
        tags = get_tags("a/b/c/d/file.pdf")
        self.assertEqual(tags, ["a", "b", "c", "d", "file"])
    
    def test_get_tags_removes_extension(self):
        """Should remove .pdf extension from filename."""
        from ingestor import get_tags
        
        tags = get_tags("test.pdf")
        self.assertEqual(tags, ["test"])


class TestLfsPointerParsing(unittest.TestCase):
    """Test Git LFS pointer file parsing."""
    
    def test_parse_lfs_pointer(self):
        """Should extract SHA256 from LFS pointer."""
        from ingestor import get_lfs_hash
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.pdf', delete=False) as f:
            f.write("version https://git-lfs.github.com/spec/v1\n")
            f.write("oid sha256:abc123def456abc123def456abc123def456abc123def456abc123def456abcd\n")
            f.write("size 12345\n")
            f.flush()
            
            result = get_lfs_hash(f.name)
            os.unlink(f.name)
        
        self.assertEqual(result, "abc123def456abc123def456abc123def456abc123def456abc123def456abcd")
    
    def test_parse_non_lfs_file(self):
        """Should return None for non-LFS files."""
        from ingestor import get_lfs_hash
        
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.pdf', delete=False) as f:
            f.write(b"%PDF-1.4 regular pdf content")
            f.flush()
            
            result = get_lfs_hash(f.name)
            os.unlink(f.name)
        
        self.assertIsNone(result)


class TestSha256Computation(unittest.TestCase):
    """Test SHA256 hash computation."""
    
    def test_compute_sha256(self):
        """Should compute correct SHA256 hash."""
        from ingestor import compute_sha256
        
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            f.write(b"test content")
            f.flush()
            
            result = compute_sha256(f.name)
            os.unlink(f.name)
        
        # Known hash for "test content"
        expected = "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72"
        self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()
