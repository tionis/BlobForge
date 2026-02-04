"""
Consolidated S3 Client for BlobForge.
Single source of truth for all S3 operations.
"""
import os
import json
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from .config import (
    S3_BUCKET, S3_REGION, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_ENDPOINT_URL,
    S3_PREFIX_RAW, S3_PREFIX_TODO, S3_PREFIX_PROCESSING, S3_PREFIX_DONE, 
    S3_PREFIX_FAILED, S3_PREFIX_DEAD, S3_PREFIX_REGISTRY, S3_PREFIX_WORKERS,
    WORKER_ID, get_worker_metadata, get_s3_supports_conditional_writes,
    get_stale_timeout_minutes
)


class S3Client:
    """
    Unified S3 client for all BlobForge components.
    Supports dry-run mode and mock mode (when boto3 is unavailable).
    """
    
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.mock = False
        self.s3 = None
        self.ClientError = Exception  # Default fallback
        
        try:
            import boto3
            import botocore.exceptions
            
            # Build client kwargs using BLOBFORGE_S3_* env vars
            kwargs = {
                "region_name": S3_REGION,
            }
            if S3_ACCESS_KEY_ID:
                kwargs["aws_access_key_id"] = S3_ACCESS_KEY_ID
            if S3_SECRET_ACCESS_KEY:
                kwargs["aws_secret_access_key"] = S3_SECRET_ACCESS_KEY
            if S3_ENDPOINT_URL:
                kwargs["endpoint_url"] = S3_ENDPOINT_URL
            
            # Warn if no explicit credentials and using custom endpoint
            if S3_ENDPOINT_URL and not (S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY):
                print(f"Warning: BLOBFORGE_S3_ENDPOINT_URL is set but credentials are missing.")
                print(f"  Set BLOBFORGE_S3_ACCESS_KEY_ID and BLOBFORGE_S3_SECRET_ACCESS_KEY")
                print(f"  Boto3 will try default credential chain (may fail for custom endpoints)")
            
            self.s3 = boto3.client('s3', **kwargs)
            self.ClientError = botocore.exceptions.ClientError
        except ImportError:
            print("Warning: boto3 not found. Running in MOCK mode.")
            self.mock = True

    # -------------------------------------------------------------------------
    # Basic Operations
    # -------------------------------------------------------------------------
    
    def exists(self, key: str) -> bool:
        """Check if an object exists in S3."""
        if self.mock:
            return False
        try:
            self.s3.head_object(Bucket=S3_BUCKET, Key=key)
            return True
        except self.ClientError:
            return False
        except Exception:
            return False

    def download_file(self, key: str, local_path: str) -> None:
        """Download a file from S3 to local path."""
        if self.mock:
            with open(local_path, "wb") as f:
                f.write(b"%PDF-1.4 mock content")
            return
        self.s3.download_file(S3_BUCKET, key, local_path)

    def upload_file(self, local_path: str, key: str, metadata: Optional[Dict[str, str]] = None) -> None:
        """Upload a local file to S3 with optional metadata."""
        if self.dry_run or self.mock:
            meta_str = json.dumps(metadata) if metadata else "{}"
            print(f"[DRY-RUN/MOCK] Uploading {local_path} -> s3://{S3_BUCKET}/{key} (Meta: {meta_str})")
            return
        
        extra_args = {}
        if metadata:
            extra_args['Metadata'] = metadata
        
        self.s3.upload_file(local_path, S3_BUCKET, key, ExtraArgs=extra_args if extra_args else None)

    def delete_object(self, key: str) -> None:
        """Delete an object from S3."""
        if self.dry_run or self.mock:
            print(f"[DRY-RUN/MOCK] Deleting {key}")
            return
        self.s3.delete_object(Bucket=S3_BUCKET, Key=key)

    def put_object(self, key: str, body: str, if_none_match: bool = False) -> None:
        """
        Write an object to S3.
        
        Args:
            key: S3 object key
            body: Content to write
            if_none_match: If True, only write if object doesn't exist (atomic lock)
        """
        if self.dry_run or self.mock:
            print(f"[DRY-RUN/MOCK] Putting {key}")
            if if_none_match and self.mock:
                import random
                if random.random() < 0.1:
                    raise Exception("Mock Precondition Failed")
            return
        
        kwargs = {'Bucket': S3_BUCKET, 'Key': key, 'Body': body}
        if if_none_match:
            kwargs['IfNoneMatch'] = '*'
        
        self.s3.put_object(**kwargs)

    def get_object(self, key: str) -> Optional[str]:
        """Read object content as string."""
        if self.mock:
            return None
        try:
            response = self.s3.get_object(Bucket=S3_BUCKET, Key=key)
            return response['Body'].read().decode('utf-8')
        except Exception:
            return None

    def get_object_json(self, key: str) -> Optional[Dict[str, Any]]:
        """Read object content as JSON."""
        content = self.get_object(key)
        if content:
            try:
                return json.loads(content)
            except json.JSONDecodeError:
                return None
        return None

    def get_object_metadata(self, key: str) -> Dict[str, str]:
        """Get S3 object metadata (x-amz-meta-* headers)."""
        if self.mock:
            return {"original-name": "mock.pdf", "tags": '["mock"]', "size": "100"}
        try:
            response = self.s3.head_object(Bucket=S3_BUCKET, Key=key)
            return response.get('Metadata', {})
        except Exception as e:
            print(f"Error getting metadata for {key}: {e}")
            return {}

    def copy_object(self, src_key: str, dest_key: str) -> None:
        """Copy an object within the same bucket."""
        if self.dry_run or self.mock:
            print(f"[DRY-RUN/MOCK] Copying {src_key} -> {dest_key}")
            return
        self.s3.copy_object(
            Bucket=S3_BUCKET,
            CopySource={'Bucket': S3_BUCKET, 'Key': src_key},
            Key=dest_key
        )

    # -------------------------------------------------------------------------
    # Listing Operations
    # -------------------------------------------------------------------------
    
    def list_objects(self, prefix: str, max_keys: int = 1000) -> List[Dict[str, Any]]:
        """
        List objects under a prefix.
        Returns list of dicts with 'Key', 'LastModified', etc.
        """
        if self.mock:
            return []
        
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            objects = []
            for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix, PaginationConfig={'MaxItems': max_keys}):
                if 'Contents' in page:
                    objects.extend(page['Contents'])
            return objects
        except Exception as e:
            print(f"Error listing {prefix}: {e}")
            return []

    def list_keys(self, prefix: str, max_keys: int = 10000) -> List[str]:
        """List object keys under a prefix."""
        objects = self.list_objects(prefix, max_keys)
        return [obj['Key'] for obj in objects]

    def count_prefix(self, prefix: str) -> int:
        """Count objects under a prefix."""
        if self.mock:
            return 0
        
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            count = 0
            for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
                if 'Contents' in page:
                    count += len(page['Contents'])
            return count
        except Exception:
            return 0

    def list_todo(self, priority: str, prefix_filter: str = "") -> List[str]:
        """
        List job hashes in a todo queue tier.
        
        Args:
            priority: Priority tier (e.g., "1_highest")
            prefix_filter: Optional prefix filter for sharding (e.g., "ab")
        """
        full_prefix = f"{S3_PREFIX_TODO}/{priority}/{prefix_filter}"
        
        if self.mock:
            mock_hash = f"{prefix_filter}mock_hash_123" if prefix_filter else "mock_hash_123"
            return [mock_hash]
        
        try:
            response = self.s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=full_prefix, MaxKeys=20)
            if 'Contents' not in response:
                return []
            return [obj['Key'].split('/')[-1] for obj in response['Contents']]
        except Exception as e:
            print(f"Error listing todo: {e}")
            return []

    def list_todo_batch(self, priority: str, max_keys: int = 50) -> List[str]:
        """
        List job hashes in a todo queue tier (batch/broad scan).
        
        Optimized for finding any available jobs quickly without shard filtering.
        Used by workers for efficient job discovery.
        
        Args:
            priority: Priority tier (e.g., "1_critical")
            max_keys: Maximum number of jobs to return (default 50)
        
        Returns:
            List of job hashes available in this priority queue
        """
        full_prefix = f"{S3_PREFIX_TODO}/{priority}/"
        
        if self.mock:
            return ["mock_hash_123"]
        
        try:
            response = self.s3.list_objects_v2(
                Bucket=S3_BUCKET, 
                Prefix=full_prefix, 
                MaxKeys=max_keys
            )
            if 'Contents' not in response:
                return []
            return [obj['Key'].split('/')[-1] for obj in response['Contents']]
        except Exception as e:
            print(f"Error listing todo batch: {e}")
            return []

    def list_processing(self) -> List[Dict[str, Any]]:
        """List all jobs currently being processed."""
        return self.list_objects(S3_PREFIX_PROCESSING + "/")

    def list_failed(self) -> List[Dict[str, Any]]:
        """List all failed jobs."""
        return self.list_objects(S3_PREFIX_FAILED + "/")

    def list_dead(self) -> List[Dict[str, Any]]:
        """List all dead-letter jobs."""
        return self.list_objects(S3_PREFIX_DEAD + "/")

    # -------------------------------------------------------------------------
    # Queue Operations (Higher-level)
    # -------------------------------------------------------------------------
    
    def create_todo_marker(self, priority: str, job_hash: str) -> None:
        """Create a todo marker for a job."""
        key = f"{S3_PREFIX_TODO}/{priority}/{job_hash}"
        self.put_object(key, "")

    def job_exists_anywhere(self, job_hash: str, priorities: List[str]) -> Dict[str, bool]:
        """
        Check if a job exists in any queue state.
        Returns dict with keys: 'todo', 'processing', 'done', 'failed', 'dead'
        """
        result = {
            'todo': False,
            'processing': False,
            'done': False,
            'failed': False,
            'dead': False
        }
        
        # Check done first (most common skip case)
        if self.exists(f"{S3_PREFIX_DONE}/{job_hash}.zip"):
            result['done'] = True
            return result
        
        # Check processing
        if self.exists(f"{S3_PREFIX_PROCESSING}/{job_hash}"):
            result['processing'] = True
        
        # Check failed
        if self.exists(f"{S3_PREFIX_FAILED}/{job_hash}"):
            result['failed'] = True
        
        # Check dead
        if self.exists(f"{S3_PREFIX_DEAD}/{job_hash}"):
            result['dead'] = True
        
        # Check todo queues
        for p in priorities:
            if self.exists(f"{S3_PREFIX_TODO}/{p}/{job_hash}"):
                result['todo'] = True
                break
        
        return result

    # -------------------------------------------------------------------------
    # Lock Operations (for workers)
    # -------------------------------------------------------------------------
    
    def acquire_lock(self, job_hash: str, worker_id: str, priority: str) -> bool:
        """
        Attempt to acquire a processing lock for a job.
        
        Uses conditional write (If-None-Match: *) if supported, otherwise falls
        back to timestamp-based soft locking for S3 providers without conditional
        write support.
        
        Returns True if lock acquired, False if already locked.
        """
        if get_s3_supports_conditional_writes():
            return self._acquire_lock_conditional(job_hash, worker_id, priority)
        else:
            return self._acquire_lock_soft(job_hash, worker_id, priority)
    
    def _acquire_lock_conditional(self, job_hash: str, worker_id: str, priority: str) -> bool:
        """
        Acquire lock using conditional write (If-None-Match: *).
        Only works on S3 providers that support conditional writes.
        """
        lock_key = f"{S3_PREFIX_PROCESSING}/{job_hash}"
        timestamp = int(time.time() * 1000)
        
        payload = json.dumps({
            "worker": worker_id,
            "started": timestamp,
            "last_heartbeat": timestamp,
            "priority": priority,
            "retries": 0  # Will be updated from todo marker if available
        })
        
        try:
            self.put_object(lock_key, payload, if_none_match=True)
            return True
        except self.ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code in ['PreconditionFailed', '412']:
                return False
            raise
        except Exception as e:
            if "Precondition Failed" in str(e) or "412" in str(e):
                return False
            raise
    
    def _acquire_lock_soft(self, job_hash: str, worker_id: str, priority: str) -> bool:
        """
        Acquire lock using timestamp-based soft locking.
        
        This works on S3 providers that don't support conditional writes.
        
        Algorithm:
        1. Check if lock already exists with a recent heartbeat -> fail
        2. Write our lock claim with timestamp + nonce
        3. Wait briefly (200ms) to allow concurrent writers to settle
        4. Re-read the lock file
        5. If our nonce is still there, we own the lock
        6. If different nonce, compare timestamps - earliest wins
        7. If we didn't win, delete our claim (optional cleanup) and fail
        
        This provides probabilistic mutual exclusion - in the rare case of
        exact timestamp collision, multiple workers may claim the same job,
        but the heartbeat mechanism will eventually resolve it.
        """
        import uuid
        
        lock_key = f"{S3_PREFIX_PROCESSING}/{job_hash}"
        timestamp = int(time.time() * 1000)
        nonce = uuid.uuid4().hex[:16]
        
        # Step 1: Check if lock already exists with recent heartbeat
        existing = self.get_object_json(lock_key)
        if existing:
            # Lock exists - check if it's stale
            last_hb = existing.get('last_heartbeat', 0)
            stale_ms = get_stale_timeout_minutes() * 60 * 1000
            if timestamp - last_hb < stale_ms:
                # Lock is fresh, someone else has it
                return False
            # Lock is stale - we can try to take over
        
        # Step 2: Write our lock claim
        payload = json.dumps({
            "worker": worker_id,
            "started": timestamp,
            "last_heartbeat": timestamp,
            "priority": priority,
            "retries": 0,
            "nonce": nonce,  # Unique identifier for this claim
            "lock_version": 2,  # Indicates soft-lock protocol
        })
        
        try:
            self.put_object(lock_key, payload)
        except Exception as e:
            # Write failed - can't acquire lock
            return False
        
        # Step 3: Wait for concurrent writers to settle
        # 200ms is enough for most S3 providers to achieve consistency
        time.sleep(0.2)
        
        # Step 4: Re-read the lock
        current = self.get_object_json(lock_key)
        if not current:
            # Lock disappeared (deleted by another process?) - fail
            return False
        
        # Step 5: Check if our nonce is still there
        current_nonce = current.get('nonce')
        if current_nonce == nonce:
            # We still own it!
            return True
        
        # Step 6: Different nonce - compare timestamps
        current_timestamp = current.get('started', 0)
        
        if timestamp < current_timestamp:
            # We were earlier - try to reclaim
            # This handles the case where our write was overwritten but we were first
            try:
                self.put_object(lock_key, payload)
                time.sleep(0.1)  # Brief wait
                recheck = self.get_object_json(lock_key)
                if recheck and recheck.get('nonce') == nonce:
                    return True
            except Exception:
                pass
        
        # Step 7: We lost the race
        return False

    def update_heartbeat(self, job_hash: str, worker_id: str, progress: Optional[Dict] = None) -> bool:
        """
        Update heartbeat timestamp for a processing job.
        Optionally include progress information.
        
        Returns True if successful, False if lock is lost/modified.
        """
        lock_key = f"{S3_PREFIX_PROCESSING}/{job_hash}"
        
        # Read current lock
        current = self.get_object_json(lock_key)
        if not current:
            return False
        
        # Verify we still own the lock
        if current.get('worker') != worker_id:
            return False
        
        # Update heartbeat
        current['last_heartbeat'] = int(time.time() * 1000)
        if progress:
            current['progress'] = progress
        
        try:
            self.put_object(lock_key, json.dumps(current))
            return True
        except Exception:
            return False

    def release_lock(self, job_hash: str) -> None:
        """Release a processing lock (delete lock file)."""
        self.delete_object(f"{S3_PREFIX_PROCESSING}/{job_hash}")

    def get_lock_info(self, job_hash: str) -> Optional[Dict[str, Any]]:
        """Get information about a processing lock."""
        return self.get_object_json(f"{S3_PREFIX_PROCESSING}/{job_hash}")

    # -------------------------------------------------------------------------
    # Failure Handling
    # -------------------------------------------------------------------------
    
    def mark_failed(self, job_hash: str, error: str, worker_id: str, retry_count: int = 0) -> None:
        """
        Mark a job as failed.
        Includes detailed error information for debugging.
        """
        payload = json.dumps({
            "error": str(error),
            "worker": worker_id,
            "failed_at": datetime.now().isoformat() + "Z",
            "timestamp": int(time.time() * 1000),
            "retries": retry_count
        })
        
        key = f"{S3_PREFIX_FAILED}/{job_hash}"
        self.put_object(key, payload)
        
        # Release processing lock
        self.release_lock(job_hash)

    def mark_dead(self, job_hash: str, error: str, total_retries: int) -> None:
        """
        Move a job to the dead-letter queue (exceeded max retries).
        """
        payload = json.dumps({
            "error": str(error),
            "moved_to_dead_at": datetime.now().isoformat() + "Z",
            "total_retries": total_retries,
            "reason": "exceeded_max_retries"
        })
        
        key = f"{S3_PREFIX_DEAD}/{job_hash}"
        self.put_object(key, payload)
        
        # Clean up failed marker if exists
        self.delete_object(f"{S3_PREFIX_FAILED}/{job_hash}")

    # -------------------------------------------------------------------------
    # Job Logs
    # -------------------------------------------------------------------------
    
    def save_job_log(self, job_hash: str, log_content: str, log_type: str = "conversion") -> bool:
        """
        Save job log to S3.
        
        Args:
            job_hash: The job hash
            log_content: Log content to save
            log_type: Type of log (conversion, error, etc.)
            
        Returns:
            True if successful
        """
        if self.dry_run or self.mock:
            print(f"[DRY-RUN/MOCK] Would save {log_type} log for {job_hash}")
            return True
        
        key = f"{S3_PREFIX_REGISTRY}/logs/{job_hash}/{log_type}.log"
        try:
            self.s3.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=log_content,
                ContentType="text/plain"
            )
            return True
        except Exception as e:
            print(f"Failed to save job log: {e}")
            return False
    
    def get_job_log(self, job_hash: str, log_type: str = "conversion") -> Optional[str]:
        """
        Get job log from S3.
        
        Args:
            job_hash: The job hash
            log_type: Type of log to retrieve
            
        Returns:
            Log content or None if not found
        """
        key = f"{S3_PREFIX_REGISTRY}/logs/{job_hash}/{log_type}.log"
        return self.get_object(key)
    
    def list_job_logs(self, job_hash: str) -> List[str]:
        """
        List all logs for a job.
        
        Returns:
            List of log type names
        """
        prefix = f"{S3_PREFIX_REGISTRY}/logs/{job_hash}/"
        keys = self.list_keys(prefix)
        return [k.split('/')[-1].replace('.log', '') for k in keys if k.endswith('.log')]
    
    def save_job_error_detail(self, job_hash: str, error: str, traceback: str = None, 
                              context: Dict[str, Any] = None) -> bool:
        """
        Save detailed error information for a failed job.
        
        Args:
            job_hash: The job hash
            error: Error message
            traceback: Full traceback string
            context: Additional context (stage, file info, etc.)
        """
        error_data = {
            "error": error,
            "timestamp": datetime.now().isoformat() + "Z",
            "traceback": traceback,
            "context": context or {}
        }
        
        if self.dry_run or self.mock:
            print(f"[DRY-RUN/MOCK] Would save error detail for {job_hash}")
            return True
        
        key = f"{S3_PREFIX_REGISTRY}/logs/{job_hash}/error.json"
        try:
            self.s3.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=json.dumps(error_data, indent=2),
                ContentType="application/json"
            )
            return True
        except Exception as e:
            print(f"Failed to save error detail: {e}")
            return False
    
    def get_job_error_detail(self, job_hash: str) -> Optional[Dict[str, Any]]:
        """Get detailed error information for a job."""
        key = f"{S3_PREFIX_REGISTRY}/logs/{job_hash}/error.json"
        return self.get_object_json(key)

    def get_retry_count(self, job_hash: str) -> int:
        """
        Get the retry count for a job from the failed queue.
        Returns 0 if not found.
        """
        data = self.get_object_json(f"{S3_PREFIX_FAILED}/{job_hash}")
        if data:
            return data.get('retries', 0)
        return 0

    def move_to_todo(self, job_hash: str, priority: str, increment_retry: bool = False) -> int:
        """
        Move a job (back) to the todo queue.
        
        Args:
            job_hash: The job hash
            priority: Target priority tier
            increment_retry: If True, increment and track retry count
        
        Returns:
            Current retry count
        """
        retry_count = 0
        
        if increment_retry:
            # Check if there's existing failure info
            failed_data = self.get_object_json(f"{S3_PREFIX_FAILED}/{job_hash}")
            if failed_data:
                retry_count = failed_data.get('retries', 0) + 1
            
            # Store retry count in the todo marker
            marker_content = json.dumps({"retries": retry_count, "queued_at": int(time.time() * 1000)})
        else:
            marker_content = ""
        
        key = f"{S3_PREFIX_TODO}/{priority}/{job_hash}"
        self.put_object(key, marker_content)
        
        return retry_count

    # -------------------------------------------------------------------------
    # Scan Operations (for status/janitor)
    # -------------------------------------------------------------------------
    
    def scan_processing_detailed(self) -> List[Dict[str, Any]]:
        """
        Scan processing queue with detailed information.
        Returns list of job info dicts.
        """
        if self.mock:
            now = datetime.now()
            return [
                {"hash": "mock_job_1", "worker": "worker-a", "age": timedelta(minutes=5), "stale": False},
                {"hash": "mock_job_2", "worker": "worker-b", "age": timedelta(hours=3), "stale": True}
            ]
        
        jobs = []
        now = datetime.now()
        
        for obj in self.list_processing():
            key = obj['Key']
            if key.endswith("/"):
                continue
            
            job_hash = key.split('/')[-1]
            data = self.get_object_json(key)
            
            if data:
                # Use internal heartbeat timestamp
                heartbeat_ts = data.get('last_heartbeat', data.get('started', 0))
                heartbeat_dt = datetime.fromtimestamp(heartbeat_ts / 1000.0)
                age = now - heartbeat_dt
                
                jobs.append({
                    "hash": job_hash,
                    "worker": data.get('worker', 'unknown'),
                    "priority": data.get('priority', 'unknown'),
                    "started": data.get('started'),
                    "last_heartbeat": heartbeat_ts,
                    "age": age,
                    "stale": age > timedelta(minutes=15),  # Shorter timeout with heartbeat
                    "progress": data.get('progress')
                })
            else:
                # Fallback to S3 LastModified
                last_mod = obj.get('LastModified')
                if last_mod:
                    last_mod = last_mod.replace(tzinfo=None)
                    age = now - last_mod
                else:
                    age = timedelta(0)
                
                jobs.append({
                    "hash": job_hash,
                    "worker": "unknown",
                    "priority": "unknown",
                    "age": age,
                    "stale": age > timedelta(minutes=15)
                })
        
        return jobs

    # -------------------------------------------------------------------------
    # Manifest Operations
    # -------------------------------------------------------------------------
    
    def get_manifest(self) -> Dict[str, Any]:
        """
        Load the manifest from S3.
        
        Returns:
            Dict with structure:
            {
                "version": 1,
                "updated_at": "...",
                "entries": {
                    "<hash>": {
                        "paths": ["path/to/file.pdf", ...],
                        "size": 12345,
                        "tags": ["tag1", "tag2"],
                        "ingested_at": "...",
                        "source": "library/rpg-books"
                    },
                    ...
                }
            }
        """
        key = f"{S3_PREFIX_REGISTRY}/manifest.json"
        data = self.get_object_json(key)
        if data:
            return data
        return {"version": 1, "updated_at": None, "entries": {}}
    
    def get_manifest_with_etag(self) -> tuple:
        """
        Load manifest with ETag for optimistic locking.
        
        Returns:
            (manifest_dict, etag) tuple. etag is None if manifest doesn't exist.
        """
        key = f"{S3_PREFIX_REGISTRY}/manifest.json"
        
        if self.mock:
            return {"version": 1, "updated_at": None, "entries": {}}, None
        
        try:
            response = self.s3.get_object(Bucket=S3_BUCKET, Key=key)
            content = response['Body'].read().decode('utf-8')
            etag = response['ETag']
            return json.loads(content), etag
        except self.ClientError as e:
            if e.response.get('Error', {}).get('Code') == 'NoSuchKey':
                return {"version": 1, "updated_at": None, "entries": {}}, None
            raise
        except Exception:
            return {"version": 1, "updated_at": None, "entries": {}}, None
    
    def update_manifest(self, new_entries: List[Dict[str, Any]], max_retries: int = 10) -> bool:
        """
        Update manifest with optimistic locking (If-Match).
        
        Falls back to force_update_manifest if the S3 provider doesn't support
        conditional writes (configured via remote config s3_supports_conditional_writes).
        
        Args:
            new_entries: List of entry dicts, each with:
                - hash: SHA256 hash
                - path: Original file path (relative)
                - size: File size in bytes
                - tags: List of tags
                - source: Source directory path
            max_retries: Number of retries on conflict (with exponential backoff)
        
        Returns:
            True if successful, False on persistent conflict
        """
        if self.dry_run:
            print(f"[DRY-RUN] Would update manifest with {len(new_entries)} entries")
            return True
        
        if not new_entries:
            return True
        
        # Check if provider supports conditional writes
        if not get_s3_supports_conditional_writes():
            return self.force_update_manifest(new_entries)
        
        key = f"{S3_PREFIX_REGISTRY}/manifest.json"
        
        for attempt in range(max_retries):
            # 1. Read current manifest + ETag
            manifest, etag = self.get_manifest_with_etag()
            
            # 2. Merge new entries
            now = datetime.now().isoformat() + "Z"
            for entry in new_entries:
                file_hash = entry['hash']
                if file_hash in manifest['entries']:
                    # Hash exists - add path if new
                    existing = manifest['entries'][file_hash]
                    if entry['path'] not in existing.get('paths', []):
                        existing.setdefault('paths', []).append(entry['path'])
                    # Update tags (merge)
                    existing_tags = set(existing.get('tags', []))
                    existing_tags.update(entry.get('tags', []))
                    existing['tags'] = list(existing_tags)
                else:
                    # New hash
                    manifest['entries'][file_hash] = {
                        'paths': [entry['path']],
                        'size': entry.get('size', 0),
                        'tags': entry.get('tags', []),
                        'ingested_at': now,
                        'source': entry.get('source', '')
                    }
            
            manifest['updated_at'] = now
            
            # 3. Write with conditional (If-Match or If-None-Match)
            try:
                if self.mock:
                    return True
                
                kwargs = {
                    'Bucket': S3_BUCKET,
                    'Key': key,
                    'Body': json.dumps(manifest, indent=2),
                    'ContentType': 'application/json'
                }
                
                if etag:
                    kwargs['IfMatch'] = etag
                else:
                    kwargs['IfNoneMatch'] = '*'
                
                self.s3.put_object(**kwargs)
                return True
                
            except self.ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', '')
                if error_code in ['PreconditionFailed', '412']:
                    # Conflict - exponential backoff with jitter
                    import random
                    delay = min(0.1 * (2 ** attempt) + random.uniform(0, 0.1), 5.0)
                    print(f"Manifest conflict (attempt {attempt + 1}/{max_retries}), retrying in {delay:.2f}s...")
                    time.sleep(delay)
                    continue
                raise
        
        print(f"Failed to update manifest after {max_retries} attempts")
        return False
    
    def force_update_manifest(self, new_entries: List[Dict[str, Any]]) -> bool:
        """
        Force update manifest without optimistic locking.
        
        WARNING: This can cause data loss if concurrent updates are happening.
        Use only for repair operations when no other processes are running.
        
        Returns:
            True if successful
        """
        if self.dry_run:
            print(f"[DRY-RUN] Would force update manifest with {len(new_entries)} entries")
            return True
        
        if not new_entries:
            return True
        
        key = f"{S3_PREFIX_REGISTRY}/manifest.json"
        
        # Read current manifest (no ETag needed)
        manifest = self.get_manifest()
        
        # Merge new entries
        now = datetime.now().isoformat() + "Z"
        for entry in new_entries:
            file_hash = entry['hash']
            if file_hash in manifest['entries']:
                existing = manifest['entries'][file_hash]
                if entry['path'] not in existing.get('paths', []):
                    existing.setdefault('paths', []).append(entry['path'])
                existing_tags = set(existing.get('tags', []))
                existing_tags.update(entry.get('tags', []))
                existing['tags'] = list(existing_tags)
            else:
                manifest['entries'][file_hash] = {
                    'paths': [entry['path']],
                    'size': entry.get('size', 0),
                    'tags': entry.get('tags', []),
                    'ingested_at': now,
                    'source': entry.get('source', '')
                }
        
        manifest['updated_at'] = now
        
        # Write without conditions
        try:
            if self.mock:
                return True
            
            self.s3.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=json.dumps(manifest, indent=2),
                ContentType='application/json'
            )
            return True
        except Exception as e:
            print(f"Failed to force update manifest: {e}")
            return False
    
    def rebuild_manifest_from_raw(self) -> Dict[str, Any]:
        """
        Rebuild manifest by scanning all files in store/raw/.
        
        This reads metadata from each raw PDF and reconstructs the manifest.
        Use this to repair a corrupted or out-of-sync manifest.
        
        Returns:
            The rebuilt manifest dict
        """
        print("Scanning store/raw/ to rebuild manifest...")
        
        manifest = {
            "version": 1,
            "updated_at": datetime.now().isoformat() + "Z",
            "entries": {}
        }
        
        raw_files = self.list_objects(f"{S3_PREFIX_RAW}/")
        total = len(raw_files)
        
        for i, obj in enumerate(raw_files):
            key = obj['Key']
            if not key.endswith('.pdf'):
                continue
            
            # Extract hash from key: store/raw/<hash>.pdf
            file_hash = key.split('/')[-1].replace('.pdf', '')
            
            # Get metadata
            metadata = self.get_object_metadata(key)
            
            original_name = metadata.get('original-name', f'{file_hash[:8]}.pdf')
            tags_str = metadata.get('tags', '[]')
            try:
                tags = json.loads(tags_str)
            except:
                tags = []
            
            size_str = metadata.get('size', '0')
            try:
                size = int(size_str)
            except:
                size = obj.get('Size', 0)
            
            manifest['entries'][file_hash] = {
                'paths': [original_name],
                'size': size,
                'tags': tags,
                'ingested_at': obj.get('LastModified', datetime.now()).isoformat() + "Z" if isinstance(obj.get('LastModified'), datetime) else str(obj.get('LastModified', '')),
                'source': 'rebuilt'
            }
            
            if (i + 1) % 100 == 0 or i + 1 == total:
                print(f"  Processed {i + 1}/{total} files...")
        
        print(f"Rebuilt manifest with {len(manifest['entries'])} entries")
        return manifest
    
    def save_manifest(self, manifest: Dict[str, Any]) -> bool:
        """
        Save a manifest dict to S3 (unconditional write).
        
        Use with rebuild_manifest_from_raw() to repair the manifest.
        """
        if self.dry_run:
            print("[DRY-RUN] Would save manifest")
            return True
        
        key = f"{S3_PREFIX_REGISTRY}/manifest.json"
        
        try:
            self.s3.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=json.dumps(manifest, indent=2),
                ContentType='application/json'
            )
            return True
        except Exception as e:
            print(f"Failed to save manifest: {e}")
            return False
    
    def lookup_by_path(self, path: str) -> Optional[str]:
        """
        Look up a file hash by its path.
        
        Returns:
            SHA256 hash if found, None otherwise
        """
        manifest = self.get_manifest()
        for file_hash, entry in manifest.get('entries', {}).items():
            if path in entry.get('paths', []):
                return file_hash
        return None
    
    def lookup_by_hash(self, file_hash: str) -> Optional[Dict[str, Any]]:
        """
        Look up file info by hash.
        
        Returns:
            Entry dict if found, None otherwise
        """
        manifest = self.get_manifest()
        return manifest.get('entries', {}).get(file_hash)
    
    def search_manifest(self, query: str) -> List[Dict[str, Any]]:
        """
        Search manifest by filename or tag (case-insensitive).
        
        Returns:
            List of matching entries with hash included
        """
        manifest = self.get_manifest()
        query_lower = query.lower()
        results = []
        
        for file_hash, entry in manifest.get('entries', {}).items():
            # Check paths
            for path in entry.get('paths', []):
                if query_lower in path.lower():
                    results.append({**entry, 'hash': file_hash})
                    break
            else:
                # Check tags
                for tag in entry.get('tags', []):
                    if query_lower in tag.lower():
                        results.append({**entry, 'hash': file_hash})
                        break
        
        return results

    # -------------------------------------------------------------------------
    # Worker Registration
    # -------------------------------------------------------------------------
    
    def register_worker(self, extra_metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Register this worker in S3.
        
        Creates a worker metadata file at registry/workers/<worker_id>.json
        containing machine info, start time, etc.
        
        Args:
            extra_metadata: Additional metadata to include
            
        Returns:
            True if registration succeeded
        """
        key = f"{S3_PREFIX_WORKERS}/{WORKER_ID}.json"
        
        metadata = get_worker_metadata()
        if extra_metadata:
            metadata.update(extra_metadata)
        
        metadata["last_heartbeat"] = datetime.utcnow().isoformat() + "Z"
        metadata["status"] = "active"
        
        if self.dry_run or self.mock:
            print(f"[DRY-RUN/MOCK] Registering worker {WORKER_ID}")
            return True
        
        try:
            self.s3.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=json.dumps(metadata, indent=2),
                ContentType="application/json"
            )
            return True
        except Exception as e:
            print(f"Failed to register worker: {e}")
            return False
    
    def update_worker_heartbeat(self, current_job: Optional[str] = None,
                                 system_metrics: Optional[Dict[str, Any]] = None) -> bool:
        """
        Update worker heartbeat and optionally current job and system metrics.
        
        Args:
            current_job: Hash of job currently being processed
            system_metrics: System metrics (CPU, RAM, etc.)
            
        Returns:
            True if update succeeded
        """
        key = f"{S3_PREFIX_WORKERS}/{WORKER_ID}.json"
        
        if self.dry_run or self.mock:
            return True
        
        try:
            # Get existing metadata
            existing = self.get_object_json(key) or get_worker_metadata()
            existing["last_heartbeat"] = datetime.utcnow().isoformat() + "Z"
            existing["status"] = "processing" if current_job else "idle"
            if current_job:
                existing["current_job"] = current_job
            elif "current_job" in existing:
                del existing["current_job"]
            
            # Add system metrics if provided
            if system_metrics:
                existing["system"] = system_metrics
            
            self.s3.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=json.dumps(existing, indent=2),
                ContentType="application/json"
            )
            return True
        except Exception as e:
            print(f"Failed to update worker heartbeat: {e}")
            return False
    
    def deregister_worker(self) -> bool:
        """
        Mark this worker as inactive (on graceful shutdown).
        
        Returns:
            True if deregistration succeeded
        """
        key = f"{S3_PREFIX_WORKERS}/{WORKER_ID}.json"
        
        if self.dry_run or self.mock:
            print(f"[DRY-RUN/MOCK] Deregistering worker {WORKER_ID}")
            return True
        
        try:
            existing = self.get_object_json(key) or get_worker_metadata()
            existing["last_heartbeat"] = datetime.utcnow().isoformat() + "Z"
            existing["status"] = "stopped"
            existing["stopped_at"] = datetime.utcnow().isoformat() + "Z"
            if "current_job" in existing:
                del existing["current_job"]
            
            self.s3.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=json.dumps(existing, indent=2),
                ContentType="application/json"
            )
            return True
        except Exception as e:
            print(f"Failed to deregister worker: {e}")
            return False
    
    def update_worker_metrics(self, metrics: Dict[str, Any]) -> bool:
        """
        Update worker throughput metrics in S3.
        
        Args:
            metrics: Throughput metrics dict
            
        Returns:
            True if update succeeded
        """
        key = f"{S3_PREFIX_WORKERS}/{WORKER_ID}.json"
        
        if self.dry_run or self.mock:
            return True
        
        try:
            existing = self.get_object_json(key) or get_worker_metadata()
            existing["metrics"] = metrics
            existing["last_heartbeat"] = datetime.utcnow().isoformat() + "Z"
            
            self.s3.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=json.dumps(existing, indent=2),
                ContentType="application/json"
            )
            return True
        except Exception as e:
            print(f"Failed to update worker metrics: {e}")
            return False
    
    def list_workers(self) -> List[Dict[str, Any]]:
        """
        List all registered workers.
        
        Returns:
            List of worker metadata dicts
        """
        if self.mock:
            return []
        
        workers = []
        keys = self.list_keys(f"{S3_PREFIX_WORKERS}/")
        
        for key in keys:
            if key.endswith(".json"):
                data = self.get_object_json(key)
                if data:
                    workers.append(data)
        
        return workers
    
    def get_active_workers(self, stale_minutes: int = 15) -> List[Dict[str, Any]]:
        """
        Get workers that have sent a heartbeat within stale_minutes.
        
        Args:
            stale_minutes: Consider worker stale after this many minutes
            
        Returns:
            List of active worker metadata dicts
        """
        workers = self.list_workers()
        cutoff = datetime.utcnow() - timedelta(minutes=stale_minutes)
        
        active = []
        for w in workers:
            try:
                last_hb = datetime.fromisoformat(w.get("last_heartbeat", "").rstrip("Z"))
                if last_hb > cutoff and w.get("status") != "stopped":
                    active.append(w)
            except (ValueError, TypeError):
                pass
        
        return active
