"""
Consolidated S3 Client for BlobForge.
Single source of truth for all S3 operations.
"""
import os
import json
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from config import S3_BUCKET, S3_PREFIX_RAW, S3_PREFIX_TODO, S3_PREFIX_PROCESSING, S3_PREFIX_DONE, S3_PREFIX_FAILED, S3_PREFIX_DEAD, S3_PREFIX_REGISTRY


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
            self.s3 = boto3.client('s3')
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
        Uses conditional write (If-None-Match: *) for atomicity.
        
        Returns True if lock acquired, False if already locked.
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
    
    def update_manifest(self, new_entries: List[Dict[str, Any]], max_retries: int = 5) -> bool:
        """
        Update manifest with optimistic locking (If-Match).
        
        Args:
            new_entries: List of entry dicts, each with:
                - hash: SHA256 hash
                - path: Original file path (relative)
                - size: File size in bytes
                - tags: List of tags
                - source: Source directory path
            max_retries: Number of retries on conflict
        
        Returns:
            True if successful, False on persistent conflict
        """
        if self.dry_run:
            print(f"[DRY-RUN] Would update manifest with {len(new_entries)} entries")
            return True
        
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
                    # Conflict - retry
                    print(f"Manifest conflict (attempt {attempt + 1}/{max_retries}), retrying...")
                    time.sleep(0.1 * (attempt + 1))  # Backoff
                    continue
                raise
        
        print(f"Failed to update manifest after {max_retries} attempts")
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
