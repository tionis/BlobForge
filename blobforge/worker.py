"""
BlobForge Worker - Distributed PDF processing agent.

Features:
- Atomic job locking with If-None-Match
- Periodic heartbeat updates
- Retry tracking and dead-letter queue support
- Improved sharding (2-char prefix = 256 shards)
- Race-condition-free job acquisition (todo marker kept until completion)
- Direct marker Python API (models stay loaded in memory)
- Rich progress tracking via tqdm interception
"""
import os
import sys
import time
import json
import shutil
import random
import argparse
import zipfile
import tempfile
import threading
import logging
from datetime import datetime
from typing import Optional, Any, Dict, Callable

# Optional psutil for system metrics
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

from .config import (
    S3_PREFIX_RAW, S3_PREFIX_TODO, S3_PREFIX_PROCESSING, S3_PREFIX_DONE,
    S3_PREFIX_FAILED, S3_PREFIX_DEAD, PRIORITIES, DEFAULT_PRIORITY, WORKER_ID,
    get_max_retries, get_heartbeat_interval, get_conversion_timeout
)
from .s3_client import S3Client

logger = logging.getLogger(__name__)

# Global progress callback for tqdm interception
_tqdm_progress_callback: Optional[Callable[[dict], None]] = None


def set_tqdm_progress_callback(callback: Optional[Callable[[dict], None]]):
    """Set a callback function to receive tqdm progress updates."""
    global _tqdm_progress_callback
    _tqdm_progress_callback = callback


def _install_tqdm_hook():
    """
    Install a hook to intercept tqdm progress updates.
    This patches tqdm.tqdm to call our callback on each update.
    """
    try:
        import tqdm
        import tqdm.auto
        
        # Store original tqdm class
        OriginalTqdm = tqdm.tqdm
        
        class ProgressTrackingTqdm(OriginalTqdm):
            """Custom tqdm that reports progress to our callback."""
            
            def __init__(self, *args, **kwargs):
                # Initialize our attributes BEFORE calling super().__init__
                # because super().__init__ may call refresh()
                self._last_callback_time = 0
                self._callback_interval = 2.0  # Update callback at most every 2 seconds
                super().__init__(*args, **kwargs)
            
            def update(self, n=1):
                result = super().update(n)
                self._report_progress()
                return result
            
            def refresh(self, *args, **kwargs):
                result = super().refresh(*args, **kwargs)
                self._report_progress()
                return result
            
            def _report_progress(self):
                """Report progress to callback if set."""
                global _tqdm_progress_callback
                if _tqdm_progress_callback is None:
                    return
                
                # Check if we have the attribute (safety check)
                if not hasattr(self, '_last_callback_time'):
                    return
                
                # Rate limit callbacks
                now = time.time()
                if now - self._last_callback_time < self._callback_interval:
                    return
                self._last_callback_time = now
                
                try:
                    # Extract progress info
                    progress_data = {
                        'tqdm_stage': str(self.desc) if self.desc else 'Processing',
                        'tqdm_current': self.n,
                        'tqdm_total': self.total,
                        'tqdm_percent': round(100 * self.n / self.total, 1) if self.total else 0,
                        'tqdm_rate': round(self.format_dict.get('rate', 0) or 0, 2),
                        'tqdm_elapsed': round(self.format_dict.get('elapsed', 0) or 0, 1),
                    }
                    # Add ETA if available
                    if self.total and self.n < self.total and progress_data['tqdm_rate'] > 0:
                        remaining = (self.total - self.n) / progress_data['tqdm_rate']
                        progress_data['tqdm_eta'] = round(remaining, 1)
                    
                    _tqdm_progress_callback(progress_data)
                except Exception as e:
                    # Don't let callback errors break tqdm
                    pass
        
        # Monkey-patch tqdm
        tqdm.tqdm = ProgressTrackingTqdm
        tqdm.auto.tqdm = ProgressTrackingTqdm
        
        # Also patch tqdm.std if it exists
        if hasattr(tqdm, 'std'):
            tqdm.std.tqdm = ProgressTrackingTqdm
        
        logger.debug("Installed tqdm progress hook")
        return True
    except ImportError:
        logger.debug("tqdm not available, progress tracking disabled")
        return False
    except Exception as e:
        logger.warning(f"Failed to install tqdm hook: {e}")
        return False


# Install the hook early, before marker imports tqdm
_tqdm_hook_installed = _install_tqdm_hook()


def get_pdf_page_count(pdf_path: str) -> Optional[int]:
    """Get page count from a PDF file."""
    try:
        # Try pypdf first (commonly available)
        try:
            from pypdf import PdfReader
            reader = PdfReader(pdf_path)
            return len(reader.pages)
        except ImportError:
            pass
        
        # Try PyPDF2 as fallback
        try:
            from PyPDF2 import PdfReader
            reader = PdfReader(pdf_path)
            return len(reader.pages)
        except ImportError:
            pass
        
        # Try pdfplumber (marker dependency)
        try:
            import pdfplumber
            with pdfplumber.open(pdf_path) as pdf:
                return len(pdf.pages)
        except ImportError:
            pass
        
        return None
    except Exception as e:
        logger.warning(f"Could not get page count: {e}")
        return None


def get_system_metrics() -> Dict[str, Any]:
    """Get current system metrics (CPU, RAM, disk)."""
    metrics = {}
    
    if HAS_PSUTIL:
        try:
            # CPU usage (non-blocking, uses previous sample)
            metrics['cpu_percent'] = psutil.cpu_percent(interval=None)
            
            # Memory usage
            mem = psutil.virtual_memory()
            metrics['memory_percent'] = mem.percent
            metrics['memory_used_gb'] = round(mem.used / (1024**3), 2)
            metrics['memory_available_gb'] = round(mem.available / (1024**3), 2)
            
            # Disk usage (root partition)
            try:
                disk = psutil.disk_usage('/')
                metrics['disk_percent'] = disk.percent
                metrics['disk_free_gb'] = round(disk.free / (1024**3), 2)
            except Exception:
                pass
            
            # Load average (Unix only)
            try:
                load1, load5, load15 = os.getloadavg()
                metrics['load_avg_1m'] = round(load1, 2)
                metrics['load_avg_5m'] = round(load5, 2)
            except (AttributeError, OSError):
                pass
                
        except Exception as e:
            metrics['error'] = str(e)
    else:
        metrics['psutil_available'] = False
    
    return metrics


class HeartbeatThread(threading.Thread):
    """Background thread that periodically updates the heartbeat for active jobs."""
    
    def __init__(self, s3_client: S3Client, worker_id: str):
        super().__init__(daemon=True)
        self.s3 = s3_client
        self.worker_id = worker_id
        self.current_job: Optional[str] = None
        self.progress: Optional[dict] = None
        self.job_start_time: Optional[float] = None
        self.original_filename: Optional[str] = None
        self.file_size: Optional[int] = None
        self.running = True
        self._lock = threading.Lock()
        self._tqdm_progress: Optional[dict] = None  # Latest tqdm progress
        
        # Initialize CPU percent measurement
        if HAS_PSUTIL:
            psutil.cpu_percent(interval=None)
    
    def set_job(self, job_hash: Optional[str], progress: Optional[dict] = None,
                original_filename: Optional[str] = None, file_size: Optional[int] = None):
        """Set the current job being processed."""
        with self._lock:
            self.current_job = job_hash
            self.progress = progress
            self.job_start_time = time.time() if job_hash else None
            self.original_filename = original_filename
            self.file_size = file_size
            self._tqdm_progress = None  # Reset tqdm progress for new job
    
    def update_progress(self, progress: dict):
        """Update progress information for current job."""
        with self._lock:
            if self.progress:
                self.progress.update(progress)
            else:
                self.progress = progress
    
    def update_tqdm_progress(self, tqdm_data: dict):
        """Update tqdm progress (called from tqdm hook)."""
        with self._lock:
            self._tqdm_progress = tqdm_data
    
    def stop(self):
        """Stop the heartbeat thread."""
        self.running = False
    
    def _build_progress_data(self) -> dict:
        """Build comprehensive progress data for heartbeat."""
        data = {}
        
        # Copy current progress
        if self.progress:
            data.update(self.progress)
        
        # Add timing info
        if self.job_start_time:
            elapsed = time.time() - self.job_start_time
            data['elapsed_seconds'] = round(elapsed, 1)
            data['elapsed_formatted'] = self._format_duration(elapsed)
        
        # Add file info
        if self.original_filename:
            data['original_filename'] = self.original_filename
        if self.file_size:
            data['file_size_bytes'] = self.file_size
            data['file_size_formatted'] = self._format_size(self.file_size)
        
        # Add tqdm progress (marker stages)
        if self._tqdm_progress:
            data['marker'] = self._tqdm_progress
        
        # Add system metrics
        data['system'] = get_system_metrics()
        
        return data
    
    @staticmethod
    def _format_duration(seconds: float) -> str:
        """Format seconds as human-readable duration."""
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            mins = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{mins}m {secs}s"
        else:
            hours = int(seconds // 3600)
            mins = int((seconds % 3600) // 60)
            return f"{hours}h {mins}m"
    
    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """Format bytes as human-readable size."""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} TB"
    
    def run(self):
        """Main heartbeat loop."""
        heartbeat_interval = get_heartbeat_interval()
        while self.running:
            time.sleep(heartbeat_interval)
            
            with self._lock:
                job = self.current_job
                progress_data = self._build_progress_data() if job else None
            
            if job:
                success = self.s3.update_heartbeat(job, self.worker_id, progress_data)
                if not success:
                    logger.warning(f"Failed to update heartbeat for {job}")
            
            # Also update worker heartbeat with system metrics
            self.s3.update_worker_heartbeat(current_job=job, system_metrics=get_system_metrics())


class Worker:
    """
    Distributed worker that processes PDF conversion jobs.
    
    Key improvements over basic design:
    1. Keeps todo marker until job completes (fixes race condition)
    2. Uses 2-character prefix sharding (256 shards vs 16)
    3. Implements heartbeat mechanism for faster stale detection
    4. Tracks retries and respects MAX_RETRIES limit
    5. Uses marker Python API directly (models stay in memory)
    6. Optimized polling: broad scan first, sharding only for contention
    7. Tracks throughput metrics (jobs completed, bytes processed, avg time)
    """
    
    # Lazy-loaded marker models (shared across all jobs)
    _marker_models: Optional[Any] = None
    _marker_converter: Optional[Any] = None
    
    # Polling optimization constants
    POLL_BATCH_SIZE = 50  # Max jobs to fetch per priority in broad scan
    MAX_LOCK_ATTEMPTS = 5  # Max attempts to lock before moving to next priority
    
    def __init__(self, s3_client: S3Client):
        self.s3 = s3_client
        self.id = WORKER_ID
        self.current_job: Optional[str] = None
        self.current_priority: Optional[str] = None
        
        # Polling optimization state
        self._empty_poll_count = 0  # Track consecutive empty polls for backoff
        self._priority_cache: dict = {}  # Cache of {priority: (has_jobs, timestamp)}
        self._cache_ttl = 120  # Seconds before priority cache expires (2 min)
        
        # Throughput metrics (for this session)
        self._session_start = time.time()
        self._jobs_completed = 0
        self._jobs_failed = 0
        self._bytes_processed = 0
        self._total_processing_time = 0.0
        
        # Register worker in S3
        logger.info(f"Registering worker {self.id}...")
        self.s3.register_worker()
        
        # Start heartbeat thread
        self.heartbeat = HeartbeatThread(s3_client, self.id)
        self.heartbeat.start()
        
        # Set up tqdm progress callback to route to heartbeat
        set_tqdm_progress_callback(self.heartbeat.update_tqdm_progress)
        
        logger.info(f"Worker {self.id} initialized.")
        self.cleanup_previous_session()
    
    def cleanup_previous_session(self):
        """
        Check for stale locks from previous runs of this worker.
        This handles the case where this worker crashed while holding a lock.
        """
        logger.info(f"Worker {self.id}: Checking for stale locks from previous session...")
        jobs = self.s3.list_processing()
        count = 0
        
        for job in jobs:
            key = job['Key']
            if key.endswith("/"):
                continue
            
            data = self.s3.get_object_json(key)
            if not data:
                continue
            
            if data.get('worker') == self.id:
                job_hash = key.split("/")[-1]
                priority = data.get('priority', DEFAULT_PRIORITY)
                logger.info(f"Recovering crashed job {job_hash} (Priority: {priority})")
                
                # Restore to todo queue (the todo marker might still exist due to our fix)
                self.s3.move_to_todo(job_hash, priority, increment_retry=False)
                self.s3.release_lock(job_hash)
                count += 1
        
        if count > 0:
            logger.info(f"Recovered {count} jobs.")
        else:
            logger.debug("No stale locks found.")
    
    def acquire_job(self) -> Optional[str]:
        """
        Attempt to acquire a job from the queue.
        
        Optimized polling strategy:
        1. Do a broad scan (no shard filter) to find available jobs quickly
        2. Shuffle candidates to distribute load across workers
        3. Only use sharding implicitly via hash distribution
        
        This reduces S3 requests from 5*256 worst case to just 5 LIST calls
        when jobs are scarce, while still providing good distribution.
        
        Returns the job hash if acquired, None otherwise.
        """
        import time as _time
        
        for priority in PRIORITIES:
            # Check priority cache to skip empty queues
            if self._is_priority_cached_empty(priority):
                continue
            
            # Broad scan: list jobs without shard filter (much faster)
            todos = self.s3.list_todo_batch(priority, max_keys=self.POLL_BATCH_SIZE)
            
            if not todos:
                # Cache that this priority is empty
                self._cache_priority_empty(priority)
                continue
            
            # Shuffle to distribute work across workers
            random.shuffle(todos)
            
            # Try to acquire a job (with limited attempts to avoid blocking)
            attempts = 0
            for job_hash in todos:
                if attempts >= self.MAX_LOCK_ATTEMPTS:
                    break
                
                # Check if job exceeds retry limit (in dead-letter queue)
                if self.s3.exists(f"{S3_PREFIX_DEAD}/{job_hash}"):
                    logger.debug(f"Skipping {job_hash}: In dead-letter queue")
                    continue
                
                logger.debug(f"Attempting to lock {job_hash} ({priority})...")
                attempts += 1
                
                # Try to acquire lock atomically
                if self.s3.acquire_lock(job_hash, self.id, priority):
                    # Get retry count from todo marker if available
                    todo_key = f"{S3_PREFIX_TODO}/{priority}/{job_hash}"
                    todo_data = self.s3.get_object_json(todo_key)
                    retry_count = 0
                    original_name = "unknown"
                    if todo_data:
                        retry_count = todo_data.get('retries', 0)
                        original_name = todo_data.get('original_name', 'unknown')
                    
                    logger.info(f"Lock acquired for {job_hash[:12]}... (priority={priority}, retry={retry_count}, file={original_name})")
                    
                    # Update lock with retry count
                    lock_data = self.s3.get_lock_info(job_hash)
                    if lock_data:
                        lock_data['retries'] = retry_count
                        self.s3.put_object(
                            f"{S3_PREFIX_PROCESSING}/{job_hash}",
                            json.dumps(lock_data)
                        )
                    
                    # NOTE: We do NOT delete the todo marker here!
                    # It will be deleted only after successful completion.
                    # This prevents the race condition where a crash after lock
                    # acquisition but before todo deletion loses the job.
                    
                    self.current_job = job_hash
                    self.current_priority = priority
                    self.heartbeat.set_job(job_hash)
                    
                    # Reset empty poll counter on success
                    self._empty_poll_count = 0
                    
                    return job_hash
                else:
                    # Lock already held by another worker
                    continue
        
        # No jobs found - track for backoff
        self._empty_poll_count += 1
        return None
    
    def _is_priority_cached_empty(self, priority: str) -> bool:
        """Check if a priority queue is cached as empty."""
        import time as _time
        if priority not in self._priority_cache:
            return False
        has_jobs, cached_at = self._priority_cache[priority]
        if _time.time() - cached_at > self._cache_ttl:
            # Cache expired
            del self._priority_cache[priority]
            return False
        return not has_jobs
    
    def _cache_priority_empty(self, priority: str):
        """Cache that a priority queue is empty."""
        import time as _time
        self._priority_cache[priority] = (False, _time.time())
    
    def get_poll_backoff(self) -> float:
        """
        Get adaptive backoff delay based on consecutive empty polls.
        
        Returns delay in seconds with exponential backoff and jitter.
        Tuned for long-running jobs (avg ~6 hours).
        """
        if self._empty_poll_count == 0:
            return 0
        
        # Exponential backoff: 5s, 10s, 20s, 40s, 80s, 160s, 320s, 640s, 1280s, 2560s, max 3600s (60 min)
        base_delay = min(3600, 5 * (2 ** min(self._empty_poll_count - 1, 9)))
        
        # Add jitter (±25%)
        jitter = base_delay * 0.25 * (random.random() * 2 - 1)
        
        return max(5, base_delay + jitter)
    
    def process(self, job_hash: str):
        """
        Process a PDF conversion job.
        
        Steps:
        1. Download PDF and metadata from S3
        2. Run marker conversion
        3. Create info.json with enriched metadata
        4. Zip results and upload
        5. Clean up (delete todo marker and lock)
        6. Update throughput metrics
        """
        logger.info(f"Processing Job: {job_hash}")
        job_start_time = time.time()
        
        # Get current retry count
        lock_info = self.s3.get_lock_info(job_hash)
        retry_count = lock_info.get('retries', 0) if lock_info else 0
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            pdf_path = os.path.join(tmp_dir, "source.pdf")
            out_dir = os.path.join(tmp_dir, "output")
            os.makedirs(out_dir, exist_ok=True)
            
            # 1. Download & Metadata
            raw_key = f"{S3_PREFIX_RAW}/{job_hash}.pdf"
            try:
                self.s3.download_file(raw_key, pdf_path)
                s3_meta = self.s3.get_object_metadata(raw_key)
                
                # Get file info for heartbeat
                original_filename = s3_meta.get("original-name", "unknown.pdf")
                try:
                    file_size = int(s3_meta.get("size", 0)) or os.path.getsize(pdf_path)
                except (ValueError, OSError):
                    file_size = 0
                
                # Get page count
                page_count = get_pdf_page_count(pdf_path)
                if page_count:
                    logger.info(f"PDF has {page_count} pages")
                
                # Update heartbeat with file info
                self.heartbeat.set_job(
                    job_hash, 
                    progress={"stage": "downloaded", "page_count": page_count},
                    original_filename=original_filename,
                    file_size=file_size
                )
                
            except Exception as e:
                logger.error(f"Download/Meta failed: {e}")
                self._handle_failure(
                    job_hash, f"Download failed: {e}", retry_count,
                    context={"stage": "download", "raw_key": raw_key}
                )
                return
            
            # 2. Convert using marker Python API
            logger.info("Running marker conversion...")
            self.heartbeat.update_progress({"stage": "converting"})
            
            try:
                if self.s3.mock:
                    # Mock marker output
                    logger.info("[MOCK] Skipping actual conversion")
                    md_text = "# Mock Conversion\n\nThis is mock content."
                    images = {}
                    marker_meta = {"mock": True}
                else:
                    md_text, images, marker_meta = self._run_marker_conversion(pdf_path)
                    # Extract page count from marker metadata if available
                    if marker_meta and 'page_stats' in marker_meta:
                        page_count = len(marker_meta['page_stats'])
                    logger.info(f"Marker conversion completed: {len(md_text)} chars, {len(images)} images, {page_count or '?'} pages")
            except Exception as e:
                logger.error(f"Marker failed: {type(e).__name__}: {e}")
                self._handle_failure(
                    job_hash, str(e), retry_count,
                    context={
                        "stage": "conversion", 
                        "original_filename": original_filename,
                        "file_size": file_size
                    }
                )
                return
            
            self.heartbeat.update_progress({
                "stage": "packaging",
                "page_count": page_count,
                "image_count": len(images),
                "output_chars": len(md_text)
            })
            
            # Save markdown
            md_path = os.path.join(out_dir, "content.md")
            with open(md_path, "w", encoding="utf-8") as f:
                f.write(md_text)
            
            # Save images to assets folder
            if images:
                assets_dir = os.path.join(out_dir, "assets")
                os.makedirs(assets_dir, exist_ok=True)
                for img_name, img in images.items():
                    img_path = os.path.join(assets_dir, img_name)
                    # Convert to RGB if needed (for JPEG)
                    if hasattr(img, 'mode') and img.mode != "RGB":
                        img = img.convert("RGB")
                    img.save(img_path)
                logger.info(f"Saved {len(images)} images to assets/")
            
            # List files for debugging
            output_files = [f for _, _, files in os.walk(out_dir) for f in files]
            logger.info(f"Output files: {output_files}")
            
            # 3. Create info.json with enriched metadata
            info = {
                "hash": job_hash,
                "converted_at": datetime.now().isoformat() + "Z",
                "worker_id": self.id,
                "original_filename": s3_meta.get("original-name", "unknown.pdf"),
                "tags": json.loads(s3_meta.get("tags", "[]")),
                "size_bytes": s3_meta.get("size", "0"),
                "marker_meta": marker_meta
            }
            with open(os.path.join(out_dir, "info.json"), "w") as f:
                json.dump(info, f, indent=2)
            
            # 4. Zip & Upload
            zip_path = os.path.join(tmp_dir, f"{job_hash}.zip")
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                for root, _, files in os.walk(out_dir):
                    for file in files:
                        p = os.path.join(root, file)
                        zf.write(p, os.path.relpath(p, out_dir))
            
            self.heartbeat.update_progress({"stage": "uploading"})
            self.s3.upload_file(zip_path, f"{S3_PREFIX_DONE}/{job_hash}.zip")
            
            # Calculate processing time
            processing_time = time.time() - job_start_time
            
            # 5. Finalize - NOW we delete the todo marker (atomic completion)
            self._complete_job(job_hash, file_size, processing_time)
            logger.info(f"Job {job_hash} Complete in {self.heartbeat._format_duration(processing_time)}.")
    
    def _complete_job(self, job_hash: str, file_size: int = 0, processing_time: float = 0):
        """
        Mark job as complete:
        - Delete todo marker (NOW it's safe to do so)
        - Delete processing lock
        - Clean up any failed marker
        - Update throughput metrics
        """
        # Update throughput metrics
        self._jobs_completed += 1
        self._bytes_processed += file_size
        self._total_processing_time += processing_time
        
        # Update worker metrics in S3
        self._update_worker_metrics()
        
        # Delete todo marker from all priorities (it should only be in one)
        for priority in PRIORITIES:
            self.s3.delete_object(f"{S3_PREFIX_TODO}/{priority}/{job_hash}")
        
        # Release processing lock
        self.s3.release_lock(job_hash)
        
        # Clean up any previous failed marker
        self.s3.delete_object(f"{S3_PREFIX_FAILED}/{job_hash}")
        
        # Clear heartbeat
        self.heartbeat.set_job(None)
        self.current_job = None
        self.current_priority = None
    
    def _update_worker_metrics(self):
        """Update worker metrics in S3 registry."""
        metrics = self.get_throughput_metrics()
        self.s3.update_worker_metrics(metrics)
    
    def get_throughput_metrics(self) -> dict:
        """Get throughput metrics for this worker session."""
        session_duration = time.time() - self._session_start
        avg_time = self._total_processing_time / self._jobs_completed if self._jobs_completed > 0 else 0
        
        return {
            "session_start": datetime.fromtimestamp(self._session_start).isoformat() + "Z",
            "session_duration_seconds": round(session_duration, 1),
            "jobs_completed": self._jobs_completed,
            "jobs_failed": self._jobs_failed,
            "bytes_processed": self._bytes_processed,
            "bytes_processed_formatted": self.heartbeat._format_size(self._bytes_processed),
            "total_processing_time": round(self._total_processing_time, 1),
            "avg_processing_time": round(avg_time, 1),
            "avg_processing_time_formatted": self.heartbeat._format_duration(avg_time) if avg_time > 0 else "N/A",
            "jobs_per_hour": round(self._jobs_completed / (session_duration / 3600), 2) if session_duration > 0 else 0,
        }
    
    def _handle_failure(self, job_hash: str, reason: str, retry_count: int,
                        traceback_str: str = None, context: dict = None):
        """
        Handle a job failure:
        - If retries < max_retries: Mark as failed (janitor will retry)
        - If retries >= max_retries: Move to dead-letter queue
        - Track failed job count
        - Save detailed error log
        """
        import traceback
        
        max_retries = get_max_retries()
        logger.warning(f"Job {job_hash} FAILED (retry {retry_count}/{max_retries}): {reason}")
        
        # Track failure metric
        self._jobs_failed += 1
        self._update_worker_metrics()
        
        # Save detailed error information
        error_context = context or {}
        error_context['worker_id'] = self.id
        error_context['retry_count'] = retry_count
        error_context['max_retries'] = max_retries
        
        # Get traceback if not provided
        if traceback_str is None:
            traceback_str = traceback.format_exc()
        
        self.s3.save_job_error_detail(
            job_hash, 
            error=reason, 
            traceback=traceback_str,
            context=error_context
        )
        
        if retry_count >= max_retries:
            logger.error(f"Job {job_hash} exceeded max retries. Moving to dead-letter queue.")
            self.s3.mark_dead(job_hash, reason, retry_count)
            
            # Also delete the todo marker since job is permanently failed
            for priority in PRIORITIES:
                self.s3.delete_object(f"{S3_PREFIX_TODO}/{priority}/{job_hash}")
        else:
            # Mark as failed - janitor will move back to todo with incremented retry
            self.s3.mark_failed(job_hash, reason, self.id, retry_count)
        
        # Clear heartbeat
        self.heartbeat.set_job(None)
        self.current_job = None
        self.current_priority = None
    
    def _init_marker(self):
        """
        Initialize marker models (lazy loading).
        Models are shared across all jobs to avoid reloading ~3GB of weights.
        """
        if Worker._marker_converter is not None:
            return
        
        logger.info("Initializing marker models (this may take a while on first run)...")
        try:
            from marker.models import create_model_dict
            from marker.converters.pdf import PdfConverter
            
            Worker._marker_models = create_model_dict()
            Worker._marker_converter = PdfConverter(
                artifact_dict=Worker._marker_models,
                config={},
            )
            logger.info("Marker models initialized successfully.")
        except ImportError as e:
            raise RuntimeError(
                f"marker-pdf not installed. Install with: pip install marker-pdf\n"
                f"Error: {e}"
            )
    
    def _run_marker_conversion(self, pdf_path: str) -> tuple:
        """
        Convert PDF to markdown using marker Python API.
        
        Returns:
            tuple: (markdown_text, images_dict, metadata_dict)
        """
        self._init_marker()
        
        from marker.output import text_from_rendered
        
        # Run conversion
        rendered = Worker._marker_converter(pdf_path)
        
        # Extract text, format, and images
        text, ext, images = text_from_rendered(rendered)
        
        # Update image paths in markdown to use assets/ prefix
        for img_name in images.keys():
            text = text.replace(f"({img_name})", f"(assets/{img_name})")
        
        # Extract metadata (convert to JSON-serializable dict)
        meta = {}
        if hasattr(rendered, 'metadata') and rendered.metadata:
            try:
                # rendered.metadata might be a pydantic model or dict
                if hasattr(rendered.metadata, 'model_dump'):
                    meta = rendered.metadata.model_dump()
                elif hasattr(rendered.metadata, 'dict'):
                    meta = rendered.metadata.dict()
                elif isinstance(rendered.metadata, dict):
                    meta = rendered.metadata
            except Exception as e:
                logger.warning(f"Could not serialize marker metadata: {e}")
                meta = {"error": str(e)}
        
        return text, images, meta
    
    def shutdown(self):
        """Clean shutdown of worker."""
        self.heartbeat.stop()
        # Wait for heartbeat thread to finish current iteration
        self.heartbeat.join(timeout=get_heartbeat_interval() + 5)
        
        # Deregister worker
        logger.info(f"Deregistering worker {self.id}...")
        self.s3.deregister_worker()
        
        logger.info(f"Worker {self.id} shut down.")


def main():
    parser = argparse.ArgumentParser(description="BlobForge PDF Worker")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually modify S3")
    parser.add_argument("--run-once", action="store_true", help="Process one job and exit")
    args = parser.parse_args()
    
    client = S3Client(dry_run=args.dry_run)
    worker = Worker(client)
    
    logger.info(f"Worker {worker.id} started. Polling for jobs...")
    
    try:
        while True:
            job = worker.acquire_job()
            if job:
                worker.process(job)
                if args.run_once:
                    break
            else:
                if args.run_once:
                    logger.info("No jobs found.")
                    break
                # Use adaptive backoff when no jobs found
                backoff = worker.get_poll_backoff()
                logger.debug(f"No jobs found, backing off for {backoff:.1f}s (empty polls: {worker._empty_poll_count})")
                time.sleep(backoff)
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    finally:
        worker.shutdown()


if __name__ == "__main__":
    main()
