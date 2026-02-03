"""
BlobForge Janitor - Recovers stale jobs and manages failed job retries.

Responsibilities:
1. Scan processing queue for stale jobs (no heartbeat update within STALE_TIMEOUT)
2. Move stale jobs back to todo queue with incremented retry count
3. Move jobs exceeding MAX_RETRIES to dead-letter queue
4. Clean up orphaned entries
"""
import os
import sys
import json
import argparse
from datetime import datetime, timedelta

from config import (
    S3_PREFIX_TODO, S3_PREFIX_PROCESSING, S3_PREFIX_FAILED, S3_PREFIX_DEAD,
    PRIORITIES, DEFAULT_PRIORITY, MAX_RETRIES, STALE_TIMEOUT_MINUTES
)
from s3_client import S3Client


def run_janitor(dry_run: bool = False, verbose: bool = False):
    """
    Main janitor routine.
    
    Scans for:
    1. Stale processing jobs (no heartbeat within STALE_TIMEOUT_MINUTES)
    2. Failed jobs ready for retry
    """
    s3 = S3Client(dry_run=dry_run)
    
    print(f"Janitor: Starting scan (stale timeout: {STALE_TIMEOUT_MINUTES} min, max retries: {MAX_RETRIES})...")
    
    now = datetime.now()
    stats = {
        "stale_recovered": 0,
        "failed_retried": 0,
        "moved_to_dead": 0,
        "errors": 0
    }
    
    # -------------------------------------------------------------------------
    # 1. Scan Processing Queue for Stale Jobs
    # -------------------------------------------------------------------------
    print("\n[1/2] Scanning processing queue for stale jobs...")
    
    processing_jobs = s3.scan_processing_detailed()
    
    for job in processing_jobs:
        job_hash = job['hash']
        age = job['age']
        is_stale = age > timedelta(minutes=STALE_TIMEOUT_MINUTES)
        
        if verbose:
            status = "STALE" if is_stale else "OK"
            print(f"  {job_hash[:12]}... Worker: {job['worker']}, Age: {age}, [{status}]")
        
        if not is_stale:
            continue
        
        # Job is stale - recover it
        print(f"  Found stale job: {job_hash} (Age: {age})")
        
        # Get lock info for priority and retry count
        lock_info = s3.get_lock_info(job_hash)
        priority = job.get('priority', DEFAULT_PRIORITY)
        retry_count = 0
        
        if lock_info:
            priority = lock_info.get('priority', DEFAULT_PRIORITY)
            retry_count = lock_info.get('retries', 0)
        
        # Increment retry count for stale recovery
        retry_count += 1
        
        if retry_count > MAX_RETRIES:
            # Too many retries - move to dead-letter queue
            print(f"    -> Moving to dead-letter queue (retries: {retry_count} > {MAX_RETRIES})")
            if not dry_run:
                s3.mark_dead(job_hash, f"Exceeded max retries ({retry_count})", retry_count)
                s3.release_lock(job_hash)
                # Clean up todo marker if exists
                for p in PRIORITIES:
                    s3.delete_object(f"{S3_PREFIX_TODO}/{p}/{job_hash}")
            stats["moved_to_dead"] += 1
        else:
            # Restore to todo queue
            print(f"    -> Restoring to queue (priority: {priority}, retry: {retry_count}/{MAX_RETRIES})")
            if not dry_run:
                # Create todo marker with retry count
                marker_content = json.dumps({
                    "retries": retry_count,
                    "queued_at": int(datetime.now().timestamp() * 1000),
                    "recovered_from": "stale_processing"
                })
                s3.put_object(f"{S3_PREFIX_TODO}/{priority}/{job_hash}", marker_content)
                s3.release_lock(job_hash)
            stats["stale_recovered"] += 1
    
    # -------------------------------------------------------------------------
    # 2. Scan Failed Queue for Retry
    # -------------------------------------------------------------------------
    print("\n[2/2] Scanning failed queue for jobs to retry...")
    
    failed_jobs = s3.list_failed()
    
    for obj in failed_jobs:
        key = obj['Key']
        if key.endswith("/"):
            continue
        
        job_hash = key.split('/')[-1]
        
        # Read failure info
        failed_data = s3.get_object_json(key)
        if not failed_data:
            if verbose:
                print(f"  {job_hash[:12]}... [ERROR] Could not read failure data")
            stats["errors"] += 1
            continue
        
        retry_count = failed_data.get('retries', 0) + 1
        error_msg = failed_data.get('error', 'Unknown error')
        
        if verbose:
            print(f"  {job_hash[:12]}... Retry: {retry_count}/{MAX_RETRIES}, Error: {error_msg[:50]}...")
        
        if retry_count > MAX_RETRIES:
            # Move to dead-letter queue
            print(f"  {job_hash[:12]}... -> Moving to dead-letter queue")
            if not dry_run:
                s3.mark_dead(job_hash, error_msg, retry_count)
            stats["moved_to_dead"] += 1
        else:
            # Restore to todo queue with incremented retry count
            # Use priority from failed data or default
            priority = failed_data.get('priority', DEFAULT_PRIORITY)
            
            print(f"  {job_hash[:12]}... -> Retrying (priority: {priority})")
            if not dry_run:
                marker_content = json.dumps({
                    "retries": retry_count,
                    "queued_at": int(datetime.now().timestamp() * 1000),
                    "recovered_from": "failed_queue",
                    "last_error": error_msg
                })
                s3.put_object(f"{S3_PREFIX_TODO}/{priority}/{job_hash}", marker_content)
                s3.delete_object(key)  # Remove from failed queue
            stats["failed_retried"] += 1
    
    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------
    print("\n--- Janitor Summary ---")
    print(f"  Stale jobs recovered: {stats['stale_recovered']}")
    print(f"  Failed jobs retried:  {stats['failed_retried']}")
    print(f"  Moved to dead-letter: {stats['moved_to_dead']}")
    print(f"  Errors:               {stats['errors']}")
    
    if dry_run:
        print("\n[DRY-RUN] No changes were made.")


def main():
    parser = argparse.ArgumentParser(description="BlobForge Janitor")
    parser.add_argument("--dry-run", action="store_true", help="Don't make changes, just report")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show all jobs, not just stale/failed")
    args = parser.parse_args()
    
    run_janitor(dry_run=args.dry_run, verbose=args.verbose)


if __name__ == "__main__":
    main()
