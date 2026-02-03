"""
BlobForge Status - Display queue statistics and job status.
"""
import os
import argparse
from datetime import datetime, timedelta

from .config import (
    S3_BUCKET, S3_PREFIX_TODO, S3_PREFIX_PROCESSING, S3_PREFIX_DONE,
    S3_PREFIX_FAILED, S3_PREFIX_DEAD, PRIORITIES, STALE_TIMEOUT_MINUTES
)
from .s3_client import S3Client


def show_status(verbose: bool = False):
    """Display comprehensive status of the processing system."""
    s3 = S3Client()
    
    print(f"--- BlobForge Status ---")
    print(f"Bucket: s3://{S3_BUCKET}")
    print(f"Stale timeout: {STALE_TIMEOUT_MINUTES} minutes")
    
    # -------------------------------------------------------------------------
    # 1. TODO Queue Counts
    # -------------------------------------------------------------------------
    print("\n[QUEUE]")
    total_todo = 0
    for prio in PRIORITIES:
        count = s3.count_prefix(f"{S3_PREFIX_TODO}/{prio}/")
        print(f"  {prio:<12}: {count}")
        total_todo += count
    print(f"  {'TOTAL':<12}: {total_todo}")
    
    # -------------------------------------------------------------------------
    # 2. Processing Status
    # -------------------------------------------------------------------------
    print("\n[PROCESSING]")
    active_jobs = s3.scan_processing_detailed()
    stale_count = sum(1 for j in active_jobs if j.get('stale', False))
    
    print(f"  Active jobs : {len(active_jobs)}")
    print(f"  Stale (>{STALE_TIMEOUT_MINUTES}m): {stale_count} {'(run janitor to recover)' if stale_count else ''}")
    
    if active_jobs and verbose:
        print("\n  Active Jobs Detail:")
        for job in sorted(active_jobs, key=lambda x: x.get('age', timedelta(0))):
            status = "[STALE]" if job.get('stale') else "[OK]"
            progress = job.get('progress', {})
            stage = progress.get('stage', '-') if progress else '-'
            print(f"    {status:7} {job['hash'][:16]}... "
                  f"Worker: {job.get('worker', '?')[:12]}, "
                  f"Age: {job.get('age', '?')}, "
                  f"Stage: {stage}")
    
    # -------------------------------------------------------------------------
    # 3. Results
    # -------------------------------------------------------------------------
    print("\n[RESULTS]")
    done_count = s3.count_prefix(f"{S3_PREFIX_DONE}/")
    failed_count = s3.count_prefix(f"{S3_PREFIX_FAILED}/")
    dead_count = s3.count_prefix(f"{S3_PREFIX_DEAD}/")
    
    print(f"  Completed   : {done_count}")
    print(f"  Failed      : {failed_count} {'(will be retried by janitor)' if failed_count else ''}")
    print(f"  Dead-letter : {dead_count} {'(exceeded max retries)' if dead_count else ''}")
    
    # -------------------------------------------------------------------------
    # 4. Progress Summary
    # -------------------------------------------------------------------------
    total_jobs = total_todo + len(active_jobs) + done_count + failed_count + dead_count
    if total_jobs > 0:
        completed_pct = (done_count / total_jobs) * 100
        failed_pct = ((failed_count + dead_count) / total_jobs) * 100
        print(f"\n[PROGRESS]")
        print(f"  Completed: {completed_pct:.1f}% ({done_count}/{total_jobs})")
        if failed_count + dead_count > 0:
            print(f"  Failed:    {failed_pct:.1f}% ({failed_count + dead_count}/{total_jobs})")


def main():
    parser = argparse.ArgumentParser(description="BlobForge Status")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed job information")
    args = parser.parse_args()
    
    show_status(verbose=args.verbose)


if __name__ == "__main__":
    main()
