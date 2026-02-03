import os
import argparse
import time
from datetime import datetime, timedelta
import json

# Configuration
S3_BUCKET = os.getenv("S3_BUCKET", "my-pdf-bucket")
S3_PREFIX_TODO = "queue/todo"
S3_PREFIX_PROCESSING = "queue/processing"
S3_PREFIX_DONE = "store/out"
S3_PREFIX_FAILED = "queue/failed"

class S3Client:
    def __init__(self, dry_run=False):
        try:
            import boto3
            self.s3 = boto3.client('s3')
            self.mock = False
        except ImportError:
            self.mock = True

    def count_prefix(self, prefix):
        if self.mock:
            return 100 # Mock count
        
        paginator = self.s3.get_paginator('list_objects_v2')
        count = 0
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            if 'Contents' in page:
                count += len(page['Contents'])
        return count

    def scan_processing(self):
        if self.mock:
            return [
                {"key": "job1", "worker": "worker-a", "age": timedelta(minutes=5), "stale": False},
                {"key": "job2", "worker": "worker-b", "age": timedelta(hours=3), "stale": True}
            ]

        jobs = []
        paginator = self.s3.get_paginator('list_objects_v2')
        now = datetime.now()

        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX_PROCESSING):
            if 'Contents' not in page:
                continue
            
            for obj in page['Contents']:
                # Optimistic: We can't read every body to get worker ID without slowing down.
                # But we can check LastModified or read a sample if needed.
                # For this summary, we'll just list them.
                key = obj['Key']
                if key.endswith("/"): continue
                
                # Fetch metadata (slow-ish, maybe limit to first 10 detailed)
                try:
                    # We have to read the object to get the internal timestamp/worker
                    # To keep it fast, maybe we only check S3 LastModified for 'stale' check?
                    # S3 LastModified is close enough to 'started' for a status check.
                    last_mod = obj['LastModified'].replace(tzinfo=None) # naive
                    age = now - last_mod
                    is_stale = age > timedelta(hours=2)
                    
                    jobs.append({
                        "key": key.split('/')[-1],
                        "worker": "Unknown (use --detail)", 
                        "age": age,
                        "stale": is_stale
                    })
                except Exception:
                    pass
        return jobs

def show_status():
    s3 = S3Client()
    print(f"--- Status for s3://{S3_BUCKET} ---")
    
    # 1. TODO Counts
    print("\n[QUEUE]")
    total_todo = 0
    for prio in ["1_high", "2_normal", "3_low"]:
        count = s3.count_prefix(f"{S3_PREFIX_TODO}/{prio}")
        print(f"  {prio:<10}: {count}")
        total_todo += count
    
    # 2. Processing
    print("\n[PROCESSING]")
    active_jobs = s3.scan_processing()
    stale_count = sum(1 for j in active_jobs if j['stale'])
    
    print(f"  Active      : {len(active_jobs)}")
    print(f"  Stale (>2h) : {stale_count} (Run janitor to recover)")
    
    # List a few active
    if active_jobs:
        print("  Latest Active:")
        # Sort by age (newest first)
        for job in sorted(active_jobs, key=lambda x: x['age'])[:5]:
             status_str = "[STALE]" if job['stale'] else "[OK]"
             print(f"    {status_str} {job['key']} (Age: {job['age']})")

    # 3. Done/Failed
    print("\n[RESULTS]")
    done_count = s3.count_prefix(S3_PREFIX_DONE)
    failed_count = s3.count_prefix(S3_PREFIX_FAILED)
    print(f"  Finished    : {done_count}")
    print(f"  Failed      : {failed_count}")
    
    # 4. Progress
    total_jobs = total_todo + len(active_jobs) + done_count + failed_count
    if total_jobs > 0:
        pct = (done_count / total_jobs) * 100
        print(f"\n[PROGRESS]: {pct:.1f}% ({done_count}/{total_jobs})")

if __name__ == "__main__":
    show_status()
