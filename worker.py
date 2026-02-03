import os
import sys
import time
import uuid
import json
import shutil
import random
import argparse
import subprocess
import zipfile
import tempfile
from datetime import datetime

# Configuration
S3_BUCKET = os.getenv("S3_BUCKET", "my-pdf-bucket")
WORKER_ID = os.getenv("WORKER_ID", str(uuid.uuid4())[:8])

# Paths
S3_PREFIX_RAW = "store/raw"
S3_PREFIX_TODO = "queue/todo"
S3_PREFIX_PROCESSING = "queue/processing"
S3_PREFIX_DONE = "store/out"
S3_PREFIX_FAILED = "queue/failed"

PRIORITIES = ["1_high", "2_normal", "3_low"]

class S3Client:
    def __init__(self, dry_run=False):
        self.dry_run = dry_run
        try:
            import boto3
            import botocore
            self.s3 = boto3.client('s3')
            self.ClientError = botocore.exceptions.ClientError
            self.mock = False
        except ImportError:
            print("Warning: boto3 not found. Running in MOCK mode.")
            self.mock = True
            self.ClientError = Exception # Dummy for mock

    def list_todo(self, priority, prefix_char=""):
        """
        List todo items for a specific priority, optionally filtering by a hex prefix character.
        """
        full_prefix = f"{S3_PREFIX_TODO}/{priority}/{prefix_char}"
        
        if self.mock:
            # Return a mock hash that starts with the prefix if provided
            mock_hash = f"{prefix_char}mock_hash_123" if prefix_char else "mock_hash_123"
            return [mock_hash]
        
        try:
            response = self.s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=full_prefix, MaxKeys=10)
            if 'Contents' not in response:
                return []
            # Extract HASH from key "queue/todo/PRIORITY/HASH"
            return [obj['Key'].split('/')[-1] for obj in response['Contents']]
        except Exception as e:
            print(f"Error listing todo: {e}")
            return []

    def download_file(self, key, local_path):
        if self.mock:
            print(f"[MOCK] Downloading {key} -> {local_path}")
            # Create dummy PDF for testing
            with open(local_path, "wb") as f:
                f.write(b"%PDF-1.4 mock content")
            return
        
        self.s3.download_file(S3_BUCKET, key, local_path)

    def upload_file(self, local_path, key):
        if self.mock or self.dry_run:
            print(f"[MOCK/DRY] Uploading {local_path} -> {key}")
            return
        self.s3.upload_file(local_path, S3_BUCKET, key)

    def delete_file(self, key):
        if self.mock or self.dry_run:
            print(f"[MOCK/DRY] Deleting {key}")
            return
        self.s3.delete_object(Bucket=S3_BUCKET, Key=key)

    def put_object(self, key, body, **kwargs):
        if self.mock or self.dry_run:
            print(f"[MOCK/DRY] Putting {key}")
            # Simulate atomic lock failure 10% of the time in mock
            if kwargs.get('IfNoneMatch') == '*' and random.random() < 0.1:
                raise Exception("Mock Precondition Failed")
            return
        
        self.s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body, **kwargs)


class Worker:
    def __init__(self, s3_client):
        self.s3 = s3_client
        self.id = WORKER_ID

    def acquire_job(self):
        """
        Iterate priorities, then shards. Try to lock.
        """
        # Randomly select a hex character (0-9, a-f) to shard the poll
        shard_char = random.choice("0123456789abcdef")
        
        for priority in PRIORITIES:
            # print(f"Polling {priority} (shard {shard_char})...") # verbose
            todos = self.s3.list_todo(priority, prefix_char=shard_char)
            
            if not todos:
                continue

            for job_hash in todos:
                print(f"Attempting to acquire lock for {job_hash} (Priority: {priority})...")
                
                # Canonical Lock Key: queue/processing/<HASH>
                lock_key = f"{S3_PREFIX_PROCESSING}/{job_hash}"
                timestamp = int(time.time() * 1000)
                payload = json.dumps({"worker": self.id, "started": timestamp, "priority": priority})
                
                try:
                    # Atomic Write: Only succeeds if key does not exist
                    self.s3.put_object(lock_key, payload, IfNoneMatch='*')
                    
                    # Lock Acquired!
                    print(f"Lock acquired for {job_hash}")
                    # Remove from TODO
                    self.s3.delete_file(f"{S3_PREFIX_TODO}/{priority}/{job_hash}")
                    return job_hash

                except self.s3.ClientError as e:
                    error_code = e.response['Error']['Code']
                    if error_code in ['PreconditionFailed', '412']:
                        print(f"Lock contention for {job_hash}. Skipping.")
                        continue
                    else:
                        print(f"Unexpected S3 error: {e}")
                        continue
                except Exception as e:
                     # Handle mock exception
                     if "Precondition Failed" in str(e):
                         print(f"Lock contention for {job_hash} (Mock). Skipping.")
                         continue
                     raise e
        
        return None

    def release_lock(self, job_hash):
        # Delete the single lock file
        key = f"{S3_PREFIX_PROCESSING}/{job_hash}"
        if self.s3.mock:
            print(f"[MOCK] Releasing lock {key}")
            return
        self.s3.delete_file(key)

    def process(self, job_hash):
        print(f"Processing Job: {job_hash}")
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Setup paths
            pdf_path = os.path.join(tmp_dir, "source.pdf")
            out_dir = os.path.join(tmp_dir, "output")
            os.makedirs(out_dir, exist_ok=True)
            
            # 1. Download
            try:
                self.s3.download_file(f"{S3_PREFIX_RAW}/{job_hash}.pdf", pdf_path)
            except Exception as e:
                print(f"Failed to download PDF: {e}")
                self.fail_job(job_hash, "Download failed")
                return

            # 2. Convert (Marker)
            print("Running marker conversion...")
            try:
                # Assuming 'marker_single' command is available
                # marker_single <pdf> <out_dir> --batch_multiplier 2
                cmd = [
                    "marker_single", 
                    pdf_path, 
                    out_dir, 
                    "--batch_multiplier", "2",
                    "--max_pages", "10" # Safety limit for dev, maybe remove for prod
                ]
                
                if self.s3.mock:
                    print(f"[MOCK] Executing: {' '.join(cmd)}")
                    # Create fake output
                    with open(os.path.join(out_dir, "index.md"), "w") as f:
                        f.write("# Mock Conversion\n\nContent.")
                    with open(os.path.join(out_dir, "info.json"), "w") as f:
                        json.dump({"hash": job_hash}, f)
                else:
                    subprocess.run(cmd, check=True, capture_output=True)
            
            except FileNotFoundError:
                print("Error: 'marker_single' command not found. Is marker-pdf installed?")
                self.fail_job(job_hash, "Marker binary missing")
                return
            except subprocess.CalledProcessError as e:
                print(f"Marker failed: {e.stderr}")
                self.fail_job(job_hash, f"Conversion failed: {e.stderr}")
                return

            # 3. Zip
            zip_path = os.path.join(tmp_dir, f"{job_hash}.zip")
            print(f"Zipping to {zip_path}...")
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                for root, _, files in os.walk(out_dir):
                    for file in files:
                        abs_path = os.path.join(root, file)
                        rel_path = os.path.relpath(abs_path, out_dir)
                        zf.write(abs_path, rel_path)

            # 4. Upload Result
            self.s3.upload_file(zip_path, f"{S3_PREFIX_DONE}/{job_hash}.zip")
            
            # 5. Cleanup Lock
            self.release_lock(job_hash)
            print(f"Job {job_hash} Complete.")

    def fail_job(self, job_hash, reason):
        print(f"Job {job_hash} FAILED: {reason}")
        # Move to failed queue
        self.s3.put_object(f"{S3_PREFIX_FAILED}/{job_hash}", json.dumps({"error": str(reason)}))
        self.release_lock(job_hash)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--run-once", action="store_true", help="Process one job and exit")
    args = parser.parse_args()

    client = S3Client(dry_run=args.dry_run)
    worker = Worker(client)

    print(f"Worker {WORKER_ID} started.")
    
    while True:
        job = worker.acquire_job()
        if job:
            worker.process(job)
            if args.run_once:
                print("Run-once mode: Exiting after successful job.")
                break
        else:
            if args.run_once:
                print("Run-once mode: No jobs found. Exiting.")
                break
            print("No jobs found. Sleeping...")
            time.sleep(10)
