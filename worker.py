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
from config import *

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
            self.ClientError = Exception

    def list_todo(self, priority, prefix_char=""):
        full_prefix = f"{S3_PREFIX_TODO}/{priority}/{prefix_char}"
        if self.mock:
            mock_hash = f"{prefix_char}mock_hash_123" if prefix_char else "mock_hash_123"
            return [mock_hash]
        try:
            response = self.s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=full_prefix, MaxKeys=10)
            if 'Contents' not in response: return []
            return [obj['Key'].split('/')[-1] for obj in response['Contents']]
        except Exception as e:
            print(f"Error listing todo: {e}")
            return []

    def download_file(self, key, local_path):
        if self.mock:
            with open(local_path, "wb") as f: f.write(b"%PDF-1.4 mock content")
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
            if kwargs.get('IfNoneMatch') == '*' and random.random() < 0.1:
                raise Exception("Mock Precondition Failed")
            return
        self.s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body, **kwargs)

    def get_object_metadata(self, key):
        if self.mock:
            return {"original-name": "mock.pdf", "tags": '["mock"]', "size": "100"}
        try:
            response = self.s3.head_object(Bucket=S3_BUCKET, Key=key)
            return response.get('Metadata', {})
        except Exception as e:
            print(f"Error getting metadata for {key}: {e}")
            return {}

    def list_processing(self):
        if self.mock: return []
        # Basic listing for cleanup
        paginator = self.s3.get_paginator('list_objects_v2')
        jobs = []
        try:
            for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX_PROCESSING):
                if 'Contents' in page: jobs.extend(page['Contents'])
        except: pass
        return jobs

    def read_object(self, key):
        if self.mock: return None
        try:
            response = self.s3.get_object(Bucket=S3_BUCKET, Key=key)
            return response['Body'].read().decode('utf-8')
        except: return None


class Worker:
    def __init__(self, s3_client):
        self.s3 = s3_client
        self.id = WORKER_ID if WORKER_ID else str(uuid.uuid4())[:8]
        if not WORKER_ID:
            print(f"Warning: WORKER_ID not set. Using random ID {self.id}. Cleanup across restarts won't work.")
        
        self.cleanup_previous_session()

    def cleanup_previous_session(self):
        print(f"Worker {self.id}: Checking for stale locks from previous session...")
        jobs = self.s3.list_processing()
        count = 0
        for job in jobs:
            key = job['Key']
            if key.endswith("/"): continue
            
            content = self.s3.read_object(key)
            if not content: continue
            
            try:
                data = json.loads(content)
                if data.get('worker') == self.id:
                    # Found one!
                    job_hash = key.split("/")[-1]
                    prio = data.get('priority', DEFAULT_PRIORITY)
                    print(f"Recovering crashed job {job_hash} (Priority: {prio})")
                    
                    # Restore to Todo
                    todo_key = f"{S3_PREFIX_TODO}/{prio}/{job_hash}"
                    self.s3.put_object(todo_key, "", Body="") # Create marker
                    self.s3.delete_file(key) # Release lock
                    count += 1
            except: continue
        
        if count > 0:
            print(f"Recovered {count} jobs.")
        else:
            print("No stale locks found.")

    def acquire_job(self):
        shard_char = random.choice("0123456789abcdef")
        
        for priority in PRIORITIES:
            todos = self.s3.list_todo(priority, prefix_char=shard_char)
            if not todos: continue

            for job_hash in todos:
                print(f"Attempting to lock {job_hash} ({priority})...")
                lock_key = f"{S3_PREFIX_PROCESSING}/{job_hash}"
                timestamp = int(time.time() * 1000)
                payload = json.dumps({"worker": self.id, "started": timestamp, "priority": priority})
                
                try:
                    self.s3.put_object(lock_key, payload, IfNoneMatch='*')
                    print(f"Lock acquired for {job_hash}")
                    self.s3.delete_file(f"{S3_PREFIX_TODO}/{priority}/{job_hash}")
                    return job_hash
                except self.s3.ClientError as e:
                    if e.response['Error']['Code'] in ['PreconditionFailed', '412']:
                        continue
                    print(f"S3 Error: {e}")
                except Exception as e:
                     if "Precondition Failed" in str(e): continue
                     raise e
        return None

    def process(self, job_hash):
        print(f"Processing Job: {job_hash}")
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            pdf_path = os.path.join(tmp_dir, "source.pdf")
            out_dir = os.path.join(tmp_dir, "output")
            os.makedirs(out_dir, exist_ok=True)
            
            # 1. Download & Metadata
            raw_key = f"{S3_PREFIX_RAW}/{job_hash}.pdf"
            try:
                self.s3.download_file(raw_key, pdf_path)
                s3_meta = self.s3.get_object_metadata(raw_key)
            except Exception as e:
                print(f"Download/Meta failed: {e}")
                self.fail_job(job_hash, "Download failed")
                return

            # 2. Convert
            print("Running marker conversion...")
            try:
                cmd = ["marker_single", pdf_path, out_dir, "--batch_multiplier", "2", "--max_pages", "10"]
                if self.s3.mock:
                    print(f"[MOCK] Executing: {' '.join(cmd)}")
                    with open(os.path.join(out_dir, "index.md"), "w") as f: f.write("# Mock")
                else:
                    subprocess.run(cmd, check=True, capture_output=True)
            except Exception as e:
                print(f"Marker failed: {e}")
                self.fail_job(job_hash, str(e))
                return

            # 3. Create info.json with enriched metadata
            info = {
                "hash": job_hash,
                "converted_at": datetime.utcnow().isoformat(),
                "original_filename": s3_meta.get("original-name", "unknown.pdf"),
                "tags": json.loads(s3_meta.get("tags", "[]")),
                "size_bytes": s3_meta.get("size", "0")
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

            self.s3.upload_file(zip_path, f"{S3_PREFIX_DONE}/{job_hash}.zip")
            self.s3.delete_file(f"{S3_PREFIX_PROCESSING}/{job_hash}")
            print(f"Job {job_hash} Complete.")

    def fail_job(self, job_hash, reason):
        print(f"Job {job_hash} FAILED: {reason}")
        self.s3.put_object(f"{S3_PREFIX_FAILED}/{job_hash}", json.dumps({"error": str(reason)}), Body=json.dumps({"error": str(reason)}))
        self.s3.delete_file(f"{S3_PREFIX_PROCESSING}/{job_hash}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--run-once", action="store_true")
    args = parser.parse_args()

    client = S3Client(dry_run=args.dry_run)
    worker = Worker(client)
    
    print(f"Worker {worker.id} started.")
    while True:
        job = worker.acquire_job()
        if job:
            worker.process(job)
            if args.run_once: break
        else:
            if args.run_once: break
            time.sleep(10)