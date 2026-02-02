import os
import sys
import time
import uuid
import json
import shutil
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

class S3Client:
    def __init__(self, dry_run=False):
        self.dry_run = dry_run
        try:
            import boto3
            self.s3 = boto3.client('s3')
            self.mock = False
        except ImportError:
            print("Warning: boto3 not found. Running in MOCK mode.")
            self.mock = True

    def list_todo(self):
        if self.mock:
            return ["mock_hash_123"]
        
        try:
            response = self.s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX_TODO)
            if 'Contents' not in response:
                return []
            # Extract HASH from key "queue/todo/HASH"
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

    def put_object(self, key, body):
        if self.mock or self.dry_run:
            print(f"[MOCK/DRY] Putting {key}")
            return
        self.s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body)

    def list_processing_claims(self, file_hash):
        """List all claims for a specific hash to determine lock winner."""
        prefix = f"{S3_PREFIX_PROCESSING}/{file_hash}."
        if self.mock:
            return [] # In mock, we assume we always win
        
        response = self.s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        return response.get('Contents', [])


class Worker:
    def __init__(self, s3_client):
        self.s3 = s3_client
        self.id = WORKER_ID

    def acquire_job(self):
        """
        1. List TODO.
        2. Pick one.
        3. Attempt to lock using S3 'Check-Then-Write' pattern.
        """
        todos = self.s3.list_todo()
        if not todos:
            return None

        for job_hash in todos:
            # Try to acquire lock
            print(f"Attempting to acquire lock for {job_hash}...")
            
            # Claim Key: queue/processing/<HASH>.<TIMESTAMP>.<WORKER_ID>
            timestamp = int(time.time() * 1000)
            claim_key = f"{S3_PREFIX_PROCESSING}/{job_hash}.{timestamp}.{self.id}"
            
            # 1. Write our claim
            self.s3.put_object(claim_key, json.dumps({"worker": self.id, "started": timestamp}))
            
            # 2. Wait briefly (S3 consistency)
            if not self.s3.mock:
                time.sleep(0.5)

            # 3. Verify we are the winner (earliest timestamp/lexicographically first)
            claims = self.s3.list_processing_claims(job_hash)
            
            # If we are the ONLY claim or the FIRST claim, we win.
            # Claims are sorted by Key usually, and Key has timestamp.
            if self.s3.mock:
                return job_hash # Always win in mock

            sorted_claims = sorted(claims, key=lambda x: x['Key'])
            if sorted_claims[0]['Key'] == claim_key:
                # We won!
                # Remove from TODO
                self.s3.delete_file(f"{S3_PREFIX_TODO}/{job_hash}")
                return job_hash
            else:
                # We lost. Delete our claim.
                print(f"Lost lock for {job_hash}. Retrying...")
                self.s3.delete_file(claim_key)
                continue # Try next job in todo list
        
        return None

    def release_lock(self, job_hash):
        # Delete all processing claims for this hash (cleanup)
        if self.s3.mock:
            return

        claims = self.s3.list_processing_claims(job_hash)
        for c in claims:
            if self.id in c['Key']:
                self.s3.delete_file(c['Key'])

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
    args = parser.parse_args()

    client = S3Client(dry_run=args.dry_run)
    worker = Worker(client)

    print(f"Worker {WORKER_ID} started.")
    
    while True:
        job = worker.acquire_job()
        if job:
            worker.process(job)
        else:
            print("No jobs found. Sleeping...")
            time.sleep(10)
