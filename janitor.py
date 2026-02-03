import os
import sys
import json
import time
import argparse
from datetime import datetime, timedelta

# Configuration
S3_BUCKET = os.getenv("S3_BUCKET", "my-pdf-bucket")
S3_PREFIX_TODO = "queue/todo"
S3_PREFIX_PROCESSING = "queue/processing"
TIMEOUT_HOURS = 2

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

    def list_processing(self):
        if self.mock:
            # Simulate a stale job
            return ["mock_stale_job"]
        
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            jobs = []
            for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX_PROCESSING):
                if 'Contents' in page:
                    jobs.extend(page['Contents'])
            return jobs
        except Exception as e:
            print(f"Error listing processing: {e}")
            return []

    def read_object(self, key):
        if self.mock:
            # Return stale timestamp
            old_time = int((datetime.now() - timedelta(hours=3)).timestamp() * 1000)
            return json.dumps({"worker": "mock_dead", "started": old_time, "priority": "2_normal"})
        
        try:
            response = self.s3.get_object(Bucket=S3_BUCKET, Key=key)
            return response['Body'].read().decode('utf-8')
        except Exception as e:
            print(f"Error reading {key}: {e}")
            return None

    def create_marker(self, key):
        if self.dry_run or self.mock:
            print(f"[DRY/MOCK] Restoring marker {key}")
            return
        self.s3.put_object(Bucket=S3_BUCKET, Key=key, Body="")

    def delete_file(self, key):
        if self.dry_run or self.mock:
            print(f"[DRY/MOCK] Deleting stale lock {key}")
            return
        self.s3.delete_object(Bucket=S3_BUCKET, Key=key)

def run_janitor(dry_run=False):
    s3 = S3Client(dry_run=dry_run)
    
    print("Janitor: Scanning for stale locks...")
    
    processing_jobs = s3.list_processing()
    
    now = datetime.now()
    count = 0
    
    for job in processing_jobs:
        if isinstance(job, str): # Mock string
             key = job
             last_modified = now # Mock
        else:
            key = job['Key']
            # We trust the internal timestamp inside the JSON, not the S3 LastModified
        
        # Canonical key check
        if key == S3_PREFIX_PROCESSING + "/":
            continue

        job_hash = key.split("/")[-1]
        
        content = s3.read_object(key)
        if not content:
            continue
            
        try:
            data = json.loads(content)
            started_ts = data.get('started')
            priority = data.get('priority', '2_normal')
            
            if not started_ts:
                print(f"Skipping {key}: No timestamp found.")
                continue
                
            started_dt = datetime.fromtimestamp(started_ts / 1000.0)
            age = now - started_dt
            
            if age > timedelta(hours=TIMEOUT_HOURS):
                print(f"Found stale job: {job_hash} (Age: {age}, Priority: {priority})")
                
                # Recover
                todo_key = f"{S3_PREFIX_TODO}/{priority}/{job_hash}"
                s3.create_marker(todo_key)
                s3.delete_file(key)
                count += 1
            
        except json.JSONDecodeError:
            print(f"Error parsing JSON for {key}")
        except Exception as e:
            print(f"Error processing {key}: {e}")

    print(f"Janitor run complete. Recovered {count} jobs.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    
    run_janitor(args.dry_run)
