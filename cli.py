import os
import argparse
import sys
import json
from config import *
import ingestor

# Re-use S3 Client logic (simplified)
class S3Client:
    def __init__(self):
        try:
            import boto3
            self.s3 = boto3.client('s3')
        except ImportError:
            print("Error: boto3 is required for CLI.")
            sys.exit(1)

    def list_keys(self, prefix):
        paginator = self.s3.get_paginator('list_objects_v2')
        keys = []
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    keys.append(obj['Key'])
        return keys

    def exists(self, key):
        try:
            self.s3.head_object(Bucket=S3_BUCKET, Key=key)
            return True
        except: return False

    def copy(self, src, dest):
        self.s3.copy_object(Bucket=S3_BUCKET, CopySource={'Bucket': S3_BUCKET, 'Key': src}, Key=dest)

    def delete(self, key):
        self.s3.delete_object(Bucket=S3_BUCKET, Key=key)

    def read_json(self, key):
        try:
            resp = self.s3.get_object(Bucket=S3_BUCKET, Key=key)
            return json.loads(resp['Body'].read().decode('utf-8'))
        except: return None

def cmd_ingest(args):
    print(f"Ingesting {args.path} with priority {args.priority}...")
    ingestor.ingest(args.path, priority=args.priority)

def cmd_reprioritize(args):
    s3 = S3Client()
    job_hash = args.hash
    new_prio = args.priority
    
    # 1. Check if Processing
    proc_key = f"{S3_PREFIX_PROCESSING}/{job_hash}"
    if s3.exists(proc_key):
        print(f"Error: Job {job_hash} is currently being processed. Cannot reprioritize.")
        return

    # 2. Check if Done
    done_key = f"{S3_PREFIX_DONE}/{job_hash}.zip"
    if s3.exists(done_key):
        print(f"Error: Job {job_hash} is already finished.")
        return

    # 3. Find current priority
    current_prio = None
    current_key = None
    
    for p in PRIORITIES:
        key = f"{S3_PREFIX_TODO}/{p}/{job_hash}"
        if s3.exists(key):
            current_prio = p
            current_key = key
            break
    
    if not current_prio:
        print(f"Error: Job {job_hash} not found in any queue.")
        return

    if current_prio == new_prio:
        print(f"Job is already in {new_prio}.")
        return

    # 4. Move
    print(f"Moving {job_hash} from {current_prio} to {new_prio}...")
    new_key = f"{S3_PREFIX_TODO}/{new_prio}/{job_hash}"
    s3.copy(current_key, new_key)
    s3.delete(current_key)
    print("Done.")

def cmd_status(args):
    s3 = S3Client()
    h = args.hash
    
    if s3.exists(f"{S3_PREFIX_DONE}/{h}.zip"):
        print(f"Status: DONE (s3://{S3_BUCKET}/{S3_PREFIX_DONE}/{h}.zip)")
        return
        
    if s3.exists(f"{S3_PREFIX_FAILED}/{h}"):
        print(f"Status: FAILED")
        err = s3.read_json(f"{S3_PREFIX_FAILED}/{h}")
        print(f"Reason: {err}")
        return

    if s3.exists(f"{S3_PREFIX_PROCESSING}/{h}"):
        print(f"Status: PROCESSING")
        meta = s3.read_json(f"{S3_PREFIX_PROCESSING}/{h}")
        print(f"Worker: {meta.get('worker')}")
        print(f"Started: {meta.get('started')}")
        return

    for p in PRIORITIES:
        if s3.exists(f"{S3_PREFIX_TODO}/{p}/{h}"):
            print(f"Status: QUEUED (Priority: {p})")
            return
            
    print("Status: UNKNOWN (Not found)")

def cmd_list(args):
    s3 = S3Client()
    
    print("--- Queue Stats ---")
    for p in PRIORITIES:
        keys = s3.list_keys(f"{S3_PREFIX_TODO}/{p}/")
        print(f"{p:<12}: {len(keys)} jobs")
        if args.verbose:
            for k in keys[:5]: print(f"  - {k.split('/')[-1]}")
            
    proc_keys = s3.list_keys(f"{S3_PREFIX_PROCESSING}/")
    print(f"Processing  : {len(proc_keys)} jobs")
    if args.verbose:
        for k in proc_keys[:5]: 
            h = k.split('/')[-1]
            print(f"  - {h}")

def main():
    parser = argparse.ArgumentParser(prog="pdf-cli")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Ingest
    p_ingest = subparsers.add_parser("ingest", help="Ingest PDF files")
    p_ingest.add_argument("path", help="Path to files or repo")
    p_ingest.add_argument("--priority", default=DEFAULT_PRIORITY, choices=PRIORITIES)
    p_ingest.set_defaults(func=cmd_ingest)

    # Reprioritize
    p_prio = subparsers.add_parser("reprioritize", help="Change job priority")
    p_prio.add_argument("hash", help="SHA256 Hash of the file")
    p_prio.add_argument("priority", choices=PRIORITIES)
    p_prio.set_defaults(func=cmd_reprioritize)

    # Status
    p_stat = subparsers.add_parser("status", help="Check status of a specific hash")
    p_stat.add_argument("hash", help="SHA256 Hash")
    p_stat.set_defaults(func=cmd_status)

    # List
    p_list = subparsers.add_parser("list", help="List queues")
    p_list.add_argument("--verbose", "-v", action="store_true")
    p_list.set_defaults(func=cmd_list)

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
