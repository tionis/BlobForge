import os
import re
import sys
import argparse
import subprocess
import hashlib
import json
from typing import Optional, List, Set
from config import *

# Regex for Git LFS pointer file
LFS_POINTER_REGEX = re.compile(r"oid sha256:([a-f0-9]{64})")

class S3Client:
    """
    Abstracts S3 operations. 
    """
    def __init__(self, dry_run=False):
        self.dry_run = dry_run
        try:
            import boto3
            self.s3 = boto3.client('s3')
            self.mock = False
        except ImportError:
            print("Warning: boto3 not found. Running in MOCK mode.")
            self.mock = True

    def exists(self, key: str) -> bool:
        if self.mock:
            return False 
        try:
            self.s3.head_object(Bucket=S3_BUCKET, Key=key)
            return True
        except:
            return False

    def upload_file(self, local_path: str, key: str, metadata: dict = None):
        if self.dry_run or self.mock:
            meta_str = json.dumps(metadata) if metadata else "{}"
            print(f"[DRY-RUN/MOCK] Uploading {local_path} -> s3://{S3_BUCKET}/{key} (Meta: {meta_str})")
            return
        
        extra_args = {}
        if metadata:
            extra_args['Metadata'] = metadata
            
        print(f"Uploading {local_path} -> s3://{S3_BUCKET}/{key}")
        self.s3.upload_file(local_path, S3_BUCKET, key, ExtraArgs=extra_args)

    def create_marker(self, key: str, content: str = ""):
        if self.dry_run or self.mock:
            print(f"[DRY-RUN/MOCK] Creating marker s3://{S3_BUCKET}/{key}")
            return
        
        self.s3.put_object(Bucket=S3_BUCKET, Key=key, Body=content)


def get_lfs_hash(filepath: str) -> Optional[str]:
    try:
        with open(filepath, 'r', errors='ignore') as f:
            header = f.read(200)
            match = LFS_POINTER_REGEX.search(header)
            if match:
                return match.group(1)
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
    return None

def compute_sha256(filepath: str) -> str:
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def pull_lfs_file(repo_path: str, filepath: str):
    print(f"Materializing LFS file: {filepath}")
    subprocess.run(["git", "lfs", "pull", "--include", filepath], check=True, cwd=repo_path)

def cleanup_lfs_file(repo_path: str, filepath: str):
    print(f"Cleaning up (reverting to pointer): {filepath}")
    subprocess.run(["git", "checkout", filepath], check=True, cwd=repo_path)

def get_tags(rel_path: str) -> List[str]:
    # ./books/comics/obelix.pdf -> ['books', 'comics', 'obelix']
    # Normalize path separators
    parts = os.path.normpath(rel_path).split(os.sep)
    # Remove filename extension for the last part? Or just keep directory structure?
    # User said: "derived from the path (so `./books/comics/obelix` becomes ['books', 'comics', 'obelix'])"
    # Assuming the user meant folders + filename-without-ext
    
    tags = []
    for p in parts[:-1]:
        if p and p != ".":
            tags.append(p)
            
    filename = parts[-1]
    name_no_ext = os.path.splitext(filename)[0]
    tags.append(name_no_ext)
    
    return tags

def ingest(repo_path: str, priority: str = DEFAULT_PRIORITY, dry_run: bool = False):
    s3 = S3Client(dry_run=dry_run)
    
    print(f"Scanning {repo_path}...")
    
    for root, _, files in os.walk(repo_path):
        for file in files:
            if not file.lower().endswith(".pdf"):
                continue
            
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, repo_path)
            
            # Metadata Prep
            tags = get_tags(rel_path)
            
            # 1. Identify Hash
            file_hash = get_lfs_hash(full_path)
            is_pointer = True
            
            if not file_hash:
                is_pointer = False
                try:
                    with open(full_path, 'rb') as f:
                        header = f.read(4)
                        if header != b'%PDF':
                            print(f"Skipping {rel_path}: Not a valid PDF")
                            continue
                except: continue
                print(f"Found non-LFS PDF: {rel_path}. Computing hash...")
                file_hash = compute_sha256(full_path)
            
            print(f"Found: {rel_path} -> {file_hash[:8]}...")

            # 2. Check Done
            zip_key = f"{S3_PREFIX_DONE}/{file_hash}.zip"
            if s3.exists(zip_key):
                print(f"  [SKIP] Already converted: {zip_key}")
                continue

            # 3. Check/Upload Raw + Metadata
            raw_key = f"{S3_PREFIX_RAW}/{file_hash}.pdf"
            if not s3.exists(raw_key):
                print(f"  [MISSING] Raw PDF not in S3.")
                
                # We need actual file size for metadata. 
                # If pointer, we need to pull it first to get size? 
                # Or LFS pointer has size! "size 12345"
                # Let's pull it to be safe and accurate for upload.
                
                if is_pointer:
                    try:
                        pull_lfs_file(repo_path, rel_path)
                        # Now file is materialized
                        size = os.path.getsize(full_path)
                        metadata = {
                            "original-name": file,
                            "tags": json.dumps(tags),
                            "size": str(size)
                        }
                        s3.upload_file(full_path, raw_key, metadata=metadata)
                        cleanup_lfs_file(repo_path, rel_path)
                    except subprocess.CalledProcessError as e:
                        print(f"  [ERROR] Git LFS pull failed: {e}")
                        continue
                else:
                    size = os.path.getsize(full_path)
                    metadata = {
                        "original-name": file,
                        "tags": json.dumps(tags),
                        "size": str(size)
                    }
                    s3.upload_file(full_path, raw_key, metadata=metadata)
            else:
                print(f"  [OK] Raw PDF exists.")

            # 4. Queue
            todo_key = f"{S3_PREFIX_TODO}/{priority}/{file_hash}"
            if not s3.exists(todo_key):
                # Check other priorities? Ideally yes, but naive check is okay for now.
                # The CLI "status" command can check widely.
                s3.create_marker(todo_key)
                print(f"  [QUEUED] Added to {todo_key}")
            else:
                print(f"  [QUEUED] Already in queue.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    parser.add_argument("--priority", default=DEFAULT_PRIORITY, choices=PRIORITIES)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    
    if not os.path.isdir(args.path):
        print(f"Error: {args.path} is not a directory")
        sys.exit(1)
        
    ingest(args.path, priority=args.priority, dry_run=args.dry_run)