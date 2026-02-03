import os
import re
import sys
import argparse
import subprocess
import hashlib
from typing import Optional, List, Set

# Configuration
# Set these env vars or use defaults
S3_BUCKET = os.getenv("S3_BUCKET", "my-pdf-bucket")
S3_PREFIX_RAW = "store/raw"
S3_PREFIX_TODO = "queue/todo"
S3_PREFIX_DONE = "store/out"

# Regex for Git LFS pointer file
# version https://git-lfs.github.com/spec/v1
# oid sha256:4d7a214614ab2935c943f9e0ff69d22eadbb8f32b1258daaa5e2ca24d17e23b8
# size 12345
LFS_POINTER_REGEX = re.compile(r"oid sha256:([a-f0-9]{64})")

class S3Client:
    """
    Abstracts S3 operations. 
    Currently mocks operations if BOTO3 is not available or if DRY_RUN is set.
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
            # Simulate some files existing (for testing)
            return False 
        try:
            self.s3.head_object(Bucket=S3_BUCKET, Key=key)
            return True
        except:
            return False

    def upload_file(self, local_path: str, key: str):
        if self.dry_run or self.mock:
            print(f"[DRY-RUN/MOCK] Uploading {local_path} -> s3://{S3_BUCKET}/{key}")
            return
        
        print(f"Uploading {local_path} -> s3://{S3_BUCKET}/{key}")
        self.s3.upload_file(local_path, S3_BUCKET, key)

    def create_marker(self, key: str, content: str = ""):
        if self.dry_run or self.mock:
            print(f"[DRY-RUN/MOCK] Creating marker s3://{S3_BUCKET}/{key}")
            return
        
        self.s3.put_object(Bucket=S3_BUCKET, Key=key, Body=content)


def get_lfs_hash(filepath: str) -> Optional[str]:
    """
    Reads the first few bytes of a file to check if it's an LFS pointer.
    Returns the SHA256 hash if found, otherwise None (implying it's a real file).
    """
    try:
        # LFS pointers are tiny (< 200 bytes)
        with open(filepath, 'r', errors='ignore') as f:
            header = f.read(200)
            match = LFS_POINTER_REGEX.search(header)
            if match:
                return match.group(1)
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
    return None

def compute_sha256(filepath: str) -> str:
    """Computes SHA256 of a regular file."""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def pull_lfs_file(repo_path: str, filepath: str):
    """Runs git lfs pull for a specific file to materialize it."""
    print(f"Materializing LFS file: {filepath}")
    subprocess.run(["git", "lfs", "pull", "--include", filepath], check=True, cwd=repo_path)

def cleanup_lfs_file(repo_path: str, filepath: str):
    """
    Reverts a file back to its pointer state to save space.
    This effectively does 'git checkout <file>' if the index expects a pointer.
    """
    print(f"Cleaning up (reverting to pointer): {filepath}")
    subprocess.run(["git", "checkout", filepath], check=True, cwd=repo_path)

def ingest(repo_path: str, priority: str = "2_normal", dry_run: bool = False):
    s3 = S3Client(dry_run=dry_run)
    
    print(f"Scanning {repo_path}...")
    
    for root, _, files in os.walk(repo_path):
        for file in files:
            if not file.lower().endswith(".pdf"):
                continue
            
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, repo_path)
            
            # 1. Identify Hash
            file_hash = get_lfs_hash(full_path)
            is_pointer = True
            
            if not file_hash:
                # It's a real PDF (not LFS pointer), compute hash manually
                is_pointer = False
                
                # Verify Magic Number
                try:
                    with open(full_path, 'rb') as f:
                        header = f.read(4)
                        if header != b'%PDF':
                            print(f"Skipping {rel_path}: Not a valid PDF (Magic number: {header})")
                            continue
                except Exception as e:
                    print(f"Error reading {rel_path}: {e}")
                    continue

                print(f"Found non-LFS PDF: {rel_path}. Computing hash...")
                file_hash = compute_sha256(full_path)
            
            print(f"Found: {rel_path} -> {file_hash[:8]}...")

            # 2. Check "Done" State (Output Zip exists)
            zip_key = f"{S3_PREFIX_DONE}/{file_hash}.zip"
            if s3.exists(zip_key):
                print(f"  [SKIP] Already converted: {zip_key}")
                continue

            # 3. Check/Upload Raw PDF
            raw_key = f"{S3_PREFIX_RAW}/{file_hash}.pdf"
            if not s3.exists(raw_key):
                print(f"  [MISSING] Raw PDF not in S3.")
                
                if is_pointer:
                    try:
                        pull_lfs_file(repo_path, rel_path) # Must use relative path for git command usually
                        # Now the file is the real PDF
                        s3.upload_file(full_path, raw_key)
                        cleanup_lfs_file(repo_path, rel_path)
                    except subprocess.CalledProcessError as e:
                        print(f"  [ERROR] Git LFS pull failed for {rel_path}: {e}")
                        continue
                else:
                    # It was already a real file
                    s3.upload_file(full_path, raw_key)
            else:
                print(f"  [OK] Raw PDF exists in S3.")

            # 4. Queue for Processing
            todo_key = f"{S3_PREFIX_TODO}/{priority}/{file_hash}"
            # Naive check: if not in todo, add it. 
            # (Ideally check processing/failed too, but this is idempotent-ish)
            if not s3.exists(todo_key):
                s3.create_marker(todo_key)
                print(f"  [QUEUED] Added to {todo_key}")
            else:
                print(f"  [QUEUED] Already in queue.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest PDFs from Git LFS to S3 Queue")
    parser.add_argument("path", help="Path to local git repository")
    parser.add_argument("--priority", default="2_normal", help="Priority tier (1_high, 2_normal, 3_low)")
    parser.add_argument("--dry-run", action="store_true", help="Don't upload or modify S3")
    
    args = parser.parse_args()
    
    if not os.path.isdir(args.path):
        print(f"Error: {args.path} is not a directory")
        sys.exit(1)
        
    ingest(args.path, priority=args.priority, dry_run=args.dry_run)
