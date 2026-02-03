"""
BlobForge Ingestor - Scans directories for PDFs and queues them for processing.

Supports:
- Git LFS pointer files (extracts SHA256 without downloading)
- Regular PDF files (computes SHA256 hash)
- State-aware queueing (checks all queue states before adding)
"""
import os
import re
import sys
import argparse
import subprocess
import hashlib
import json
from typing import Optional, List

from config import (
    S3_PREFIX_RAW, S3_PREFIX_TODO, S3_PREFIX_DONE, S3_PREFIX_FAILED,
    S3_PREFIX_PROCESSING, S3_PREFIX_DEAD, PRIORITIES, DEFAULT_PRIORITY
)
from s3_client import S3Client

# Regex for Git LFS pointer file
LFS_POINTER_REGEX = re.compile(r"oid sha256:([a-f0-9]{64})")


def get_lfs_hash(filepath: str) -> Optional[str]:
    """Extract SHA256 hash from a Git LFS pointer file."""
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
    """Compute SHA256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def pull_lfs_file(repo_path: str, filepath: str):
    """Materialize a Git LFS file by pulling it."""
    print(f"Materializing LFS file: {filepath}")
    subprocess.run(["git", "lfs", "pull", "--include", filepath], check=True, cwd=repo_path)


def cleanup_lfs_file(repo_path: str, filepath: str):
    """Revert an LFS file back to pointer state."""
    print(f"Cleaning up (reverting to pointer): {filepath}")
    subprocess.run(["git", "checkout", filepath], check=True, cwd=repo_path)


def get_tags(rel_path: str) -> List[str]:
    """
    Derive tags from file path.
    Example: ./books/comics/obelix.pdf -> ['books', 'comics', 'obelix']
    """
    parts = os.path.normpath(rel_path).split(os.sep)
    
    tags = []
    for p in parts[:-1]:
        if p and p != ".":
            tags.append(p)
    
    filename = parts[-1]
    name_no_ext = os.path.splitext(filename)[0]
    tags.append(name_no_ext)
    
    return tags


def ingest(repo_path: str, priority: str = DEFAULT_PRIORITY, dry_run: bool = False):
    """
    Scan a directory for PDFs and queue them for processing.
    
    This function is state-aware and checks all queue states before adding a job:
    - Skips if already done (output exists)
    - Skips if currently processing
    - Skips if already queued (any priority)
    - Skips if in failed queue (will be retried by janitor)
    - Skips if in dead-letter queue (permanently failed)
    """
    s3 = S3Client(dry_run=dry_run)
    
    print(f"Scanning {repo_path}...")
    stats = {"found": 0, "skipped_done": 0, "skipped_queued": 0, "skipped_processing": 0,
             "skipped_failed": 0, "skipped_dead": 0, "uploaded": 0, "queued": 0}
    
    for root, _, files in os.walk(repo_path):
        for file in files:
            if not file.lower().endswith(".pdf"):
                continue
            
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, repo_path)
            stats["found"] += 1
            
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
                except Exception:
                    continue
                print(f"Found non-LFS PDF: {rel_path}. Computing hash...")
                file_hash = compute_sha256(full_path)
            
            print(f"Found: {rel_path} -> {file_hash[:8]}...")
            
            # 2. Check ALL queue states before proceeding
            states = s3.job_exists_anywhere(file_hash, PRIORITIES)
            
            if states['done']:
                print(f"  [SKIP] Already converted")
                stats["skipped_done"] += 1
                continue
            
            if states['processing']:
                print(f"  [SKIP] Currently being processed")
                stats["skipped_processing"] += 1
                continue
            
            if states['dead']:
                print(f"  [SKIP] In dead-letter queue (exceeded max retries)")
                stats["skipped_dead"] += 1
                continue
            
            if states['failed']:
                print(f"  [SKIP] In failed queue (will be retried by janitor)")
                stats["skipped_failed"] += 1
                continue
            
            if states['todo']:
                print(f"  [SKIP] Already in queue")
                stats["skipped_queued"] += 1
                continue
            
            # 3. Check/Upload Raw + Metadata
            raw_key = f"{S3_PREFIX_RAW}/{file_hash}.pdf"
            if not s3.exists(raw_key):
                print(f"  [UPLOAD] Raw PDF not in S3.")
                
                if is_pointer:
                    try:
                        pull_lfs_file(repo_path, rel_path)
                        size = os.path.getsize(full_path)
                        metadata = {
                            "original-name": file,
                            "tags": json.dumps(tags),
                            "size": str(size)
                        }
                        s3.upload_file(full_path, raw_key, metadata=metadata)
                        cleanup_lfs_file(repo_path, rel_path)
                        stats["uploaded"] += 1
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
                    stats["uploaded"] += 1
            else:
                print(f"  [OK] Raw PDF exists.")
            
            # 4. Queue the job
            s3.create_todo_marker(priority, file_hash)
            print(f"  [QUEUED] Added with priority {priority}")
            stats["queued"] += 1
    
    # Print summary
    print("\n--- Ingest Summary ---")
    print(f"  Found:              {stats['found']} PDFs")
    print(f"  Uploaded:           {stats['uploaded']}")
    print(f"  Queued:             {stats['queued']}")
    print(f"  Skipped (done):     {stats['skipped_done']}")
    print(f"  Skipped (queued):   {stats['skipped_queued']}")
    print(f"  Skipped (processing): {stats['skipped_processing']}")
    print(f"  Skipped (failed):   {stats['skipped_failed']}")
    print(f"  Skipped (dead):     {stats['skipped_dead']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BlobForge Ingestor")
    parser.add_argument("path", help="Path to directory containing PDFs")
    parser.add_argument("--priority", default=DEFAULT_PRIORITY, choices=PRIORITIES,
                        help="Queue priority for new jobs")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually modify S3")
    args = parser.parse_args()
    
    if not os.path.isdir(args.path):
        print(f"Error: {args.path} is not a directory")
        sys.exit(1)
    
    ingest(args.path, priority=args.priority, dry_run=args.dry_run)
