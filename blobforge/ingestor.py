"""
BlobForge Ingestor - Scans directories for PDFs and queues them for processing.

Supports:
- Git LFS pointer files (extracts SHA256 without downloading)
- Regular PDF files (computes SHA256 hash with xattr caching)
- State-aware queueing (checks all queue states before adding)
"""
import os
import re
import argparse
import subprocess
import hashlib
import json
from typing import Optional, List

from .config import S3_PREFIX_RAW, PRIORITIES, DEFAULT_PRIORITY
from .s3_client import S3Client
from .coordinator_client import CoordinatorClient
from .utils import compute_sha256_with_cache, get_cached_hash

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
    """
    Compute SHA256 hash of a file with xattr caching support.
    
    Uses the caching mechanism defined in docs/file_hashing_via_xattrs.md:
    - Checks for cached hash in user.checksum.sha256 xattr
    - Validates cache using file mtime stored in user.checksum.mtime
    - On cache miss, computes hash and stores it for future use
    
    This significantly speeds up re-ingestion of unchanged files.
    """
    return compute_sha256_with_cache(filepath)


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


def ingest(paths: List[str], priority: str = DEFAULT_PRIORITY, dry_run: bool = False, original_name: Optional[str] = None):
    """
    Ingest PDF files or directories and queue them for processing.
    
    Args:
        paths: List of file paths or directories to ingest.
               Files must be PDFs. Directories are scanned recursively.
        priority: Queue priority for new jobs.
        dry_run: If True, don't make any changes to S3.
        original_name: Optional original filename to use for metadata.
                       Useful when ingesting renamed/temporary files.
    
    The Bunny coordinator is the sole source of job and file metadata. S3 stores
    only raw PDFs, conversion results, and coordinator backups.
    """
    s3 = S3Client(dry_run=dry_run)
    coordinator = CoordinatorClient()
    if not coordinator.available:
        raise RuntimeError(
            "Ingestion requires BLOBFORGE_COORDINATOR_URL and "
            "BLOBFORGE_COORDINATOR_TOKEN"
        )
    
    # Expand paths into list of (full_path, base_path) tuples
    # base_path is used to compute relative paths for tags
    files_to_process = []
    
    for path in paths:
        path = os.path.abspath(path)
        if os.path.isfile(path):
            if path.lower().endswith('.pdf'):
                # For single files, base_path is the parent directory
                files_to_process.append((path, os.path.dirname(path)))
            else:
                print(f"Skipping {path}: Not a PDF file")
        elif os.path.isdir(path):
            # Walk directory recursively
            for root, _, files in os.walk(path):
                for file in files:
                    if file.lower().endswith('.pdf'):
                        full_path = os.path.join(root, file)
                        files_to_process.append((full_path, path))
        else:
            print(f"Warning: {path} does not exist, skipping")
    
    if not files_to_process:
        print("No PDF files found.")
        return
    
    print(f"Found {len(files_to_process)} PDF(s) to process...")
    
    known_hashes = set()
    print("Using Bunny coordinator for persistent file and queue state.")
    
    stats = {"found": 0, "skipped": 0, "uploaded": 0, "queued": 0}
    
    for full_path, base_path in files_to_process:
        file = os.path.basename(full_path)
        rel_path = os.path.relpath(full_path, base_path)
        stats["found"] += 1
        
        # Use an explicitly supplied name (for example a Web upload), else the detected filename.
        display_name = original_name if original_name else file
        
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
            
            cached_hash = get_cached_hash(full_path)
            if cached_hash:
                file_hash = cached_hash
            else:
                print(f"Found non-LFS PDF: {rel_path}. Computing hash...")
                file_hash = compute_sha256(full_path)
        
        print(f"Found: {rel_path} -> {file_hash[:8]}...")
        
        # 2. Avoid duplicate entries within this ingestion run.
        if file_hash in known_hashes:
            print(f"  [SKIP] Already encountered in this run")
            stats["skipped"] += 1
            continue
        
        # 3. Check/Upload Raw + Metadata
        raw_key = f"{S3_PREFIX_RAW}/{file_hash}.pdf"
        size = 0
        if not s3.exists(raw_key):
            print(f"  [UPLOAD] Raw PDF not in S3.")
            
            if is_pointer:
                try:
                    pull_lfs_file(base_path, rel_path)
                    size = os.path.getsize(full_path)
                    metadata = {
                        "original-name": display_name,
                        "tags": json.dumps(tags),
                        "size": str(size)
                    }
                    s3.upload_file(full_path, raw_key, metadata=metadata)
                    cleanup_lfs_file(base_path, rel_path)
                    stats["uploaded"] += 1
                except subprocess.CalledProcessError as e:
                    print(f"  [ERROR] Git LFS pull failed: {e}")
                    continue
            else:
                size = os.path.getsize(full_path)
                metadata = {
                    "original-name": display_name,
                    "tags": json.dumps(tags),
                    "size": str(size)
                }
                s3.upload_file(full_path, raw_key, metadata=metadata)
                stats["uploaded"] += 1
        else:
            print(f"  [OK] Raw PDF exists.")
        
        # 4. Queue the job in the authoritative coordination backend.
        if dry_run:
            print(f"  [DRY-RUN] Would enqueue with priority {priority}")
            stats["queued"] += 1
            known_hashes.add(file_hash)
            continue
        try:
            if not size:
                raw_meta = s3.get_object_metadata(raw_key)
                size = int(raw_meta.get("size", 0))
            job = coordinator.enqueue(
                file_hash,
                priority=priority,
                original_name=display_name,
                size_bytes=size,
                paths=[rel_path],
                tags=tags,
                source=os.path.basename(base_path),
            )
            job_status = job.get("status", "todo")
            if job_status == "todo":
                print(f"  [QUEUED] Added with priority {job.get('priority', priority)}")
                stats["queued"] += 1
            else:
                print(f"  [SKIP] Coordinator already has job in state: {job_status}")
                stats["skipped"] += 1
        except Exception as e:
            print(f"  [ERROR] Coordinator enqueue failed: {e}")
            continue
        
        # Add to known hashes so duplicates in same run are caught
        known_hashes.add(file_hash)
        
    # Print summary
    print("\n--- Ingest Summary ---")
    print(f"  Found:    {stats['found']} PDFs")
    print(f"  Uploaded: {stats['uploaded']}")
    print(f"  Queued:   {stats['queued']}")
    print(f"  Skipped:  {stats['skipped']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BlobForge Ingestor")
    parser.add_argument("paths", nargs='+', help="PDF files or directories to ingest")
    parser.add_argument("--priority", default=DEFAULT_PRIORITY, choices=PRIORITIES,
                        help="Queue priority for new jobs")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually modify S3")
    args = parser.parse_args()
    
    ingest(args.paths, priority=args.priority, dry_run=args.dry_run)
