"""
BlobForge Ingestor - Scans directories for PDFs and queues them for processing.

Supports:
- Git LFS pointer files (extracts SHA256 without downloading)
- Regular PDF files (computes SHA256 hash with xattr caching)
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

from .config import (
    S3_PREFIX_RAW, S3_PREFIX_TODO, S3_PREFIX_DONE, S3_PREFIX_FAILED,
    S3_PREFIX_PROCESSING, S3_PREFIX_DEAD, PRIORITIES, DEFAULT_PRIORITY
)
from .s3_client import S3Client
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
    
    This function is state-aware and checks all queue states before adding a job:
    - Skips if already done (output exists)
    - Skips if currently processing
    - Skips if already queued (any priority)
    - Skips if in failed queue (will be retried by janitor)
    - Skips if in dead-letter queue (permanently failed)
    
    Also updates the manifest with file metadata for lookup.
    """
    s3 = S3Client(dry_run=dry_run)
    
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
    
    # Fetch manifest once for efficient duplicate detection
    print("Fetching manifest for duplicate detection...")
    manifest = s3.get_manifest()
    known_hashes = set(manifest.get('entries', {}).keys())
    print(f"Manifest contains {len(known_hashes)} known files.")
    
    stats = {"found": 0, "skipped": 0, "uploaded": 0, "queued": 0}
    
    # Batch manifest entries for efficient updates
    manifest_batch = []
    MANIFEST_BATCH_SIZE = 50
    
    for full_path, base_path in files_to_process:
        file = os.path.basename(full_path)
        rel_path = os.path.relpath(full_path, base_path)
        stats["found"] += 1
        
        # Use original_name if provided (e.g., from telegram upload), else use detected filename
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
        
        # 2. Check if already known (manifest is source of truth)
        if file_hash in known_hashes:
            print(f"  [SKIP] Already ingested")
            stats["skipped"] += 1
            continue
        
        # 3. Check/Upload Raw + Metadata
        raw_key = f"{S3_PREFIX_RAW}/{file_hash}.pdf"
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
        
        # 4. Queue the job
        s3.create_todo_marker(priority, file_hash)
        print(f"  [QUEUED] Added with priority {priority}")
        stats["queued"] += 1
        
        # Add to known hashes so duplicates in same run are caught
        known_hashes.add(file_hash)
        
        # 5. Collect manifest entry
        try:
            size = os.path.getsize(full_path)
        except Exception:
            size = 0
        
        manifest_batch.append({
            'hash': file_hash,
            'path': rel_path,
            'size': size,
            'tags': tags,
            'source': os.path.basename(base_path)
        })
        
        # Batch update manifest
        if len(manifest_batch) >= MANIFEST_BATCH_SIZE:
            print(f"  [MANIFEST] Updating with {len(manifest_batch)} entries...")
            s3.update_manifest(manifest_batch)
            manifest_batch = []
    
    # Final manifest update for remaining entries
    failed_manifest_entries = []
    
    if manifest_batch:
        print(f"  [MANIFEST] Updating with {len(manifest_batch)} remaining entries...")
        if not s3.update_manifest(manifest_batch):
            failed_manifest_entries.extend(manifest_batch)
    
    # If manifest update failed, save entries for later recovery
    if failed_manifest_entries:
        recovery_file = "/tmp/blobforge_manifest_recovery.json"
        try:
            # Append to existing recovery file if it exists
            existing = []
            if os.path.exists(recovery_file):
                with open(recovery_file, 'r') as f:
                    existing = json.load(f)
            existing.extend(failed_manifest_entries)
            with open(recovery_file, 'w') as f:
                json.dump(existing, f, indent=2)
            print(f"\n  [WARNING] Manifest update failed for {len(failed_manifest_entries)} entries.")
            print(f"  Saved to {recovery_file} for recovery.")
            print(f"  Run 'blobforge manifest --repair' to retry, or 'blobforge manifest --rebuild' to rebuild from S3.")
        except Exception as e:
            print(f"\n  [ERROR] Could not save recovery file: {e}")
            print(f"  Failed entries: {[e['hash'][:12] for e in failed_manifest_entries]}")
    
    # Print summary
    print("\n--- Ingest Summary ---")
    print(f"  Found:    {stats['found']} PDFs")
    print(f"  Uploaded: {stats['uploaded']}")
    print(f"  Queued:   {stats['queued']}")
    print(f"  Skipped:  {stats['skipped']} (already in manifest)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BlobForge Ingestor")
    parser.add_argument("paths", nargs='+', help="PDF files or directories to ingest")
    parser.add_argument("--priority", default=DEFAULT_PRIORITY, choices=PRIORITIES,
                        help="Queue priority for new jobs")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually modify S3")
    args = parser.parse_args()
    
    ingest(args.paths, priority=args.priority, dry_run=args.dry_run)
