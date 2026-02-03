"""
BlobForge CLI - Command-line interface for managing PDF conversion jobs.

Commands:
- ingest: Scan directory and queue PDFs for processing
- status: Check status of a specific job by hash
- list: List queue statistics
- reprioritize: Change priority of a queued job
- retry: Retry a failed or dead-letter job
- janitor: Run janitor to recover stale jobs
"""
import os
import sys
import json
import argparse

from .config import (
    S3_BUCKET, S3_PREFIX_RAW, S3_PREFIX_TODO, S3_PREFIX_PROCESSING,
    S3_PREFIX_DONE, S3_PREFIX_FAILED, S3_PREFIX_DEAD, PRIORITIES, DEFAULT_PRIORITY
)
from .s3_client import S3Client
from . import ingestor
from . import janitor as janitor_module
from . import status as status_module


def cmd_ingest(args):
    """Ingest PDFs from a directory."""
    print(f"Ingesting {args.path} with priority {args.priority}...")
    ingestor.ingest(args.path, priority=args.priority, dry_run=args.dry_run)


def cmd_reprioritize(args):
    """Change the priority of a queued job."""
    s3 = S3Client()
    job_hash = args.hash
    new_prio = args.priority
    
    # 1. Check if Processing
    if s3.exists(f"{S3_PREFIX_PROCESSING}/{job_hash}"):
        print(f"Error: Job {job_hash} is currently being processed. Cannot reprioritize.")
        return 1
    
    # 2. Check if Done
    if s3.exists(f"{S3_PREFIX_DONE}/{job_hash}.zip"):
        print(f"Error: Job {job_hash} is already finished.")
        return 1
    
    # 3. Check if Dead
    if s3.exists(f"{S3_PREFIX_DEAD}/{job_hash}"):
        print(f"Error: Job {job_hash} is in dead-letter queue. Use 'retry' command first.")
        return 1
    
    # 4. Find current priority
    current_prio = None
    current_key = None
    
    for p in PRIORITIES:
        key = f"{S3_PREFIX_TODO}/{p}/{job_hash}"
        if s3.exists(key):
            current_prio = p
            current_key = key
            break
    
    if not current_prio:
        # Check if in failed queue
        if s3.exists(f"{S3_PREFIX_FAILED}/{job_hash}"):
            print(f"Error: Job {job_hash} is in failed queue. It will be retried by janitor.")
            return 1
        print(f"Error: Job {job_hash} not found in any queue.")
        return 1
    
    if current_prio == new_prio:
        print(f"Job is already in {new_prio}.")
        return 0
    
    # 5. Move to new priority (preserve retry count if present)
    print(f"Moving {job_hash} from {current_prio} to {new_prio}...")
    
    # Read existing marker content
    existing_content = s3.get_object(current_key)
    
    new_key = f"{S3_PREFIX_TODO}/{new_prio}/{job_hash}"
    s3.copy_object(current_key, new_key)
    s3.delete_object(current_key)
    
    print("Done.")
    return 0


def cmd_status(args):
    """Check the status of a specific job."""
    s3 = S3Client()
    h = args.hash
    
    # Check done
    if s3.exists(f"{S3_PREFIX_DONE}/{h}.zip"):
        print(f"Status: DONE")
        print(f"Output: s3://{S3_BUCKET}/{S3_PREFIX_DONE}/{h}.zip")
        return 0
    
    # Check dead-letter
    if s3.exists(f"{S3_PREFIX_DEAD}/{h}"):
        print(f"Status: DEAD (exceeded max retries)")
        data = s3.get_object_json(f"{S3_PREFIX_DEAD}/{h}")
        if data:
            print(f"Error: {data.get('error', 'Unknown')}")
            print(f"Total retries: {data.get('total_retries', '?')}")
        print(f"\nUse 'retry {h}' to retry this job.")
        return 0
    
    # Check failed
    if s3.exists(f"{S3_PREFIX_FAILED}/{h}"):
        print(f"Status: FAILED (pending retry)")
        data = s3.get_object_json(f"{S3_PREFIX_FAILED}/{h}")
        if data:
            print(f"Error: {data.get('error', 'Unknown')}")
            print(f"Retries so far: {data.get('retries', 0)}")
            print(f"Worker: {data.get('worker', '?')}")
        print(f"\nJanitor will retry this job automatically.")
        return 0
    
    # Check processing
    if s3.exists(f"{S3_PREFIX_PROCESSING}/{h}"):
        print(f"Status: PROCESSING")
        data = s3.get_object_json(f"{S3_PREFIX_PROCESSING}/{h}")
        if data:
            print(f"Worker: {data.get('worker', '?')}")
            started = data.get('started')
            if started:
                from datetime import datetime
                started_dt = datetime.fromtimestamp(started / 1000.0)
                print(f"Started: {started_dt.isoformat()}")
            progress = data.get('progress')
            if progress:
                print(f"Progress: {progress}")
        return 0
    
    # Check todo queues
    for p in PRIORITIES:
        key = f"{S3_PREFIX_TODO}/{p}/{h}"
        if s3.exists(key):
            print(f"Status: QUEUED")
            print(f"Priority: {p}")
            data = s3.get_object_json(key)
            if data:
                retries = data.get('retries', 0)
                if retries > 0:
                    print(f"Previous retries: {retries}")
            return 0
    
    # Check if raw exists
    if s3.exists(f"{S3_PREFIX_RAW}/{h}.pdf"):
        print(f"Status: RAW ONLY (not queued)")
        print(f"The PDF exists but is not queued for processing.")
        print(f"Use ingest to add it to the queue.")
        return 0
    
    print("Status: UNKNOWN (not found)")
    return 1


def cmd_list(args):
    """List queue statistics."""
    s3 = S3Client()
    
    print("--- Queue Statistics ---")
    
    # Todo queues
    print("\n[TODO]")
    total = 0
    for p in PRIORITIES:
        keys = s3.list_keys(f"{S3_PREFIX_TODO}/{p}/")
        count = len(keys)
        total += count
        print(f"  {p:<12}: {count}")
        if args.verbose and keys:
            for k in keys[:5]:
                print(f"    - {k.split('/')[-1][:16]}...")
            if len(keys) > 5:
                print(f"    ... and {len(keys) - 5} more")
    print(f"  {'TOTAL':<12}: {total}")
    
    # Processing
    print("\n[PROCESSING]")
    proc_keys = s3.list_keys(f"{S3_PREFIX_PROCESSING}/")
    proc_keys = [k for k in proc_keys if not k.endswith("/")]
    print(f"  Active: {len(proc_keys)}")
    if args.verbose and proc_keys:
        for k in proc_keys[:5]:
            h = k.split('/')[-1]
            data = s3.get_object_json(k)
            worker = data.get('worker', '?') if data else '?'
            print(f"    - {h[:16]}... (worker: {worker})")
    
    # Failed
    print("\n[FAILED]")
    failed_keys = s3.list_keys(f"{S3_PREFIX_FAILED}/")
    failed_keys = [k for k in failed_keys if not k.endswith("/")]
    print(f"  Pending retry: {len(failed_keys)}")
    
    # Dead
    print("\n[DEAD-LETTER]")
    dead_keys = s3.list_keys(f"{S3_PREFIX_DEAD}/")
    dead_keys = [k for k in dead_keys if not k.endswith("/")]
    print(f"  Permanently failed: {len(dead_keys)}")
    if args.verbose and dead_keys:
        for k in dead_keys[:5]:
            h = k.split('/')[-1]
            print(f"    - {h[:16]}...")
    
    # Done
    print("\n[DONE]")
    done_count = s3.count_prefix(f"{S3_PREFIX_DONE}/")
    print(f"  Completed: {done_count}")


def cmd_retry(args):
    """Retry a failed or dead-letter job."""
    s3 = S3Client()
    job_hash = args.hash
    priority = args.priority
    
    # Check if already done
    if s3.exists(f"{S3_PREFIX_DONE}/{job_hash}.zip"):
        print(f"Error: Job {job_hash} is already completed.")
        return 1
    
    # Check if already queued
    for p in PRIORITIES:
        if s3.exists(f"{S3_PREFIX_TODO}/{p}/{job_hash}"):
            print(f"Error: Job {job_hash} is already queued (priority: {p}).")
            return 1
    
    # Check if processing
    if s3.exists(f"{S3_PREFIX_PROCESSING}/{job_hash}"):
        print(f"Error: Job {job_hash} is currently being processed.")
        return 1
    
    # Check dead-letter queue
    dead_key = f"{S3_PREFIX_DEAD}/{job_hash}"
    failed_key = f"{S3_PREFIX_FAILED}/{job_hash}"
    
    source = None
    if s3.exists(dead_key):
        source = "dead-letter"
        data = s3.get_object_json(dead_key)
    elif s3.exists(failed_key):
        source = "failed"
        data = s3.get_object_json(failed_key)
    else:
        # Check if raw exists
        if s3.exists(f"{S3_PREFIX_RAW}/{job_hash}.pdf"):
            print(f"Job {job_hash} is not in failed or dead-letter queue.")
            print(f"Creating new todo marker...")
            source = "raw"
            data = None
        else:
            print(f"Error: Job {job_hash} not found anywhere.")
            return 1
    
    # Reset retry count if requested
    retry_count = 0
    if not args.reset_retries and data:
        retry_count = data.get('retries', data.get('total_retries', 0))
    
    # Create new todo marker
    marker_content = json.dumps({
        "retries": retry_count,
        "queued_at": int(__import__('time').time() * 1000),
        "recovered_from": f"manual_retry_{source}",
        "previous_error": data.get('error', 'Unknown') if data else None
    })
    
    print(f"Retrying job {job_hash} from {source}...")
    print(f"  Priority: {priority}")
    print(f"  Retry count: {retry_count} {'(reset)' if args.reset_retries else ''}")
    
    s3.put_object(f"{S3_PREFIX_TODO}/{priority}/{job_hash}", marker_content)
    
    # Clean up source
    if source == "dead-letter":
        s3.delete_object(dead_key)
    elif source == "failed":
        s3.delete_object(failed_key)
    
    print("Done. Job queued for processing.")
    return 0


def cmd_janitor(args):
    """Run the janitor to recover stale jobs."""
    janitor_module.run_janitor(dry_run=args.dry_run, verbose=args.verbose)


def cmd_worker(args):
    """Start a worker to process jobs."""
    from . import worker as worker_module
    import time
    import logging
    
    logger = logging.getLogger(__name__)
    
    client = S3Client(dry_run=args.dry_run)
    w = worker_module.Worker(client)
    
    logger.info(f"Worker {w.id} started. Polling for jobs...")
    
    try:
        while True:
            job = w.acquire_job()
            if job:
                w.process(job)
                if args.run_once:
                    break
            else:
                if args.run_once:
                    logger.info("No jobs found.")
                    break
                time.sleep(10)
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    finally:
        w.shutdown()


def cmd_dashboard(args):
    """Show system status dashboard."""
    status_module.show_status(verbose=args.verbose)


def cmd_lookup(args):
    """Look up files in the manifest."""
    s3 = S3Client()
    
    if args.hash:
        # Look up by hash
        entry = s3.lookup_by_hash(args.hash)
        if entry:
            print(f"Hash: {args.hash}")
            print(f"Paths: {', '.join(entry.get('paths', ['?']))}")
            print(f"Size: {entry.get('size', 0):,} bytes")
            print(f"Tags: {', '.join(entry.get('tags', []))}")
            print(f"Ingested: {entry.get('ingested_at', '?')}")
            print(f"Source: {entry.get('source', '?')}")
        else:
            print(f"Hash {args.hash} not found in manifest.")
            return 1
    elif args.path:
        # Look up by path
        file_hash = s3.lookup_by_path(args.path)
        if file_hash:
            print(f"Path: {args.path}")
            print(f"Hash: {file_hash}")
            entry = s3.lookup_by_hash(file_hash)
            if entry:
                print(f"Size: {entry.get('size', 0):,} bytes")
                print(f"Tags: {', '.join(entry.get('tags', []))}")
        else:
            print(f"Path '{args.path}' not found in manifest.")
            return 1
    return 0


def cmd_search(args):
    """Search the manifest by filename or tag."""
    s3 = S3Client()
    
    results = s3.search_manifest(args.query)
    
    if not results:
        print(f"No matches found for '{args.query}'")
        return 1
    
    print(f"Found {len(results)} matches for '{args.query}':\n")
    
    for entry in results[:args.limit]:
        print(f"  Hash: {entry['hash'][:16]}...")
        paths = entry.get('paths', [])
        if paths:
            print(f"  Path: {paths[0]}")
            if len(paths) > 1:
                print(f"        (+{len(paths) - 1} more)")
        print(f"  Tags: {', '.join(entry.get('tags', [])[:5])}")
        print()
    
    if len(results) > args.limit:
        print(f"... and {len(results) - args.limit} more results")
    
    return 0


def cmd_manifest_stats(args):
    """Show manifest statistics."""
    s3 = S3Client()
    
    manifest = s3.get_manifest()
    entries = manifest.get('entries', {})
    
    print("--- Manifest Statistics ---")
    print(f"Version: {manifest.get('version', '?')}")
    print(f"Last updated: {manifest.get('updated_at', 'Never')}")
    print(f"Total entries: {len(entries)}")
    
    if entries:
        total_size = sum(e.get('size', 0) for e in entries.values())
        total_paths = sum(len(e.get('paths', [])) for e in entries.values())
        all_tags = set()
        for e in entries.values():
            all_tags.update(e.get('tags', []))
        
        print(f"Total size: {total_size / (1024*1024*1024):.2f} GB")
        print(f"Total paths: {total_paths}")
        print(f"Unique tags: {len(all_tags)}")
        
        if args.verbose:
            print("\nTop 10 tags:")
            from collections import Counter
            tag_counts = Counter()
            for e in entries.values():
                tag_counts.update(e.get('tags', []))
            for tag, count in tag_counts.most_common(10):
                print(f"  {tag}: {count}")
    
    return 0


def main():
    parser = argparse.ArgumentParser(
        prog="blobforge",
        description="BlobForge - Distributed PDF Conversion System"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Ingest
    p_ingest = subparsers.add_parser("ingest", help="Ingest PDF files from a directory")
    p_ingest.add_argument("path", help="Path to directory containing PDFs")
    p_ingest.add_argument("--priority", default=DEFAULT_PRIORITY, choices=PRIORITIES,
                          help="Queue priority for new jobs")
    p_ingest.add_argument("--dry-run", action="store_true", help="Don't make changes")
    p_ingest.set_defaults(func=cmd_ingest)
    
    # Status (single job)
    p_status = subparsers.add_parser("status", help="Check status of a specific job")
    p_status.add_argument("hash", help="SHA256 hash of the PDF")
    p_status.set_defaults(func=cmd_status)
    
    # List
    p_list = subparsers.add_parser("list", help="List queue statistics")
    p_list.add_argument("--verbose", "-v", action="store_true", help="Show job details")
    p_list.set_defaults(func=cmd_list)
    
    # Reprioritize
    p_prio = subparsers.add_parser("reprioritize", help="Change priority of a queued job")
    p_prio.add_argument("hash", help="SHA256 hash of the PDF")
    p_prio.add_argument("priority", choices=PRIORITIES, help="New priority")
    p_prio.set_defaults(func=cmd_reprioritize)
    
    # Retry
    p_retry = subparsers.add_parser("retry", help="Retry a failed or dead-letter job")
    p_retry.add_argument("hash", help="SHA256 hash of the PDF")
    p_retry.add_argument("--priority", default=DEFAULT_PRIORITY, choices=PRIORITIES,
                         help="Queue priority for retried job")
    p_retry.add_argument("--reset-retries", action="store_true",
                         help="Reset retry counter to 0")
    p_retry.set_defaults(func=cmd_retry)
    
    # Janitor
    p_janitor = subparsers.add_parser("janitor", help="Run janitor to recover stale jobs")
    p_janitor.add_argument("--dry-run", action="store_true", help="Don't make changes")
    p_janitor.add_argument("--verbose", "-v", action="store_true", help="Show all jobs")
    p_janitor.set_defaults(func=cmd_janitor)
    
    # Worker
    p_worker = subparsers.add_parser("worker", help="Start a worker to process jobs")
    p_worker.add_argument("--dry-run", action="store_true", help="Don't actually modify S3")
    p_worker.add_argument("--run-once", action="store_true", help="Process one job and exit")
    p_worker.set_defaults(func=cmd_worker)
    
    # Dashboard
    p_dash = subparsers.add_parser("dashboard", help="Show system status dashboard")
    p_dash.add_argument("--verbose", "-v", action="store_true", help="Show detailed info")
    p_dash.set_defaults(func=cmd_dashboard)
    
    # Lookup (manifest)
    p_lookup = subparsers.add_parser("lookup", help="Look up a file in the manifest")
    p_lookup.add_argument("--hash", help="Look up by SHA256 hash")
    p_lookup.add_argument("--path", help="Look up by file path")
    p_lookup.set_defaults(func=cmd_lookup)
    
    # Search (manifest)
    p_search = subparsers.add_parser("search", help="Search manifest by filename or tag")
    p_search.add_argument("query", help="Search query (filename or tag)")
    p_search.add_argument("--limit", type=int, default=20, help="Max results to show")
    p_search.set_defaults(func=cmd_search)
    
    # Manifest stats
    p_manifest = subparsers.add_parser("manifest", help="Show manifest statistics")
    p_manifest.add_argument("--verbose", "-v", action="store_true", help="Show tag breakdown")
    p_manifest.set_defaults(func=cmd_manifest_stats)
    
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    
    args = parser.parse_args()
    result = args.func(args)
    sys.exit(result if result else 0)


if __name__ == "__main__":
    main()
