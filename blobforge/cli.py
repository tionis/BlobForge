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
    S3_PREFIX_DONE, S3_PREFIX_FAILED, S3_PREFIX_DEAD, PRIORITIES, DEFAULT_PRIORITY,
    get_remote_config, save_remote_config, refresh_remote_config, WORKER_ID,
    get_stale_timeout_minutes
)
from .s3_client import S3Client
from . import ingestor
from . import janitor as janitor_module
from . import status as status_module


def cmd_ingest(args):
    """Ingest PDFs from files or directories."""
    if len(args.paths) == 1:
        print(f"Ingesting {args.paths[0]} with priority {args.priority}...")
    else:
        print(f"Ingesting {len(args.paths)} paths with priority {args.priority}...")
    ingestor.ingest(args.paths, priority=args.priority, dry_run=args.dry_run)


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
    """Show manifest statistics, repair, or rebuild."""
    s3 = S3Client(dry_run=args.dry_run if hasattr(args, 'dry_run') else False)
    
    # Handle repair from recovery file
    if args.repair:
        recovery_file = "/tmp/blobforge_manifest_recovery.json"
        if not os.path.exists(recovery_file):
            print("No recovery file found at /tmp/blobforge_manifest_recovery.json")
            print("Nothing to repair.")
            return 0
        
        with open(recovery_file, 'r') as f:
            entries = json.load(f)
        
        if not entries:
            print("Recovery file is empty.")
            os.remove(recovery_file)
            return 0
        
        print(f"Found {len(entries)} entries to repair...")
        
        if args.force:
            # Force update without optimistic locking
            print("Using force update (no locking)...")
            if s3.force_update_manifest(entries):
                print("Manifest repaired successfully.")
                os.remove(recovery_file)
                return 0
            else:
                print("Force update failed.")
                return 1
        else:
            # Try normal update with retries
            if s3.update_manifest(entries, max_retries=20):
                print("Manifest repaired successfully.")
                os.remove(recovery_file)
                return 0
            else:
                print("Repair failed. Try again with --force if no other processes are running.")
                return 1
    
    # Handle full rebuild from S3
    if args.rebuild:
        print("Rebuilding manifest from S3 store/raw/...")
        print("WARNING: This will replace the current manifest.")
        
        if not args.force:
            confirm = input("Continue? [y/N] ").strip().lower()
            if confirm != 'y':
                print("Aborted.")
                return 1
        
        manifest = s3.rebuild_manifest_from_raw()
        
        if args.dry_run:
            print(f"[DRY-RUN] Would save manifest with {len(manifest['entries'])} entries")
            return 0
        
        if s3.save_manifest(manifest):
            print("Manifest rebuilt and saved successfully.")
            return 0
        else:
            print("Failed to save rebuilt manifest.")
            return 1
    
    # Handle sync (update manifest with metadata from already-ingested files)
    if args.sync:
        return cmd_manifest_sync(args, s3)
    
    # Default: show stats
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
    
    # Check for recovery file
    recovery_file = "/tmp/blobforge_manifest_recovery.json"
    if os.path.exists(recovery_file):
        try:
            with open(recovery_file, 'r') as f:
                pending = json.load(f)
            if pending:
                print(f"\n⚠️  {len(pending)} entries pending in recovery file.")
                print(f"   Run 'blobforge manifest --repair' to retry.")
        except:
            pass
    
    return 0


def cmd_manifest_sync(args, s3):
    """Sync manifest with files that are already ingested but missing from manifest."""
    print("Syncing manifest with ingested files...")
    
    # Get current manifest
    manifest = s3.get_manifest()
    manifest_hashes = set(manifest.get('entries', {}).keys())
    
    # List all raw files
    raw_files = s3.list_objects(f"{S3_PREFIX_RAW}/")
    raw_hashes = set()
    
    for obj in raw_files:
        key = obj['Key']
        if key.endswith('.pdf'):
            file_hash = key.split('/')[-1].replace('.pdf', '')
            raw_hashes.add(file_hash)
    
    # Find files in raw that are not in manifest
    missing = raw_hashes - manifest_hashes
    
    if not missing:
        print("Manifest is in sync. No missing entries.")
        return 0
    
    print(f"Found {len(missing)} files in S3 not in manifest.")
    
    # Build entries for missing files
    entries = []
    for file_hash in missing:
        key = f"{S3_PREFIX_RAW}/{file_hash}.pdf"
        metadata = s3.get_object_metadata(key)
        
        original_name = metadata.get('original-name', f'{file_hash[:8]}.pdf')
        tags_str = metadata.get('tags', '[]')
        try:
            tags = json.loads(tags_str)
        except:
            tags = []
        
        size_str = metadata.get('size', '0')
        try:
            size = int(size_str)
        except:
            size = 0
        
        entries.append({
            'hash': file_hash,
            'path': original_name,
            'size': size,
            'tags': tags,
            'source': 'synced'
        })
    
    print(f"Adding {len(entries)} entries to manifest...")
    
    if args.force:
        if s3.force_update_manifest(entries):
            print("Manifest synced successfully.")
            return 0
        else:
            print("Sync failed.")
            return 1
    else:
        if s3.update_manifest(entries, max_retries=20):
            print("Manifest synced successfully.")
            return 0
        else:
            print("Sync failed. Try again with --force if no other processes are running.")
            return 1


def cmd_config(args):
    """View or update remote configuration."""
    s3 = S3Client()
    
    if args.show:
        # Show current config
        refresh_remote_config()
        config = get_remote_config()
        print("--- Remote Configuration ---")
        print(f"(Stored in S3: {S3_BUCKET}/registry/config.json)")
        print()
        for key, value in sorted(config.items()):
            print(f"  {key}: {value}")
        return 0
    
    if args.set:
        # Parse key=value pairs
        updates = {}
        for kv in args.set:
            if '=' not in kv:
                print(f"Error: Invalid format '{kv}'. Use key=value")
                return 1
            key, value = kv.split('=', 1)
            # Try to convert to int if it looks like a number
            try:
                value = int(value)
            except ValueError:
                pass
            updates[key] = value
        
        # Get current config and merge
        refresh_remote_config()
        config = get_remote_config()
        config.update(updates)
        
        if save_remote_config(config):
            print("Configuration updated:")
            for key, value in updates.items():
                print(f"  {key} = {value}")
            return 0
        else:
            print("Error: Failed to save configuration")
            return 1
    
    # Default: show config
    refresh_remote_config()
    config = get_remote_config()
    print("--- Remote Configuration ---")
    for key, value in sorted(config.items()):
        print(f"  {key}: {value}")
    return 0


def cmd_workers(args):
    """List registered workers."""
    s3 = S3Client()
    
    if args.active:
        stale_timeout = get_stale_timeout_minutes()
        workers = s3.get_active_workers(stale_minutes=stale_timeout)
        title = f"Active Workers (heartbeat < {stale_timeout}m ago)"
    else:
        workers = s3.list_workers()
        title = "All Registered Workers"
    
    print(f"--- {title} ---")
    print()
    
    if not workers:
        print("  No workers found.")
        return 0
    
    # Sort by last heartbeat
    from datetime import datetime
    workers.sort(key=lambda w: w.get('last_heartbeat', ''), reverse=True)
    
    for w in workers:
        worker_id = w.get('worker_id', '?')[:12]
        hostname = w.get('hostname', '?')
        status = w.get('status', '?')
        last_hb = w.get('last_heartbeat', '?')
        current_job = w.get('current_job')
        
        status_icon = "🟢" if status == "active" or status == "processing" else "🔴" if status == "stopped" else "⚪"
        
        if current_job:
            print(f"  {status_icon} {worker_id} ({hostname}) [{status}] - Processing: {current_job[:12]}...")
        else:
            print(f"  {status_icon} {worker_id} ({hostname}) [{status}]")
        
        if args.verbose:
            print(f"      Platform: {w.get('platform', '?')} {w.get('platform_release', '')}")
            print(f"      Python: {w.get('python_version', '?')}")
            print(f"      CPUs: {w.get('cpu_count', '?')}, Memory: {w.get('memory_gb', '?')} GB")
            print(f"      Last heartbeat: {last_hb}")
            print()
    
    print()
    print(f"Total: {len(workers)} worker(s)")
    
    if args.active:
        # Show this worker's ID
        print(f"\nThis machine's worker ID: {WORKER_ID}")
    
    return 0


def main():
    parser = argparse.ArgumentParser(
        prog="blobforge",
        description="BlobForge - Distributed PDF Conversion System"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Ingest
    p_ingest = subparsers.add_parser("ingest", help="Ingest PDF files or directories")
    p_ingest.add_argument("paths", nargs='+', help="PDF files or directories to ingest (supports shell globbing)")
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
    
    # Manifest stats/repair/rebuild
    p_manifest = subparsers.add_parser("manifest", help="Manifest operations: stats, repair, rebuild, sync")
    p_manifest.add_argument("--verbose", "-v", action="store_true", help="Show tag breakdown")
    p_manifest.add_argument("--repair", action="store_true", 
                            help="Retry failed manifest updates from recovery file")
    p_manifest.add_argument("--rebuild", action="store_true",
                            help="Rebuild manifest by scanning S3 store/raw/")
    p_manifest.add_argument("--sync", action="store_true",
                            help="Add files in S3 that are missing from manifest")
    p_manifest.add_argument("--force", action="store_true",
                            help="Skip confirmation and use non-locking update")
    p_manifest.add_argument("--dry-run", action="store_true", help="Don't make changes")
    p_manifest.set_defaults(func=cmd_manifest_stats)
    
    # Config
    p_config = subparsers.add_parser("config", help="View or update remote configuration")
    p_config.add_argument("--show", action="store_true", help="Show current configuration")
    p_config.add_argument("--set", nargs="+", metavar="KEY=VALUE",
                          help="Set configuration values (e.g., max_retries=5)")
    p_config.set_defaults(func=cmd_config)
    
    # Workers
    p_workers = subparsers.add_parser("workers", help="List registered workers")
    p_workers.add_argument("--active", action="store_true", help="Show only active workers")
    p_workers.add_argument("--verbose", "-v", action="store_true", help="Show detailed info")
    p_workers.set_defaults(func=cmd_workers)
    
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    
    args = parser.parse_args()
    result = args.func(args)
    sys.exit(result if result else 0)


if __name__ == "__main__":
    main()
