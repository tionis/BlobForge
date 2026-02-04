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
            # Try to convert to appropriate type
            # Boolean
            if value.lower() in ('true', 'yes', '1'):
                value = True
            elif value.lower() in ('false', 'no', '0'):
                value = False
            else:
                # Try int
                try:
                    value = int(value)
                except ValueError:
                    # Try float
                    try:
                        value = float(value)
                    except ValueError:
                        pass  # Keep as string
            updates[key] = value
        
        # Get current config and merge
        refresh_remote_config()
        config = get_remote_config()
        config.update(updates)
        
        if save_remote_config(config):
            print("Configuration updated:")
            for key, value in updates.items():
                value_type = type(value).__name__
                print(f"  {key} = {value} ({value_type})")
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
    
    print(f"{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")
    print()
    
    if not workers:
        print("  No workers found.")
        return 0
    
    # Sort by last heartbeat
    from datetime import datetime
    workers.sort(key=lambda w: w.get('last_heartbeat', ''), reverse=True)
    
    # Aggregate metrics
    total_completed = 0
    total_failed = 0
    total_bytes = 0
    
    for w in workers:
        worker_id = w.get('worker_id', '?')[:12]
        hostname = w.get('hostname', '?')
        status = w.get('status', '?')
        last_hb = w.get('last_heartbeat', '?')
        current_job = w.get('current_job')
        metrics = w.get('metrics', {})
        system = w.get('system', {})
        
        status_icon = "🟢" if status == "active" or status == "processing" else "🔴" if status == "stopped" else "⚪"
        
        # Get metrics
        jobs_completed = metrics.get('jobs_completed', 0)
        jobs_failed = metrics.get('jobs_failed', 0)
        jobs_per_hour = metrics.get('jobs_per_hour', 0)
        bytes_processed = metrics.get('bytes_processed', 0)
        avg_time = metrics.get('avg_processing_time_formatted', '-')
        
        total_completed += jobs_completed
        total_failed += jobs_failed
        total_bytes += bytes_processed
        
        # System metrics
        cpu = system.get('cpu_percent', '-')
        mem = system.get('memory_percent', '-')
        load = system.get('load_avg_1m', '-')
        
        print(f"  {status_icon} {worker_id} ({hostname})")
        
        if current_job:
            print(f"      Status: {status} - Processing: {current_job[:16]}...")
        else:
            print(f"      Status: {status}")
        
        if jobs_completed > 0 or args.verbose:
            print(f"      Jobs: {jobs_completed} completed, {jobs_failed} failed | {jobs_per_hour:.1f}/hr | Avg: {avg_time}")
        
        if cpu != '-':
            print(f"      System: CPU {cpu}%, MEM {mem}%, Load {load}")
        
        if args.verbose:
            print(f"      Platform: {w.get('platform', '?')} {w.get('platform_release', '')}")
            print(f"      Python: {w.get('python_version', '?')}")
            print(f"      CPUs: {w.get('cpu_count', '?')}, Memory: {w.get('memory_gb', '?')} GB")
            print(f"      Last heartbeat: {last_hb}")
        
        print()
    
    print(f"{'─' * 70}")
    print(f"  Total: {len(workers)} worker(s)")
    
    if total_completed > 0:
        size_str = f"{total_bytes / (1024**3):.2f} GB" if total_bytes > 1024**3 else f"{total_bytes / (1024**2):.1f} MB"
        print(f"  Aggregate: {total_completed} completed, {total_failed} failed, {size_str} processed")
    
    if args.active:
        print(f"\n  This machine's worker ID: {WORKER_ID}")
    
    return 0


def cmd_test_s3(args):
    """Test S3 endpoint capabilities."""
    import time
    import uuid
    
    print("=" * 60)
    print("BlobForge S3 Endpoint Capability Test")
    print("=" * 60)
    print()
    
    # Get S3 config info
    from .config import S3_BUCKET, S3_ENDPOINT_URL, S3_REGION, S3_PREFIX
    
    print(f"Endpoint:  {S3_ENDPOINT_URL or 'AWS S3 (default)'}")
    print(f"Bucket:    {S3_BUCKET}")
    print(f"Region:    {S3_REGION}")
    print(f"Prefix:    {S3_PREFIX or '(none)'}")
    print()
    
    # Create a test-specific prefix to avoid conflicts
    test_prefix = f"{S3_PREFIX}_blobforge_test_{uuid.uuid4().hex[:8]}"
    
    results = {
        'connectivity': None,
        'write': None,
        'read': None,
        'delete': None,
        'list': None,
        'metadata': None,
        'conditional_if_none_match': None,
        'conditional_if_match': None,
        'multipart': None,
    }
    
    s3 = S3Client(dry_run=False)
    
    if s3.mock:
        print("⚠️  Running in MOCK mode (boto3 not available)")
        print("    Install boto3 to test actual S3 connectivity:")
        print("    pip install boto3")
        return 1
    
    # Helper to print test results
    def report(name, success, detail=""):
        icon = "✅" if success else "❌"
        print(f"  {icon} {name}")
        if detail:
            print(f"      {detail}")
        return success
    
    print("-" * 60)
    print("Basic Operations")
    print("-" * 60)
    
    # Test 1: Connectivity / Write
    test_key = f"{test_prefix}/test_write.txt"
    test_content = f"BlobForge test at {time.time()}"
    try:
        s3.s3.put_object(Bucket=S3_BUCKET, Key=test_key, Body=test_content)
        results['connectivity'] = True
        results['write'] = True
        report("Connectivity", True)
        report("Write (PUT)", True)
    except Exception as e:
        results['connectivity'] = False
        results['write'] = False
        report("Connectivity", False, str(e))
        print("\n❌ Cannot proceed without basic connectivity.")
        return 1
    
    # Test 2: Read
    try:
        resp = s3.s3.get_object(Bucket=S3_BUCKET, Key=test_key)
        body = resp['Body'].read().decode('utf-8')
        if body == test_content:
            results['read'] = True
            report("Read (GET)", True)
        else:
            results['read'] = False
            report("Read (GET)", False, "Content mismatch")
    except Exception as e:
        results['read'] = False
        report("Read (GET)", False, str(e))
    
    # Test 3: List
    try:
        resp = s3.s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=test_prefix, MaxKeys=10)
        if 'Contents' in resp and len(resp['Contents']) > 0:
            results['list'] = True
            report("List (LIST)", True)
        else:
            results['list'] = False
            report("List (LIST)", False, "No objects returned")
    except Exception as e:
        results['list'] = False
        report("List (LIST)", False, str(e))
    
    # Test 4: Metadata
    try:
        meta_key = f"{test_prefix}/test_metadata.txt"
        s3.s3.put_object(
            Bucket=S3_BUCKET, 
            Key=meta_key, 
            Body="test",
            Metadata={"custom-key": "custom-value", "another": "test123"}
        )
        resp = s3.s3.head_object(Bucket=S3_BUCKET, Key=meta_key)
        meta = resp.get('Metadata', {})
        if meta.get('custom-key') == 'custom-value':
            results['metadata'] = True
            report("Custom Metadata", True)
        else:
            results['metadata'] = False
            report("Custom Metadata", False, f"Got: {meta}")
        s3.s3.delete_object(Bucket=S3_BUCKET, Key=meta_key)
    except Exception as e:
        results['metadata'] = False
        report("Custom Metadata", False, str(e))
    
    print()
    print("-" * 60)
    print("Conditional Writes (Required for Distributed Locking)")
    print("-" * 60)
    
    # Test 5: If-None-Match: * (create if not exists)
    cond_key = f"{test_prefix}/test_conditional.txt"
    try:
        # First write should succeed
        s3.s3.put_object(
            Bucket=S3_BUCKET, 
            Key=cond_key, 
            Body="first write",
            IfNoneMatch='*'
        )
        
        # Second write should fail with PreconditionFailed
        try:
            s3.s3.put_object(
                Bucket=S3_BUCKET, 
                Key=cond_key, 
                Body="second write (should fail)",
                IfNoneMatch='*'
            )
            # If we got here, If-None-Match is not enforced
            results['conditional_if_none_match'] = False
            report("If-None-Match: *", False, "Second write succeeded (should have failed)")
        except s3.ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code in ['PreconditionFailed', '412']:
                results['conditional_if_none_match'] = True
                report("If-None-Match: *", True, "PreconditionFailed correctly returned")
            else:
                results['conditional_if_none_match'] = False
                report("If-None-Match: *", False, f"Unexpected error: {error_code}")
    except s3.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code in ['NotImplemented', '501']:
            results['conditional_if_none_match'] = False
            report("If-None-Match: *", False, "Not implemented by this S3 provider")
        else:
            results['conditional_if_none_match'] = False
            report("If-None-Match: *", False, f"Error: {error_code} - {e}")
    except Exception as e:
        results['conditional_if_none_match'] = False
        report("If-None-Match: *", False, str(e))
    
    # Test 6: If-Match (ETag-based conditional update)
    etag_key = f"{test_prefix}/test_etag.txt"
    try:
        # Write initial version and get ETag
        resp = s3.s3.put_object(Bucket=S3_BUCKET, Key=etag_key, Body="version 1")
        
        # Get the ETag
        head_resp = s3.s3.head_object(Bucket=S3_BUCKET, Key=etag_key)
        etag = head_resp['ETag']
        
        # Update with correct ETag should succeed
        s3.s3.put_object(
            Bucket=S3_BUCKET, 
            Key=etag_key, 
            Body="version 2",
            IfMatch=etag
        )
        
        # Update with old ETag should fail
        try:
            s3.s3.put_object(
                Bucket=S3_BUCKET, 
                Key=etag_key, 
                Body="version 3 (should fail)",
                IfMatch=etag  # Old ETag
            )
            # If we got here, If-Match is not enforced
            results['conditional_if_match'] = False
            report("If-Match (ETag)", False, "Stale ETag update succeeded (should have failed)")
        except s3.ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code in ['PreconditionFailed', '412']:
                results['conditional_if_match'] = True
                report("If-Match (ETag)", True, "PreconditionFailed correctly returned")
            else:
                results['conditional_if_match'] = False
                report("If-Match (ETag)", False, f"Unexpected error: {error_code}")
    except s3.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code in ['NotImplemented', '501']:
            results['conditional_if_match'] = False
            report("If-Match (ETag)", False, "Not implemented by this S3 provider")
        else:
            results['conditional_if_match'] = False
            report("If-Match (ETag)", False, f"Error: {error_code} - {e}")
    except Exception as e:
        results['conditional_if_match'] = False
        report("If-Match (ETag)", False, str(e))
    
    # Test 7: Delete
    print()
    print("-" * 60)
    print("Cleanup Operations")
    print("-" * 60)
    
    try:
        # List and delete all test objects
        resp = s3.s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=test_prefix)
        if 'Contents' in resp:
            for obj in resp['Contents']:
                s3.s3.delete_object(Bucket=S3_BUCKET, Key=obj['Key'])
        results['delete'] = True
        report("Delete (DELETE)", True, f"Cleaned up {len(resp.get('Contents', []))} test objects")
    except Exception as e:
        results['delete'] = False
        report("Delete (DELETE)", False, str(e))
    
    # Summary
    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    
    basic_ok = all([results['connectivity'], results['write'], results['read'], 
                    results['list'], results['delete']])
    conditional_ok = results['conditional_if_none_match'] and results['conditional_if_match']
    
    print()
    if basic_ok and conditional_ok:
        print("✅ FULLY COMPATIBLE")
        print("   This S3 endpoint supports all BlobForge features.")
        print()
        print("   Recommended config:")
        print("     s3_supports_conditional_writes: true")
    elif basic_ok and results['conditional_if_none_match']:
        print("⚠️  PARTIALLY COMPATIBLE (no If-Match)")
        print("   Distributed locking works, but manifest updates may have race conditions.")
        print()
        print("   Recommended config:")
        print("     s3_supports_conditional_writes: true")
        print("   Note: Run only one ingestor at a time, or use --force for manifest updates.")
    elif basic_ok:
        print("✅ COMPATIBLE (with soft locking)")
        print("   Basic operations work. BlobForge will use timestamp-based soft locking")
        print("   instead of atomic conditional writes.")
        print()
        print("   Recommended config:")
        print("     blobforge config --set s3_supports_conditional_writes=false")
        print()
        print("   How soft locking works:")
        print("   - Workers write lock claims with timestamps")
        print("   - After a brief delay, the earliest timestamp wins")
        print("   - Provides probabilistic mutual exclusion (very rare collisions)")
        print()
        print("   For manifest updates:")
        print("   - Run only one ingestor at a time, OR")
        print("   - Use 'blobforge manifest --sync --force' to merge changes")
    else:
        print("❌ NOT COMPATIBLE")
        print("   Basic S3 operations failed. Check credentials and endpoint URL.")
    
    print()
    print("-" * 60)
    print("Feature Matrix:")
    print("-" * 60)
    for feature, status in results.items():
        if status is None:
            icon = "⚪"
            label = "Not tested"
        elif status:
            icon = "✅"
            label = "Supported"
        else:
            icon = "❌"
            label = "Not supported"
        print(f"  {icon} {feature:<30} {label}")
    
    return 0 if basic_ok else 1


# =============================================================================
# New CLI Commands: Logs, Watch, Download, Preview, Queue Management
# =============================================================================

def cmd_logs(args):
    """View logs for a job."""
    s3 = S3Client()
    job_hash = args.hash
    
    # List available logs
    logs = s3.list_job_logs(job_hash)
    
    if not logs:
        print(f"No logs found for job {job_hash}")
        
        # Check if job exists
        if s3.exists(f"{S3_PREFIX_DONE}/{job_hash}.zip"):
            print("(Job completed successfully - no error logs)")
        elif s3.exists(f"{S3_PREFIX_DEAD}/{job_hash}"):
            # Show dead-letter info
            data = s3.get_object_json(f"{S3_PREFIX_DEAD}/{job_hash}")
            if data:
                print(f"\nDead-letter info:")
                print(f"  Error: {data.get('error', 'Unknown')}")
                print(f"  Retries: {data.get('total_retries', '?')}")
        elif s3.exists(f"{S3_PREFIX_FAILED}/{job_hash}"):
            data = s3.get_object_json(f"{S3_PREFIX_FAILED}/{job_hash}")
            if data:
                print(f"\nFailed job info:")
                print(f"  Error: {data.get('error', 'Unknown')}")
                print(f"  Retries: {data.get('retries', 0)}")
        return 1
    
    print(f"Available logs for {job_hash}:")
    for log_type in logs:
        print(f"  - {log_type}")
    
    # Show specific log or error.json by default
    log_type = args.type or ("error" if "error" in logs else logs[0])
    
    print(f"\n--- {log_type} log ---")
    
    if log_type == "error":
        # Show structured error
        error_data = s3.get_job_error_detail(job_hash)
        if error_data:
            print(f"Timestamp: {error_data.get('timestamp', '?')}")
            print(f"Error: {error_data.get('error', '?')}")
            
            context = error_data.get('context', {})
            if context:
                print(f"\nContext:")
                for k, v in context.items():
                    print(f"  {k}: {v}")
            
            traceback_str = error_data.get('traceback')
            if traceback_str and traceback_str != 'NoneType: None\n':
                print(f"\nTraceback:")
                print(traceback_str)
        else:
            # Fall back to text log
            content = s3.get_job_log(job_hash, log_type)
            if content:
                print(content)
    else:
        content = s3.get_job_log(job_hash, log_type)
        if content:
            print(content)
        else:
            print(f"Log '{log_type}' not found")
    
    return 0


def cmd_watch(args):
    """Watch system status in real-time (simple refresh mode)."""
    import time
    import subprocess
    
    interval = args.interval
    
    print(f"Watching BlobForge status (refresh every {interval}s, Ctrl+C to stop)...")
    print()
    
    try:
        while True:
            # Clear screen
            subprocess.run(['clear'], check=False)
            
            # Show status
            status_module.show_status(verbose=args.verbose)
            
            print(f"\n[Refreshing in {interval}s... Press Ctrl+C to stop]")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nStopped watching.")
    
    return 0


def cmd_download(args):
    """Download completed job results."""
    import tempfile
    
    s3 = S3Client()
    job_hash = args.hash
    output_path = args.output
    
    # Check if job is done
    done_key = f"{S3_PREFIX_DONE}/{job_hash}.zip"
    if not s3.exists(done_key):
        print(f"Error: Job {job_hash} is not completed.")
        
        # Provide status hint
        if s3.exists(f"{S3_PREFIX_PROCESSING}/{job_hash}"):
            print("Job is currently being processed.")
        elif s3.exists(f"{S3_PREFIX_FAILED}/{job_hash}"):
            print("Job is in failed state (pending retry).")
        elif s3.exists(f"{S3_PREFIX_DEAD}/{job_hash}"):
            print("Job is in dead-letter queue.")
        else:
            # Check todo
            for p in PRIORITIES:
                if s3.exists(f"{S3_PREFIX_TODO}/{p}/{job_hash}"):
                    print(f"Job is queued (priority: {p}).")
                    break
        return 1
    
    # Determine output path
    if output_path is None:
        output_path = f"{job_hash}.zip"
    
    print(f"Downloading {job_hash}.zip to {output_path}...")
    
    try:
        s3.download_file(done_key, output_path)
        print(f"Downloaded: {output_path}")
        
        # Show file size
        import os
        size = os.path.getsize(output_path)
        print(f"Size: {size:,} bytes")
        
        return 0
    except Exception as e:
        print(f"Error downloading: {e}")
        return 1


def cmd_preview(args):
    """Preview the content of a completed job."""
    import tempfile
    import zipfile
    
    s3 = S3Client()
    job_hash = args.hash
    
    # Check if job is done
    done_key = f"{S3_PREFIX_DONE}/{job_hash}.zip"
    if not s3.exists(done_key):
        print(f"Error: Job {job_hash} is not completed.")
        return 1
    
    # Download to temp file
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = tmp.name
    
    try:
        print(f"Fetching {job_hash}...")
        s3.download_file(done_key, tmp_path)
        
        with zipfile.ZipFile(tmp_path, 'r') as zf:
            # List contents
            files = zf.namelist()
            print(f"\nContents:")
            for f in files:
                info = zf.getinfo(f)
                print(f"  {f} ({info.file_size:,} bytes)")
            
            # Show info.json if present
            if 'info.json' in files:
                print(f"\n--- info.json ---")
                with zf.open('info.json') as f:
                    info_data = json.loads(f.read().decode('utf-8'))
                    for k, v in info_data.items():
                        if k == 'marker_meta':
                            print(f"  {k}: <...>")
                        else:
                            print(f"  {k}: {v}")
            
            # Show markdown preview
            if 'content.md' in files:
                print(f"\n--- content.md (first {args.lines} lines) ---")
                with zf.open('content.md') as f:
                    content = f.read().decode('utf-8')
                    lines = content.split('\n')[:args.lines]
                    print('\n'.join(lines))
                    if len(content.split('\n')) > args.lines:
                        print(f"\n... ({len(content.split(chr(10)))} total lines)")
        
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1
    finally:
        import os
        try:
            os.unlink(tmp_path)
        except:
            pass


def cmd_retry_all(args):
    """Retry all failed or dead-letter jobs."""
    s3 = S3Client()
    
    count = 0
    
    if args.failed or not args.dead:
        # Retry failed jobs
        failed_jobs = s3.list_failed()
        for job in failed_jobs:
            key = job['Key']
            if key.endswith("/"):
                continue
            job_hash = key.split('/')[-1]
            
            if args.dry_run:
                print(f"[DRY-RUN] Would retry failed job: {job_hash}")
            else:
                # Move back to todo
                data = s3.get_object_json(key)
                retry_count = data.get('retries', 0) if data else 0
                if not args.reset_retries:
                    retry_count += 1
                
                marker = json.dumps({"retries": retry_count, "queued_at": int(__import__('time').time() * 1000)})
                s3.put_object(f"{S3_PREFIX_TODO}/{args.priority}/{job_hash}", marker)
                s3.delete_object(key)
                print(f"Retried: {job_hash}")
            count += 1
    
    if args.dead:
        # Retry dead-letter jobs
        dead_jobs = s3.list_dead()
        for job in dead_jobs:
            key = job['Key']
            if key.endswith("/"):
                continue
            job_hash = key.split('/')[-1]
            
            if args.dry_run:
                print(f"[DRY-RUN] Would retry dead job: {job_hash}")
            else:
                retry_count = 0 if args.reset_retries else 0  # Dead jobs always reset
                marker = json.dumps({"retries": retry_count, "queued_at": int(__import__('time').time() * 1000)})
                s3.put_object(f"{S3_PREFIX_TODO}/{args.priority}/{job_hash}", marker)
                s3.delete_object(key)
                print(f"Retried (from dead): {job_hash}")
            count += 1
    
    if args.dry_run:
        print(f"\n[DRY-RUN] Would retry {count} jobs")
    else:
        print(f"\nRetried {count} jobs")
    
    return 0


def cmd_clear_dead(args):
    """Clear the dead-letter queue."""
    s3 = S3Client()
    
    dead_jobs = s3.list_dead()
    dead_jobs = [j for j in dead_jobs if not j['Key'].endswith("/")]
    
    if not dead_jobs:
        print("Dead-letter queue is empty.")
        return 0
    
    print(f"Found {len(dead_jobs)} jobs in dead-letter queue.")
    
    if not args.force:
        confirm = input("Delete all? This cannot be undone. [y/N] ").strip().lower()
        if confirm != 'y':
            print("Aborted.")
            return 1
    
    for job in dead_jobs:
        key = job['Key']
        job_hash = key.split('/')[-1]
        
        if args.dry_run:
            print(f"[DRY-RUN] Would delete: {job_hash}")
        else:
            s3.delete_object(key)
            print(f"Deleted: {job_hash}")
    
    if not args.dry_run:
        print(f"\nCleared {len(dead_jobs)} jobs from dead-letter queue.")
    
    return 0


def cmd_search_queue(args):
    """Search queue by filename pattern."""
    s3 = S3Client()
    query = args.query.lower()
    
    results = []
    
    # Search todo queues
    for prio in PRIORITIES:
        keys = s3.list_keys(f"{S3_PREFIX_TODO}/{prio}/")
        for key in keys:
            job_hash = key.split('/')[-1]
            # Get manifest entry to find filename
            entry = s3.lookup_by_hash(job_hash)
            if entry:
                paths = entry.get('paths', [])
                for path in paths:
                    if query in path.lower():
                        results.append({
                            'hash': job_hash,
                            'path': path,
                            'status': 'queued',
                            'priority': prio
                        })
                        break
    
    # Search processing
    for job in s3.list_processing():
        key = job['Key']
        if key.endswith("/"):
            continue
        job_hash = key.split('/')[-1]
        entry = s3.lookup_by_hash(job_hash)
        if entry:
            paths = entry.get('paths', [])
            for path in paths:
                if query in path.lower():
                    results.append({
                        'hash': job_hash,
                        'path': path,
                        'status': 'processing'
                    })
                    break
    
    if not results:
        print(f"No jobs found matching '{args.query}'")
        return 1
    
    print(f"Found {len(results)} jobs matching '{args.query}':\n")
    for r in results[:args.limit]:
        status = r.get('status', '?')
        prio = r.get('priority', '')
        prio_str = f" [{prio}]" if prio else ""
        print(f"  [{status.upper()}{prio_str}] {r['hash'][:16]}... - {r['path']}")
    
    if len(results) > args.limit:
        print(f"\n... and {len(results) - args.limit} more")
    
    return 0


def cmd_cancel(args):
    """Cancel a running job (move back to queue)."""
    s3 = S3Client()
    job_hash = args.hash
    
    # Check if job is processing
    if not s3.exists(f"{S3_PREFIX_PROCESSING}/{job_hash}"):
        print(f"Error: Job {job_hash} is not currently processing.")
        return 1
    
    lock_data = s3.get_lock_info(job_hash)
    priority = lock_data.get('priority', DEFAULT_PRIORITY) if lock_data else DEFAULT_PRIORITY
    
    if args.priority:
        priority = args.priority
    
    print(f"Cancelling job {job_hash}...")
    print(f"  Current worker: {lock_data.get('worker', '?') if lock_data else '?'}")
    print(f"  Moving to: {priority}")
    
    if args.dry_run:
        print("[DRY-RUN] Would cancel job")
        return 0
    
    # Move back to todo
    s3.move_to_todo(job_hash, priority, increment_retry=False)
    s3.release_lock(job_hash)
    
    print("Job cancelled and re-queued.")
    print("Note: The worker may still be processing. It will fail on completion.")
    
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
    
    # Test S3
    p_test_s3 = subparsers.add_parser("test-s3", help="Test S3 endpoint capabilities")
    p_test_s3.set_defaults(func=cmd_test_s3)
    
    # =========================================================================
    # New Commands: Logs, Watch, Download, Preview, Queue Management
    # =========================================================================
    
    # Logs
    p_logs = subparsers.add_parser("logs", help="View logs for a job")
    p_logs.add_argument("hash", help="SHA256 hash of the PDF")
    p_logs.add_argument("--type", "-t", help="Log type to view (default: error)")
    p_logs.set_defaults(func=cmd_logs)
    
    # Watch
    p_watch = subparsers.add_parser("watch", help="Watch system status in real-time")
    p_watch.add_argument("--interval", "-i", type=int, default=10, help="Refresh interval in seconds")
    p_watch.add_argument("--verbose", "-v", action="store_true", help="Show detailed info")
    p_watch.set_defaults(func=cmd_watch)
    
    # Download
    p_download = subparsers.add_parser("download", help="Download completed job results")
    p_download.add_argument("hash", help="SHA256 hash of the PDF")
    p_download.add_argument("--output", "-o", help="Output path (default: <hash>.zip)")
    p_download.set_defaults(func=cmd_download)
    
    # Preview
    p_preview = subparsers.add_parser("preview", help="Preview completed job content")
    p_preview.add_argument("hash", help="SHA256 hash of the PDF")
    p_preview.add_argument("--lines", "-n", type=int, default=50, help="Lines of markdown to show")
    p_preview.set_defaults(func=cmd_preview)
    
    # Retry-all
    p_retry_all = subparsers.add_parser("retry-all", help="Retry all failed/dead jobs")
    p_retry_all.add_argument("--failed", action="store_true", help="Retry failed jobs only")
    p_retry_all.add_argument("--dead", action="store_true", help="Retry dead-letter jobs only")
    p_retry_all.add_argument("--priority", default=DEFAULT_PRIORITY, choices=PRIORITIES,
                             help="Queue priority for retried jobs")
    p_retry_all.add_argument("--reset-retries", action="store_true", help="Reset retry counters")
    p_retry_all.add_argument("--dry-run", action="store_true", help="Don't make changes")
    p_retry_all.set_defaults(func=cmd_retry_all)
    
    # Clear-dead
    p_clear_dead = subparsers.add_parser("clear-dead", help="Clear the dead-letter queue")
    p_clear_dead.add_argument("--force", action="store_true", help="Skip confirmation")
    p_clear_dead.add_argument("--dry-run", action="store_true", help="Don't make changes")
    p_clear_dead.set_defaults(func=cmd_clear_dead)
    
    # Search-queue
    p_search_queue = subparsers.add_parser("search-queue", help="Search queue by filename")
    p_search_queue.add_argument("query", help="Filename pattern to search")
    p_search_queue.add_argument("--limit", type=int, default=20, help="Max results to show")
    p_search_queue.set_defaults(func=cmd_search_queue)
    
    # Cancel
    p_cancel = subparsers.add_parser("cancel", help="Cancel a running job")
    p_cancel.add_argument("hash", help="SHA256 hash of the PDF")
    p_cancel.add_argument("--priority", choices=PRIORITIES, help="Priority when re-queued")
    p_cancel.add_argument("--dry-run", action="store_true", help="Don't make changes")
    p_cancel.set_defaults(func=cmd_cancel)
    
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    
    args = parser.parse_args()
    result = args.func(args)
    sys.exit(result if result else 0)


if __name__ == "__main__":
    main()
