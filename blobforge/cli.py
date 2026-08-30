"""
BlobForge CLI - Command-line interface for managing PDF conversion jobs.

Commands:
- ingest: Scan directory and queue PDFs for processing
- hydrated: Clean or package local hydrated outputs
- status: Check status of a specific job by hash
- list: List queue statistics
- reprioritize: Change priority of a queued job
- retry: Retry a failed or dead-letter job
- janitor: Run janitor to recover stale jobs
"""
import os
import mimetypes
import secrets
import sys
import json
import argparse
from concurrent.futures import (
    FIRST_COMPLETED,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    as_completed,
    wait,
)
from pathlib import Path

from .config import (
    S3_BUCKET, S3_PREFIX, S3_PREFIX_RAW, S3_PREFIX_TODO, S3_PREFIX_PROCESSING,
    S3_PREFIX_DONE, S3_PREFIX_FAILED, S3_PREFIX_DEAD,
    PRIORITIES, DEFAULT_PRIORITY,
    WORKER_ID,
    get_stale_timeout_minutes
)
from .s3_client import S3Client
from . import ingestor
from . import status as status_module
from . import hydrator as hydrator_module
from . import hydrated_outputs
from . import legacy_migration
from .converters import run_converter
from .corpus import build_manifest, pdf_pages
from .evaluation import compare as compare_artifacts
from .local_import import import_legacy_sources, import_stage
from .mdaf import blake3_bytes
from .mdaf.digest import canonical_json_bytes
from .review import build_review_bundle, summarize_review_result
from .reprocessing import reprocess_mdaf
from .routing import RoutingFeatures, route_pdf
from .coordinator_client import CoordinatorClient, CoordinatorError
from .utils import rewrite_asset_paths, utc_now_iso


COORDINATOR_PRIORITIES = ("1_urgent", "2_high", "3_normal", "4_low")


def _coordinator_client():
    client = CoordinatorClient()
    return client if client.available else None


def _apply_coordinator_overrides(args):
    """Apply optional command-line coordinator credentials for this process."""
    url = getattr(args, "coordinator_url", None)
    token = getattr(args, "token", None)
    if bool(url) != bool(token):
        print("Error: --coordinator-url and --token must be provided together")
        return False
    if url:
        os.environ["BLOBFORGE_COORDINATOR_URL"] = url
        os.environ["BLOBFORGE_COORDINATOR_TOKEN"] = token
    return True


def _recipe_digest_arg(value):
    digest = value.lower()
    hexadecimal = digest.removeprefix("blake3:")
    if (
        (":" in digest and not digest.startswith("blake3:"))
        or len(hexadecimal) != 64
        or any(char not in "0123456789abcdef" for char in hexadecimal)
    ):
        raise argparse.ArgumentTypeError(
            "recipe digest must be 64 hexadecimal characters, optionally prefixed with blake3:"
        )
    return digest


def _require_management_ui(action):
    print(
        f"'{action}' is managed by the Bunny coordinator. "
        "Use its authenticated management UI."
    )
    return True


def cmd_ingest(args):
    """Ingest PDFs from files or directories."""
    if not _apply_coordinator_overrides(args):
        return 1
    if len(args.paths) == 1:
        print(f"Ingesting {args.paths[0]} with priority {args.priority}...")
    else:
        print(f"Ingesting {len(args.paths)} paths with priority {args.priority}...")
    ingestor.ingest(args.paths, priority=args.priority, dry_run=args.dry_run)


def _upload_files(paths):
    """Resolve explicit files and recursively discover PDFs in directories."""
    files = []
    seen = set()
    for raw in paths:
        path = Path(raw).expanduser()
        candidates = (
            sorted(
                candidate
                for candidate in path.rglob("*")
                if candidate.is_file() and candidate.suffix.lower() == ".pdf"
            )
            if path.is_dir()
            else [path]
        )
        for candidate in candidates:
            if not candidate.is_file():
                raise ValueError(f"upload path is not a file or directory: {candidate}")
            resolved = candidate.resolve()
            if resolved not in seen:
                files.append(candidate)
                seen.add(resolved)
    if not files:
        raise ValueError("no files found; directories are searched recursively for PDFs")
    return files


def _upload_recipe(coordinator, selector, media_type):
    recipes = [
        recipe for recipe in coordinator.list_recipes(media_type)
        if recipe.get("enabled")
        and int(recipe.get("worker_count") or 0) > 0
        and "source" in recipe.get("input_kinds", ["source"])
    ]
    normalized = selector.lower()
    if normalized.startswith("blake3:") or (
        len(normalized) == 64
        and all(char in "0123456789abcdef" for char in normalized)
    ):
        digest = _recipe_digest_arg(normalized)
        tagged = digest if digest.startswith("blake3:") else f"blake3:{digest}"
        matches = [
            recipe for recipe in recipes
            if recipe.get("recipe_digest") in {digest, tagged}
        ]
    else:
        matches = [
            recipe for recipe in recipes
            if normalized in {
                str(recipe.get("backend") or "").lower(),
                str(recipe.get("display_name") or "").lower(),
            }
        ]
    choices = ", ".join(
        f"{item.get('backend')} ({item.get('recipe_digest')})" for item in recipes
    ) or "none"
    if not matches:
        raise ValueError(
            f"recipe {selector!r} is not available for {media_type}; "
            f"available: {choices}"
        )
    if len(matches) != 1:
        raise ValueError(
            f"recipe selector {selector!r} is ambiguous; use an exact digest. "
            f"Available: {choices}"
        )
    return str(matches[0]["recipe_digest"])


def cmd_upload(args):
    """Stream local sources to the self-hosted coordinator and queue them."""
    if not _apply_coordinator_overrides(args):
        return 1
    coordinator = _coordinator_client()
    if not coordinator:
        print("Error: BLOBFORGE_COORDINATOR_URL and BLOBFORGE_COORDINATOR_TOKEN are required")
        return 1
    coordinator.timeout = args.timeout
    files = _upload_files(args.paths)
    tags = list(dict.fromkeys(
        tag.strip() for value in args.tag for tag in value.split(",") if tag.strip()
    ))
    outcomes = []
    recipe_cache = {}
    failures = 0
    for path in files:
        media_type = (
            args.media_type
            or mimetypes.guess_type(path.name)[0]
            or "application/octet-stream"
        )
        recipe_digest = None
        if not args.unassigned:
            key = (args.recipe, media_type)
            if key not in recipe_cache:
                recipe_cache[key] = _upload_recipe(
                    coordinator, args.recipe, media_type
                )
            recipe_digest = recipe_cache[key]
        item = {
            "path": str(path),
            "media_type": media_type,
            "priority": args.priority,
            "tags": tags,
            "recipe_digest": recipe_digest,
        }
        if args.dry_run:
            item["status"] = "planned"
        else:
            try:
                job = coordinator.upload_admin_source(
                    str(path), filename=path.name, media_type=media_type,
                    priority=args.priority, tags=tags,
                    recipe_digest=recipe_digest,
                )
                item.update({
                    "status": str(job.get("status") or "unknown"),
                    "hash": str(job.get("hash") or ""),
                })
            except (CoordinatorError, OSError, ValueError) as exc:
                failures += 1
                item.update({"status": "error", "error": str(exc)})
        outcomes.append(item)
        if not args.json:
            recipe = recipe_digest or "unassigned"
            if item["status"] == "error":
                print(f"ERROR {path}: {item['error']}")
            elif args.dry_run:
                print(f"PLAN  {path} -> {recipe} ({args.priority})")
            else:
                print(
                    f"OK    {path} -> {item['hash']} [{item['status']}] "
                    f"via {recipe}"
                )
    if args.json:
        print(json.dumps({"files": outcomes, "failed": failures}, indent=2))
    elif not args.dry_run:
        print(f"Uploaded {len(outcomes) - failures}/{len(outcomes)} source(s).")
    return 1 if failures else 0


def cmd_cleanup_legacy(args):
    """Preview or delete obsolete S3 queue and registry objects."""
    prefixes = [f"{S3_PREFIX}queue/", f"{S3_PREFIX}registry/"]
    execute = bool(args.execute)
    if execute and not args.yes:
        print(f"This permanently deletes all objects under these prefixes in {S3_BUCKET}:")
        for prefix in prefixes:
            print(f"  {prefix}")
        if input("Type DELETE to continue: ").strip() != "DELETE":
            print("Cancelled.")
            return 1

    s3 = S3Client()
    total = 0
    deleted = 0
    for prefix in prefixes:
        result = s3.purge_prefix(prefix, dry_run=not execute)
        total += result["count"]
        deleted += result["deleted"]
        verb = "Deleted" if execute else "Found"
        print(f"{verb} {result['deleted'] if execute else result['count']} object(s) under {prefix}")
        if not execute:
            for key in result["preview"]:
                print(f"  {key}")
    if execute:
        print(f"Deleted {deleted} legacy object(s). Raw PDFs, outputs, and backups were untouched.")
    else:
        print(f"Dry run: {total} legacy object(s) would be deleted. Re-run with --execute.")
    return 0


def cmd_reprioritize(args):
    """Change the priority of a queued job (managed by the coordinator UI)."""
    _require_management_ui("reprioritize")
    return 1


def cmd_status(args):
    """Check the status of a specific job."""
    coordinator = _coordinator_client()
    if coordinator:
        try:
            job = coordinator.get_job(args.hash)
        except CoordinatorError as exc:
            print(f"Status lookup failed: {exc}", file=sys.stderr)
            return 1
        print(f"Status: {str(job.get('status', 'unknown')).upper()}")
        print(f"Priority: {job.get('priority', '?')}")
        if job.get("original_name"):
            print(f"File: {job['original_name']}")
        if job.get("worker_id"):
            print(f"Worker: {job['worker_id']}")
        if job.get("error_message"):
            print(f"Error: {job['error_message']}")
        print(f"Retries: {job.get('retry_count', 0)}/{job.get('max_retries', '?')}")
        return 0
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
    if _coordinator_client():
        status_module.show_status(verbose=args.verbose)
        return 0
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
    """Retry a failed or dead-letter job (managed by the coordinator UI)."""
    _require_management_ui("retry")
    return 1


def cmd_convert(args):
    """Convert a PDF file locally (offline)."""
    import time

    from .conversion_identity import (
        conversion_recipe_digest,
        current_conversion_provenance,
        current_conversion_recipe,
    )
    from .utils import compute_sha256_with_cache
    
    input_path = args.path
    output_dir = args.output
    
    if not os.path.exists(input_path):
        print(f"Error: File '{input_path}' not found.")
        return 1
    
    if not output_dir:
        base_name = os.path.splitext(os.path.basename(input_path))[0]
        output_dir = os.path.join(os.getcwd(), base_name)
    
    os.makedirs(output_dir, exist_ok=True)
    assets_dir = os.path.join(output_dir, "assets")
    os.makedirs(assets_dir, exist_ok=True)
    
    print(f"Converting '{input_path}'...")
    print(f"Output directory: {output_dir}")
    
    try:
        from marker.models import create_model_dict
        from marker.converters.pdf import PdfConverter
        from marker.output import text_from_rendered
    except ImportError:
        print("Error: marker-pdf not installed. Install with: pip install marker-pdf")
        return 1
    
    start_time = time.time()
    
    print("Loading models...")
    model_dict = create_model_dict()
    converter = PdfConverter(
        artifact_dict=model_dict,
        config={}
    )
    
    print("Processing PDF...")
    rendered = converter(input_path)
    text, _, images = text_from_rendered(rendered)
    
    # Update image paths in markdown
    text = rewrite_asset_paths(text, images.keys())
    
    # Save markdown
    md_path = os.path.join(output_dir, "content.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(text)
    
    # Save images
    for img_name, img in images.items():
        img_path = os.path.join(assets_dir, img_name)
        if hasattr(img, 'mode') and img.mode != "RGB":
            img = img.convert("RGB")
        img.save(img_path)
    
    # Save metadata
    conversion_recipe = current_conversion_recipe()
    meta = {
        "document_hash": compute_sha256_with_cache(input_path),
        "conversion_recipe_digest": conversion_recipe_digest(conversion_recipe),
        "conversion_recipe": conversion_recipe,
        "conversion_provenance": current_conversion_provenance(conversion_recipe),
        "converted_at": utc_now_iso(),
        "original_filename": os.path.basename(input_path),
        "processing_time_seconds": round(time.time() - start_time, 2),
    }
    
    # Extract marker metadata
    if hasattr(rendered, 'metadata') and rendered.metadata:
        try:
            if hasattr(rendered.metadata, 'model_dump'):
                meta['marker_meta'] = rendered.metadata.model_dump()
            elif hasattr(rendered.metadata, 'dict'):
                meta['marker_meta'] = rendered.metadata.dict()
            elif isinstance(rendered.metadata, dict):
                meta['marker_meta'] = rendered.metadata
        except Exception:
            pass
            
    with open(os.path.join(output_dir, "info.json"), "w") as f:
        json.dump(meta, f, indent=2)
    
    print(f"Conversion complete in {meta['processing_time_seconds']}s.")
    print(f"Markdown: {md_path}")
    print(f"Images: {len(images)} saved to {assets_dir}")
    
    return 0


def cmd_hydrate(args):
    """Hydrate local markdown/assets from completed conversion outputs."""
    if not _apply_coordinator_overrides(args):
        return 1
    if len(args.paths) == 1:
        print(f"Hydrating conversions for {args.paths[0]}...")
    else:
        print(f"Hydrating conversions for {len(args.paths)} paths...")
    coordinator = _coordinator_client()
    return hydrator_module.hydrate(
        args.paths,
        force=args.force,
        dry_run=args.dry_run,
        client=coordinator,
        refresh_status=args.refresh_status,
    )


def cmd_hydrated_clean(args):
    """Preview or remove hydrated Markdown/assets next to PDFs."""
    return hydrated_outputs.clean(args.paths, execute=args.execute)


def cmd_hydrated_textpack(args):
    """Preview or replace hydrated outputs with TextPack archives."""
    return hydrated_outputs.textpack(
        args.paths,
        execute=args.execute,
        force=args.force,
    )


def cmd_hydrated_clean_textpacks(args):
    """Preview or remove TextPacks next to PDFs."""
    return hydrated_outputs.clean_textpacks(args.paths, execute=args.execute)


def cmd_hydrated_unpack(args):
    """Preview or restore TextPacks to hydrated Markdown/assets."""
    return hydrated_outputs.unpack(
        args.paths,
        execute=args.execute,
        force=args.force,
    )


def cmd_migrate_inventory(args):
    """Index the read-only local mirror in the resumable migration catalog."""
    summary = legacy_migration.inventory(args.workspace)
    print(f"Sources:          {summary.sources:,}")
    print(f"Legacy artifacts: {summary.legacy_artifacts:,}")
    print(f"Paired:           {summary.paired:,}")
    print(f"Converted:        {summary.converted:,}")
    print(f"Failed:           {summary.failed:,}")
    return 0


def cmd_migrate_legacy(args):
    """Convert paired legacy ZIPs into locally validated MDAF artifacts."""
    legacy_migration.inventory(args.workspace)
    _run_lock = legacy_migration.acquire_enrichment_run_lock(args.workspace)
    recovered_attempts = legacy_migration.recover_interrupted_enrichment_attempts(
        args.workspace
    )
    if recovered_attempts:
        print(f"Recovered {recovered_attempts} interrupted enrichment attempt(s).")
    hashes = [args.hash] if args.hash else legacy_migration.pending_hashes(
        args.workspace, args.limit
    )
    if not hashes:
        print("No pending paired legacy artifacts.")
        return 0
    failures = 0
    if args.jobs == 1:
        for index, digest in enumerate(hashes, 1):
            try:
                output = legacy_migration.convert_one(digest, args.workspace)
                print(f"[{index}/{len(hashes)}] {digest} -> {output}")
            except Exception as exc:
                failures += 1
                print(f"[{index}/{len(hashes)}] {digest}: ERROR: {exc}", file=sys.stderr)
                if args.fail_fast:
                    break
    else:
        with ThreadPoolExecutor(max_workers=args.jobs) as executor:
            futures = {
                executor.submit(legacy_migration.convert_one, digest, args.workspace): digest
                for digest in hashes
            }
            for index, future in enumerate(as_completed(futures), 1):
                digest = futures[future]
                try:
                    output = future.result()
                    print(f"[{index}/{len(hashes)}] {digest} -> {output}")
                except Exception as exc:
                    failures += 1
                    print(f"[{index}/{len(hashes)}] {digest}: ERROR: {exc}", file=sys.stderr)
                    if args.fail_fast:
                        for pending in futures:
                            pending.cancel()
                        break
    print(f"Converted {len(hashes) - failures:,}; failed {failures:,}")
    return 1 if failures else 0


def cmd_migrate_enrich(args):
    """Build derived MDAFs with PDF-backed Markdown source mappings."""
    if not args.hashes and args.limit is None and not args.all:
        print(
            "Refusing an unbounded enrichment run; provide a hash, --limit for a canary, "
            "or --all after canary approval.",
            file=sys.stderr,
        )
        return 2
    legacy_migration.inventory(args.workspace)
    if args.hashes:
        hashes = list(dict.fromkeys(args.hashes))
    else:
        _, hashes = legacy_migration.pending_enrichment_hashes(
            args.workspace, None if args.all else args.limit
        )
    if not hashes:
        print("No pending legacy enrichments for the current recipe.")
        return 0
    failures = 0
    completed = 0
    if args.jobs == 1:
        for index, digest in enumerate(hashes, 1):
            try:
                output = legacy_migration.enrich_one(digest, args.workspace)
                completed += 1
                print(f"[{index}/{len(hashes)}] {digest} -> {output}")
            except Exception as exc:
                failures += 1
                print(f"[{index}/{len(hashes)}] {digest}: ERROR: {exc}", file=sys.stderr)
                if args.fail_fast:
                    break
    else:
        items = legacy_migration.enrichment_work_items(
            hashes,
            args.workspace,
            large_pages=args.large_pages,
            large_bytes=int(args.large_mib * 1024 * 1024),
        )
        large_count = sum(item.large for item in items)
        print(
            f"Size-aware schedule: {large_count} large, {len(items) - large_count} ordinary; "
            "at most one large document will run at once."
        )
        pending_items = items
        active = {}
        stopped = False
        reported = 0
        with ProcessPoolExecutor(max_workers=args.jobs) as executor:
            while (pending_items or active) and not stopped:
                slots = args.jobs - len(active)
                selected, pending_items = legacy_migration.select_enrichment_work_items(
                    pending_items,
                    slots,
                    large_running=any(item.large for item in active.values()),
                )
                for item in selected:
                    future = executor.submit(
                        legacy_migration.enrich_one,
                        item.legacy_sha256,
                        args.workspace,
                    )
                    active[future] = item
                if not active:
                    raise RuntimeError("size-aware enrichment scheduler made no progress")
                finished, _ = wait(active, return_when=FIRST_COMPLETED)
                for future in finished:
                    item = active.pop(future)
                    reported += 1
                    try:
                        output = future.result()
                        completed += 1
                        print(
                            f"[{reported}/{len(hashes)}] {item.legacy_sha256} -> {output} "
                            f"({item.pages} pages, {item.source_bytes / 1024**2:.1f} MiB)"
                        )
                    except Exception as exc:
                        failures += 1
                        print(
                            f"[{reported}/{len(hashes)}] {item.legacy_sha256}: ERROR: {exc}",
                            file=sys.stderr,
                        )
                        if args.fail_fast:
                            stopped = True
                            pending_items.clear()
                            for running in active:
                                running.cancel()
                            break
    summary = legacy_migration.enrichment_summary(args.workspace)
    print(
        f"Enriched {completed:,}; failed {failures:,}; "
        f"recipe total {summary.converted:,}/{summary.eligible:,}"
    )
    return 1 if failures else 0


def cmd_migrate_enrich_status(args):
    summary = legacy_migration.enrichment_summary(args.workspace)
    print(f"Recipe:        {summary.recipe_digest}")
    print(f"Eligible:      {summary.eligible:,}")
    print(f"Pending:       {summary.pending:,}")
    print(f"Processing:    {summary.processing:,}")
    print(f"Converted:     {summary.converted:,}")
    print(f"Failed:        {summary.failed:,}")
    block_coverage = summary.mapped_blocks / summary.total_blocks if summary.total_blocks else 0
    byte_coverage = summary.mapped_bytes / summary.total_bytes if summary.total_bytes else 0
    print(
        f"Block coverage: {summary.mapped_blocks:,}/{summary.total_blocks:,} "
        f"({block_coverage:.1%})"
    )
    print(
        f"Byte coverage:  {summary.mapped_bytes:,}/{summary.total_bytes:,} "
        f"({byte_coverage:.1%})"
    )
    if summary.measured_documents:
        pages_per_hour = (
            summary.measured_pages * 3600 / summary.elapsed_seconds
            if summary.elapsed_seconds
            else 0
        )
        print(
            f"Telemetry:      {summary.measured_documents:,}/{summary.converted:,} documents, "
            f"{summary.measured_pages:,} pages, {summary.elapsed_seconds:.1f} process-seconds"
        )
        print(
            f"Resources:      {summary.peak_rss_bytes / 1024**2:.1f} MiB max peak RSS, "
            f"{summary.output_bytes / 1024**2:.1f} MiB outputs, "
            f"{pages_per_hour:,.1f} pages/process-hour"
        )
    else:
        print("Telemetry:      no instrumented enrichment attempts yet")
    return 0


def cmd_migrate_enrich_verify(args):
    result = legacy_migration.verify_enrichments(args.workspace, args.limit)
    print(f"Checked: {result.checked:,}")
    print(f"Valid:   {result.valid:,}")
    print(f"Invalid: {len(result.errors):,}")
    for error in result.errors:
        print(f"ERROR: {error}", file=sys.stderr)
    return 1 if result.errors else 0


def cmd_migrate_report(args):
    path = legacy_migration.export_manifest(args.workspace, args.output)
    summary = legacy_migration.inventory(args.workspace)
    print(f"Manifest:  {path}")
    print(f"Converted: {summary.converted:,}/{summary.paired:,}")
    print(f"Failed:    {summary.failed:,}")
    return 1 if summary.failed else 0


def cmd_migrate_verify(args):
    result = legacy_migration.verify_outputs(args.workspace, args.limit)
    print(f"Checked: {result.checked:,}")
    print(f"Valid:   {result.valid:,}")
    print(f"Invalid: {len(result.errors):,}")
    for error in result.errors:
        print(f"ERROR: {error}", file=sys.stderr)
    return 1 if result.errors else 0


def cmd_migrate_stage(args):
    result = legacy_migration.stage_v2(args.workspace, args.output, args.run_id)
    print(f"Stage:     {result.root}")
    print(f"Sources:   {result.sources:,}")
    print(f"Artifacts: {result.artifacts:,}")
    print(f"Recipe:    {result.recipe_digest}")
    return 0


def cmd_migrate_import_local(args):
    summary = import_stage(
        args.stage,
        args.data_dir,
        run_id=args.run_id,
        dry_run=not args.execute,
    )
    mode = "validated" if not args.execute else "imported"
    print(
        f"Local backend stage {mode}: checked={summary.checked}, "
        f"imported={summary.imported}, skipped={summary.skipped}"
    )
    if not args.execute:
        print("Dry run only; pass --execute to write local server state.")
    return 0


def cmd_migrate_import_sources(args):
    summary = import_legacy_sources(
        args.workspace,
        args.data_dir,
        dry_run=not args.execute,
    )
    mode = "validated" if not args.execute else "imported"
    print(
        f"Legacy sources {mode}: checked={summary.checked}, "
        f"imported={summary.imported}, skipped={summary.skipped}"
    )
    if not args.execute:
        print("Dry run only; pass --execute to write missing sources and jobs.")
    return 0


def cmd_evaluate_converter(args):
    """Run one isolated converter adapter and emit a validated MDAF."""
    repository = Path(__file__).resolve().parent.parent
    provider_engine = {
        "mistral-wiki": "mistral",
        "mistral-wiki-v2": "mistral",
        "mistral-wiki-v3": "mistral",
        "datalab-wiki": "datalab",
    }.get(args.engine, args.engine)
    project = repository / "evaluators" / provider_engine
    adapter = project / "adapter.py"
    output = Path(args.output) if args.output else Path(args.path).with_suffix(
        f".{args.engine}.mdaf"
    )
    parameters = {
        "do_ocr": not args.no_ocr,
        "do_table_structure": not args.no_tables,
        "generate_picture_images": not args.no_images,
        "extract_images": not args.no_images,
        "max_pages": args.max_pages,
        "max_cost_usd": args.max_cost_usd,
        "model": args.model,
    }
    environment = None
    embedded_recipe = None
    if args.engine in {
        "mistral",
        "mistral-wiki",
        "mistral-wiki-v2",
        "mistral-wiki-v3",
    }:
        raw_recipe_path = (
            repository / "blobforge" / "recipes" / "mistral-ocr-4.1-v1.json"
        )
        raw_recipe = json.loads(raw_recipe_path.read_text(encoding="utf-8"))
        recipe_path = (
            repository
            / "blobforge"
            / "recipes"
            / (
                "mistral-ocr-4.1-wiki-v3.json"
                if args.engine == "mistral-wiki-v3"
                else (
                    "mistral-ocr-4.1-wiki-v2.json"
                    if args.engine == "mistral-wiki-v2"
                    else (
                        "mistral-ocr-4.1-wiki-v1.json"
                        if args.engine == "mistral-wiki"
                        else "mistral-ocr-4.1-v1.json"
                    )
                )
            )
        )
        recipe = json.loads(recipe_path.read_text(encoding="utf-8"))
        embedded_recipe = recipe if recipe.get("schema", "").endswith("/v3") else None
        parameters["recipe_digest"] = blake3_bytes(canonical_json_bytes(recipe))
        if args.engine in {"mistral-wiki", "mistral-wiki-v2", "mistral-wiki-v3"}:
            parameters["provider_request_digest"] = blake3_bytes(
                canonical_json_bytes(raw_recipe)
            )
            parameters["normalization_profile"] = (
                "wiki-v2"
                if args.engine in {"mistral-wiki-v2", "mistral-wiki-v3"}
                else "wiki-v1"
            )
        parameters["api_rights_confirmed"] = args.confirm_api_rights
        response_cache = Path(
            args.response_cache
            or os.environ.get("BLOBFORGE_MISTRAL_RESPONSE_CACHE")
            or Path.home() / ".cache" / "blobforge" / "mistral-responses"
        ).expanduser()
        environment = {"BLOBFORGE_MISTRAL_RESPONSE_CACHE": str(response_cache)}
        if args.plan:
            pages = pdf_pages(args.path)
            estimated_cost = pages * 0.004
            page_limit_ok = args.max_pages is not None and args.max_pages >= pages
            cost_limit_ok = (
                args.max_cost_usd is not None
                and args.max_cost_usd >= estimated_cost
            )
            credential_configured = bool(os.environ.get("MISTRAL_API_KEY"))
            print(f"Source:       {Path(args.path).resolve()}")
            print(f"Pages:        {pages:,}")
            print(f"Bytes:        {Path(args.path).stat().st_size:,}")
            print(f"Recipe:       {parameters['recipe_digest']}")
            if args.engine in {"mistral-wiki", "mistral-wiki-v2", "mistral-wiki-v3"}:
                print(f"Provider key: {parameters['provider_request_digest']}")
            print(f"List price:   ${estimated_cost:.3f}")
            print(f"Page ceiling: {args.max_pages if args.max_pages is not None else 'missing'}")
            print(
                f"Cost ceiling: "
                f"{f'${args.max_cost_usd:.3f}' if args.max_cost_usd is not None else 'missing'}"
            )
            print(f"Cache:        {response_cache}")
            print(
                "Credential:   "
                + ("configured" if credential_configured else "not configured")
            )
            print(
                "API rights:   "
                + ("confirmed" if args.confirm_api_rights else "not confirmed")
            )
            ready = (
                page_limit_ok
                and cost_limit_ok
                and credential_configured
                and args.confirm_api_rights
            )
            print(f"Ready:        {'yes' if ready else 'no'}")
            print("No provider request was made.")
            return 0
        if not args.confirm_api_rights:
            print(
                "Refusing Mistral upload without --confirm-api-rights.",
                file=sys.stderr,
            )
            return 2
    elif args.engine in {"datalab", "datalab-wiki"}:
        raw_recipe_path = (
            repository
            / "blobforge"
            / "recipes"
            / "datalab-convert-accurate-v1.json"
        )
        raw_recipe = json.loads(raw_recipe_path.read_text(encoding="utf-8"))
        recipe_path = (
            repository
            / "blobforge"
            / "recipes"
            / (
                "datalab-convert-accurate-wiki-v1.json"
                if args.engine == "datalab-wiki"
                else "datalab-convert-accurate-v1.json"
            )
        )
        recipe = json.loads(recipe_path.read_text(encoding="utf-8"))
        parameters["recipe_digest"] = blake3_bytes(canonical_json_bytes(recipe))
        if args.engine == "datalab-wiki":
            parameters["provider_request_digest"] = blake3_bytes(
                canonical_json_bytes(raw_recipe)
            )
            parameters["normalization_profile"] = "wiki-v1"
        parameters["api_rights_confirmed"] = args.confirm_api_rights
        parameters["mode"] = "accurate"
        response_cache = Path(
            args.response_cache
            or os.environ.get("BLOBFORGE_DATALAB_RESPONSE_CACHE")
            or Path.home() / ".cache" / "blobforge" / "datalab-responses"
        ).expanduser()
        environment = {"BLOBFORGE_DATALAB_RESPONSE_CACHE": str(response_cache)}
        if args.plan:
            pages = pdf_pages(args.path)
            source_bytes = Path(args.path).stat().st_size
            page_limit_ok = (
                args.max_pages is not None
                and args.max_pages >= pages
                and pages <= 7_000
            )
            size_limit_ok = source_bytes <= 200_000_000
            cost_limit_ok = args.max_cost_usd is not None and args.max_cost_usd > 0
            credential_configured = bool(os.environ.get("DATALAB_API_KEY"))
            print(f"Source:       {Path(args.path).resolve()}")
            print(f"Pages:        {pages:,}")
            print(f"Bytes:        {source_bytes:,}")
            print(f"Recipe:       {parameters['recipe_digest']}")
            if args.engine == "datalab-wiki":
                print(f"Provider key: {parameters['provider_request_digest']}")
            print("Mode:         accurate")
            print("Price:        provider-returned after conversion")
            print(f"Page ceiling: {args.max_pages if args.max_pages is not None else 'missing'}")
            print(
                f"Cost ceiling: "
                f"{f'${args.max_cost_usd:.3f}' if args.max_cost_usd is not None else 'missing'}"
            )
            print(f"Cache:        {response_cache}")
            print(
                "Credential:   "
                + ("configured" if credential_configured else "not configured")
            )
            print(
                "API rights:   "
                + ("confirmed" if args.confirm_api_rights else "not confirmed")
            )
            ready = (
                page_limit_ok
                and size_limit_ok
                and cost_limit_ok
                and credential_configured
                and args.confirm_api_rights
            )
            print(f"Ready:        {'yes' if ready else 'no'}")
            print(
                "The API has no preflight quote; the returned charge is checked "
                "against the ceiling."
            )
            print("No provider request was made.")
            return 0
        if not args.confirm_api_rights:
            print(
                "Refusing Datalab upload without --confirm-api-rights.",
                file=sys.stderr,
            )
            return 2
    elif args.plan:
        print("--plan is supported only for hosted evaluators.", file=sys.stderr)
        return 2
    result = run_converter(
        ["uv", "run", "--project", str(project), "python", str(adapter)],
        args.path,
        output,
        parameters=parameters,
        recipe=embedded_recipe,
        timeout_seconds=args.timeout,
        environment=environment,
    )
    print(f"Artifact: {result.artifact_path}")
    print(f"Identity: {result.identity}")
    print(f"Elapsed:  {result.elapsed_seconds:.1f}s")
    for diagnostic in result.diagnostics:
        print(f"{diagnostic.get('severity', 'info')}: {diagnostic.get('message', diagnostic)}")
    return 0


def cmd_route_plan(args):
    """Evaluate the versioned PDF rulebook routing policy without mutating jobs."""
    path = Path(args.path)
    decision = route_pdf(
        RoutingFeatures(
            media_type="application/pdf",
            source_class="born-digital-pnp-rulebook",
            page_count=pdf_pages(path),
            native_text_ratio=args.native_text_ratio,
            language=args.language,
            quality_tier=args.quality_tier,
            layout_class=args.layout_class,
            complex_tables=args.complex_tables,
            equations=args.equations,
            external_processing_allowed=args.confirm_api_rights,
            max_cost_usd=args.max_cost_usd,
        ),
        allow_canary=args.allow_canary,
        recipe_override=args.recipe_override,
    )
    print(json.dumps(decision.as_json(), ensure_ascii=False, indent=2, sort_keys=True))
    if args.apply_job:
        if not decision.eligible:
            return 2
        if not _apply_coordinator_overrides(args):
            return 1
        coordinator = _coordinator_client()
        if coordinator is None:
            print("Error: coordinator URL and token are required", file=sys.stderr)
            return 1
        request_body = dict(decision.features)
        request_body.update(
            {
                "allow_canary": args.allow_canary,
                "recipe_override": args.recipe_override,
            }
        )
        applied = coordinator.route_conversion(args.apply_job, request_body)
        print(json.dumps({"applied": applied}, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if decision.eligible else 2


def cmd_corpus_inventory(args):
    """Freeze an evaluation corpus with BLAKE3/SHA-256/page metadata."""
    result = build_manifest(args.path, args.output)
    print(f"Manifest:  {result.path}")
    print(f"Identity:  {result.digest}")
    print(f"Documents: {result.documents:,}")
    print(f"Pages:     {result.pages:,}")
    print(f"Bytes:     {result.bytes:,}")
    return 0


def cmd_compare_mdaf(args):
    metrics = compare_artifacts(args.artifacts, args.output)
    columns = ("text_bytes", "words", "headings", "table_rows", "assets", "mappings", "mapped_pages")
    print("artifact\t" + "\t".join(columns))
    for item in metrics:
        print(Path(item.path).name + "\t" + "\t".join(str(getattr(item, column)) for column in columns))
    if args.output:
        print(f"JSON: {args.output}")
    return 0


def cmd_reprocess_mdaf(args):
    """Create an immutable post-processing derivative from retained evidence."""
    result = reprocess_mdaf(
        args.parent,
        args.recipe,
        args.output,
        recipe_root=args.recipe_root,
    )
    print(f"Artifact:      {result.path}")
    print(f"Identity:      {result.identity}")
    print(f"Derived from:  {result.parent_identity}")
    print(f"Source recipe: {result.source_recipe_digest}")
    print(f"Target recipe: {result.target_recipe_digest}")
    print(f"Normalization: {dict(result.normalization_stats or {})}")
    print("Provider calls: none")
    return 0


def cmd_reprocess_plan(args):
    """Plan or queue coordinator-native artifact-input upgrades."""
    if not _apply_coordinator_overrides(args):
        return 1
    coordinator = _coordinator_client()
    if coordinator is None:
        print("Error: coordinator URL and token are required", file=sys.stderr)
        return 1
    result = coordinator.plan_reprocessing(
        target_recipe_digest=args.target_recipe,
        source_recipe_digest=args.source_recipe,
        source_keys=args.source_key,
        execute=args.execute,
        priority=args.priority,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    if not args.execute:
        print("Preview only; pass --execute to queue eligible derivatives.")
    return 0


def cmd_review_bundle(args):
    """Build a blinded, source-backed browser review from comparable MDAFs."""
    seed = secrets.token_hex(32) if args.random_seed else args.seed
    result = build_review_bundle(
        args.source,
        args.artifacts,
        args.output,
        pages=args.pages,
        seed=seed,
        key_output=args.key_output,
    )
    print(f"Review:     {result.root / 'index.html'}")
    print(f"Key:        {result.key_path}")
    print(f"Campaign:   {result.campaign_digest}")
    print(f"Candidates: {result.artifacts}")
    print(f"Pages:      {result.pages}")
    return 0


def cmd_review_summarize(args):
    """Validate, unblind, and summarize a browser-exported review result."""
    summary = summarize_review_result(args.result, args.key)
    serialized = json.dumps(summary, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("x", encoding="utf-8") as destination:
            destination.write(serialized)
        print(f"Summary: {output.resolve()}")
    else:
        print(serialized, end="")
    return 0


def cmd_janitor(args):
    """Run the janitor to recover stale jobs (managed by the coordinator UI)."""
    _require_management_ui("janitor")
    return 1


def cmd_worker(args):
    """Start a worker to process jobs."""
    from . import worker as worker_module
    
    try:
        run_schedule = (
            worker_module.WorkerSchedule.from_specs(
                args.run_window,
                abort_running=args.abort_outside_window
            )
            if args.run_window else None
        )
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1

    coordinator_url = args.coordinator_url or os.getenv("BLOBFORGE_COORDINATOR_URL", "")
    coordinator_token = args.token or os.getenv("BLOBFORGE_COORDINATOR_TOKEN", "")
    if not coordinator_url or not coordinator_token:
        print("Error: a coordinator URL and enrolled worker token are required")
        return 1
    if coordinator_url:
        os.environ["BLOBFORGE_COORDINATOR_URL"] = coordinator_url
        os.environ["BLOBFORGE_COORDINATOR_TOKEN"] = coordinator_token
    coordinator = CoordinatorClient(coordinator_url, coordinator_token)
    isolate_conversion = args.isolate_conversion or args.abort_outside_window
    w = worker_module.Worker(None, isolate_conversion=isolate_conversion, coordinator_client=coordinator)
    return worker_module.run_worker_loop(
        w,
        run_once=args.run_once,
        idle_sleep=10,
        run_schedule=run_schedule
    )


def cmd_recipe_worker(args):
    """Start the exact-recipe, isolated MDAF worker."""
    from .recipe_runtime import datalab_wiki_v1_recipe, mistral_wiki_v3_recipe
    from .recipe_worker import RecipeWorker

    coordinator_url = args.coordinator_url or os.getenv("BLOBFORGE_COORDINATOR_URL", "")
    coordinator_token = args.token or os.getenv("BLOBFORGE_COORDINATOR_TOKEN", "")
    if not coordinator_url or not coordinator_token:
        print("Error: a coordinator URL and enrolled worker token are required")
        return 1
    if not args.confirm_api_rights:
        print("Error: hosted recipe workers require --confirm-api-rights")
        return 1
    key_name = "MISTRAL_API_KEY" if args.provider == "mistral" else "DATALAB_API_KEY"
    if not args.cache_only and not os.getenv(key_name):
        print(f"Error: {key_name} is required unless --cache-only is selected")
        return 1
    try:
        factory = (
            mistral_wiki_v3_recipe
            if args.provider == "mistral"
            else datalab_wiki_v1_recipe
        )
        provider_account = args.provider_account or f"{args.provider}:primary"
        response_cache = args.response_cache or str(
            Path.home() / ".cache" / "blobforge" / f"{args.provider}-responses"
        )
        recipes = [factory(
            max_pages=args.max_pages,
            max_cost_usd=args.max_cost_usd,
            response_cache=response_cache,
            api_rights_confirmed=args.confirm_api_rights,
            cache_only=args.cache_only,
            provider_account=provider_account,
            billing_currency=args.billing_currency,
        )]
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"Error: {exc}")
        return 1
    worker = RecipeWorker(
        CoordinatorClient(coordinator_url, coordinator_token),
        recipes,
        timeout_seconds=args.timeout,
        heartbeat_interval=args.heartbeat_interval,
    )
    return worker.run(run_once=args.run_once, idle_sleep=args.idle_sleep)


def cmd_serve(args):
    """Run the self-hosted SQLite/filesystem coordinator."""
    try:
        import uvicorn
        from .server.app import create_app
    except ImportError:
        print('Error: install the server dependencies with `uv sync --extra server`')
        return 1
    uvicorn.run(create_app(), host=args.host, port=args.port, log_level=args.log_level)
    return 0


def cmd_dashboard(args):
    """Show system status dashboard."""
    if not _apply_coordinator_overrides(args):
        return 1
    status_module.show_status(verbose=args.verbose)


def cmd_config(args):
    """Read coordinator configuration; mutations belong in the Web UI."""
    coordinator = _coordinator_client()
    if not coordinator:
        print("Error: coordinator URL and token are required")
        return 1
    print("--- Coordinator Configuration ---")
    for key, value in sorted(coordinator.get_config().items()):
        print(f"  {key}: {value}")
    return 0


def cmd_workers(args):
    """List registered workers."""
    if not _apply_coordinator_overrides(args):
        return 1
    coordinator = _coordinator_client()
    stale_timeout = get_stale_timeout_minutes()
    if not coordinator:
        print("Error: coordinator URL and token are required")
        return 1
    workers = coordinator.snapshot().get("workers", [])
    workers = [
        {
            **(worker.get("metadata") or {}),
            **worker,
            "metrics": worker.get("metrics") or {},
            "system": (worker.get("metrics") or {}).get("system", {}),
        }
        for worker in workers
    ]
    if args.active:
        cutoff = __import__("time").time() * 1000 - stale_timeout * 60 * 1000
        workers = [worker for worker in workers if float(worker.get("last_heartbeat") or 0) >= cutoff and worker.get("status") not in {"stopped", "stale"}]
        title = f"Active Workers (coordinator heartbeat < {stale_timeout}m ago)"
    else:
        title = "All Coordinator Workers"
    
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
        state_detail = w.get('state') or {}
        metrics = w.get('metrics', {})
        system = w.get('system', {})
        
        status_icon = "🟢" if status in {"active", "processing", "idle"} else "🟠" if status == "suspended" else "🔴" if status == "stopped" else "⚪"
        
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
        elif status == "suspended" and state_detail.get("until"):
            until = datetime.fromtimestamp(float(state_detail["until"]) / 1000).astimezone()
            print(f"      Status: suspended ({state_detail.get('reason', 'run condition')}, until {until.isoformat(timespec='minutes')})")
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
            print(f"      Recipe: {w.get('conversion_recipe_digest', 'unavailable')}")
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
        print("   Conditional object creation works, but conditional replacement does not.")
        print()
        print("   Recommended config:")
        print("     s3_supports_conditional_writes: true")
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
    if not _apply_coordinator_overrides(args):
        return 1
    coordinator = _coordinator_client()
    if not coordinator:
        print("Error: BLOBFORGE_COORDINATOR_URL and BLOBFORGE_COORDINATOR_TOKEN are required")
        return 1
    job_hash = args.hash
    output_path = args.output
    recipe_digest = args.recipe_digest

    job = coordinator.get_job(job_hash)
    if not job:
        print(f"Error: Job {job_hash} does not exist.")
        return 1
    if not recipe_digest and job.get("status") != "done":
        print(f"Error: Job {job_hash} is not completed.")
        print(f"Job is in state: {job.get('status')}")
        return 1

    if output_path is None:
        suffix = f".{recipe_digest[:12]}" if recipe_digest else ""
        output_path = f"{job_hash}{suffix}.zip"

    selected = f" recipe {recipe_digest}" if recipe_digest else " selected recipe"
    print(f"Downloading {job_hash}{selected} to {output_path}...")

    try:
        coordinator.download_output(job_hash, output_path, recipe_digest)
        print(f"Downloaded: {output_path}")
        print(f"Size: {os.path.getsize(output_path):,} bytes")
        return 0
    except Exception as e:
        print(f"Error downloading: {e}")
        return 1


def cmd_preview(args):
    """Preview the content of a completed job."""
    import tempfile
    import zipfile

    if not _apply_coordinator_overrides(args):
        return 1
    coordinator = _coordinator_client()
    if not coordinator:
        print("Error: BLOBFORGE_COORDINATOR_URL and BLOBFORGE_COORDINATOR_TOKEN are required")
        return 1
    job_hash = args.hash
    recipe_digest = args.recipe_digest

    job = coordinator.get_job(job_hash)
    if not job:
        print(f"Error: Job {job_hash} does not exist.")
        return 1
    if not recipe_digest and job.get("status") != "done":
        print(f"Error: Job {job_hash} is not completed.")
        return 1

    # Download to temp file
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        print(f"Fetching {job_hash}...")
        coordinator.download_output(job_hash, tmp_path, recipe_digest)
        
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


def cmd_artifacts(args):
    """List retained conversion artifacts for a source document."""
    from datetime import datetime

    if not _apply_coordinator_overrides(args):
        return 1
    coordinator = _coordinator_client()
    if not coordinator:
        print("Error: BLOBFORGE_COORDINATOR_URL and BLOBFORGE_COORDINATOR_TOKEN are required")
        return 1
    job = coordinator.get_job(args.hash)
    if not job:
        print(f"Error: Job {args.hash} does not exist.")
        return 1
    artifacts = coordinator.list_artifacts(args.hash)
    if args.json:
        print(json.dumps({"hash": args.hash, "selected_recipe_digest": job.get("recipe_digest"), "artifacts": artifacts}, indent=2, sort_keys=True))
        return 0
    if not artifacts:
        print(f"No retained conversion artifacts for {args.hash}.")
        return 0

    selected = job.get("recipe_digest")
    if selected is None and job.get("status") == "done":
        selected = "0" * 64
    print(f"Conversion artifacts for {args.hash}:")
    for artifact in artifacts:
        digest = str(artifact.get("recipe_digest") or "")
        provenance = artifact.get("provenance") or {}
        packages = provenance.get("packages") or {}
        recipe = artifact.get("recipe") or {}
        created_ms = artifact.get("created_at")
        created = "unknown"
        if isinstance(created_ms, (int, float)) and created_ms:
            created = datetime.fromtimestamp(created_ms / 1000).astimezone().isoformat(timespec="seconds")
        flags = []
        if digest == selected:
            flags.append("selected")
        if artifact.get("legacy") or digest == "0" * 64:
            flags.append("legacy")
        label = f" ({', '.join(flags)})" if flags else ""
        print(f"  {digest}{label}")
        print(f"    Created: {created}")
        engine = recipe.get("engine") or artifact.get("converter_backend") or "unknown"
        engine_generation = recipe.get("engine_generation") or ""
        converter_version = packages.get("marker-pdf") or artifact.get("converter_version") or "unknown"
        print(f"    Engine: {engine} {engine_generation}".rstrip())
        print(f"    Converter version: {converter_version}")
        print(f"    Worker: {artifact.get('worker_id') or 'unknown'}")
        print(f"    Size: {int(artifact.get('output_size_bytes') or 0):,} bytes")
    return 0


def cmd_request_conversion(args):
    """Select an existing recipe artifact or queue that recipe for conversion."""
    if not _apply_coordinator_overrides(args):
        return 1
    coordinator = _coordinator_client()
    if not coordinator:
        print("Error: BLOBFORGE_COORDINATOR_URL and BLOBFORGE_COORDINATOR_TOKEN are required")
        return 1
    recipe_digest = getattr(args, "recipe_digest", None)
    backend = getattr(args, "backend", None)
    if bool(recipe_digest) == bool(backend):
        print("Error: specify exactly one recipe digest or --backend")
        return 1
    artifacts = coordinator.list_artifacts(args.hash)
    existing = any(
        artifact.get("recipe_digest") == recipe_digest for artifact in artifacts
    ) if recipe_digest else False
    selector = recipe_digest or f"backend {backend}"
    action = "select retained artifact" if existing else "queue conversion"
    if args.dry_run:
        print(f"Would {action} {selector} for {args.hash}.")
        return 0
    if backend:
        outcome = coordinator.request_conversion(args.hash, backend=backend)
    else:
        outcome = coordinator.request_conversion(args.hash, recipe_digest)
    print(f"Coordinator will {action}: {outcome.get('status', 'unknown')}.")
    return 0


def cmd_retry_all(args):
    """Retry all failed or dead-letter jobs (managed by the coordinator UI)."""
    _require_management_ui("retry-all")
    return 1


def cmd_clear_dead(args):
    """Clear the dead-letter queue (managed by the coordinator UI)."""
    _require_management_ui("clear-dead")
    return 1


def cmd_cancel(args):
    """Cancel a running job (managed by the coordinator UI)."""
    _require_management_ui("cancel")
    return 1


def main():
    parser = argparse.ArgumentParser(
        prog="blobforge",
        description="BlobForge - content conversion and artifact orchestration"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Ingest
    p_ingest = subparsers.add_parser("ingest", help="Ingest PDF files or directories")
    p_ingest.add_argument("paths", nargs='+', help="PDF files or directories to ingest (supports shell globbing)")
    p_ingest.add_argument("--priority", default=DEFAULT_PRIORITY, choices=PRIORITIES,
                          help="Queue priority for new jobs")
    p_ingest.add_argument("--dry-run", action="store_true", help="Don't make changes")
    p_ingest.add_argument("--coordinator-url", help="Coordinator base URL")
    p_ingest.add_argument("--token", help="Admin token for the coordinator")
    p_ingest.set_defaults(func=cmd_ingest)

    p_upload = subparsers.add_parser(
        "upload", help="Upload files to the self-hosted coordinator and queue them"
    )
    p_upload.add_argument(
        "paths", nargs="+",
        help="Files or directories; directories are searched recursively for PDFs",
    )
    p_upload.add_argument(
        "--priority", default="3_normal", choices=COORDINATOR_PRIORITIES
    )
    p_upload.add_argument(
        "--tag", action="append", default=[],
        help="Tag to attach; repeat it or provide comma-separated tags",
    )
    p_upload.add_argument(
        "--media-type", help="Override media type for every selected file"
    )
    assignment = p_upload.add_mutually_exclusive_group(required=True)
    assignment.add_argument(
        "--recipe",
        help="Exact digest, or an unambiguous active backend/display name",
    )
    assignment.add_argument(
        "--unassigned", action="store_true",
        help="Queue without a recipe; hosted workers will not claim the job",
    )
    p_upload.add_argument("--dry-run", action="store_true")
    p_upload.add_argument(
        "--json", action="store_true", help="Emit machine-readable results"
    )
    p_upload.add_argument(
        "--timeout", type=float, default=600.0,
        help="Per-socket upload timeout in seconds",
    )
    p_upload.add_argument("--coordinator-url", help="Coordinator base URL")
    p_upload.add_argument("--token", help="Revocable admin token")
    p_upload.set_defaults(func=cmd_upload)

    p_cleanup = subparsers.add_parser(
        "cleanup-legacy",
        help="Remove obsolete S3 queue and registry objects",
    )
    p_cleanup.add_argument("--execute", action="store_true", help="Delete objects; the default is a dry run")
    p_cleanup.add_argument("--yes", action="store_true", help="Skip the DELETE confirmation when used with --execute")
    p_cleanup.set_defaults(func=cmd_cleanup_legacy)
    
    # Convert (local)
    p_convert = subparsers.add_parser("convert", help="Convert a PDF file locally (offline)")
    p_convert.add_argument("path", help="Path to the PDF file")
    p_convert.add_argument("--output", "-o", help="Output directory (default: current_dir/filename)")
    p_convert.set_defaults(func=cmd_convert)

    # Hydrate local markdown/assets from completed conversions
    p_hydrate = subparsers.add_parser("hydrate", help="Hydrate local markdown/assets from completed conversions")
    p_hydrate.add_argument("paths", nargs='+', help="PDF files or directories to hydrate")
    p_hydrate.add_argument("--force", action="store_true", help="Overwrite existing markdown/assets")
    p_hydrate.add_argument("--dry-run", action="store_true", help="Preview changes without writing files")
    p_hydrate.add_argument("--refresh-status", action="store_true",
                           help="Rebuild the local done-set mirror from scratch, re-syncing every hash")
    p_hydrate.add_argument("--coordinator-url", help="Coordinator base URL")
    p_hydrate.add_argument("--token", help="Admin token for the coordinator")
    p_hydrate.set_defaults(func=cmd_hydrate)

    # Local maintenance for outputs previously created by hydrate.
    p_hydrated = subparsers.add_parser(
        "hydrated",
        help="Maintain hydrated Markdown/assets next to PDFs",
    )
    hydrated_subparsers = p_hydrated.add_subparsers(
        dest="hydrated_command", required=True
    )
    p_hydrated_clean = hydrated_subparsers.add_parser(
        "clean",
        help="Find and remove hydrated Markdown/assets (dry run by default)",
    )
    p_hydrated_clean.add_argument(
        "paths", nargs="+", help="PDF files or directories to scan recursively"
    )
    p_hydrated_clean.add_argument(
        "--execute", action="store_true", help="Remove the discovered outputs"
    )
    p_hydrated_clean.set_defaults(func=cmd_hydrated_clean)

    p_hydrated_textpack = hydrated_subparsers.add_parser(
        "textpack",
        help="Replace hydrated outputs with .textpack archives (dry run by default)",
    )
    p_hydrated_textpack.add_argument(
        "paths", nargs="+", help="PDF files or directories to scan recursively"
    )
    p_hydrated_textpack.add_argument(
        "--execute", action="store_true", help="Create archives and remove source outputs"
    )
    p_hydrated_textpack.add_argument(
        "--force", action="store_true", help="Overwrite existing .textpack files"
    )
    p_hydrated_textpack.set_defaults(func=cmd_hydrated_textpack)

    p_hydrated_clean_textpacks = hydrated_subparsers.add_parser(
        "clean-textpacks",
        help="Find and remove .textpack files (dry run by default)",
    )
    p_hydrated_clean_textpacks.add_argument(
        "paths", nargs="+", help="PDF files or directories to scan recursively"
    )
    p_hydrated_clean_textpacks.add_argument(
        "--execute", action="store_true", help="Remove the discovered TextPacks"
    )
    p_hydrated_clean_textpacks.set_defaults(func=cmd_hydrated_clean_textpacks)

    p_hydrated_unpack = hydrated_subparsers.add_parser(
        "unpack",
        help="Restore .textpack files to Markdown/assets (dry run by default)",
    )
    p_hydrated_unpack.add_argument(
        "paths", nargs="+", help="PDF files or directories to scan recursively"
    )
    p_hydrated_unpack.add_argument(
        "--execute", action="store_true", help="Restore outputs and remove each TextPack"
    )
    p_hydrated_unpack.add_argument(
        "--force", action="store_true", help="Overwrite existing Markdown/assets"
    )
    p_hydrated_unpack.set_defaults(func=cmd_hydrated_unpack)

    p_migrate = subparsers.add_parser(
        "migrate", help="Build and inspect the local legacy-to-MDAF migration"
    )
    migrate_subparsers = p_migrate.add_subparsers(
        dest="migration_command", required=True
    )
    p_migrate_inventory = migrate_subparsers.add_parser(
        "inventory", help="Index the read-only rclone mirror"
    )
    p_migrate_inventory.add_argument(
        "--workspace", default=str(legacy_migration.DEFAULT_WORKSPACE)
    )
    p_migrate_inventory.set_defaults(func=cmd_migrate_inventory)

    p_migrate_legacy = migrate_subparsers.add_parser(
        "legacy", help="Convert paired legacy ZIP/PDF inputs to local MDAF"
    )
    p_migrate_legacy.add_argument("hash", nargs="?", help="One legacy SHA-256")
    p_migrate_legacy.add_argument(
        "--workspace", default=str(legacy_migration.DEFAULT_WORKSPACE)
    )
    p_migrate_legacy.add_argument("--limit", type=int, help="Maximum pending artifacts")
    p_migrate_legacy.add_argument(
        "--jobs", type=int, choices=range(1, 5), default=1,
        help="Concurrent local conversions (default: 1; use 2 for bounded bulk migration)",
    )
    p_migrate_legacy.add_argument("--fail-fast", action="store_true")
    p_migrate_legacy.set_defaults(func=cmd_migrate_legacy)
    p_migrate_enrich = migrate_subparsers.add_parser(
        "enrich", help="Derive PDF-aligned MDAFs from converted legacy artifacts"
    )
    p_migrate_enrich.add_argument(
        "hashes", nargs="*", metavar="HASH", help="One or more legacy SHA-256 values"
    )
    p_migrate_enrich.add_argument(
        "--workspace", default=str(legacy_migration.DEFAULT_WORKSPACE)
    )
    p_migrate_enrich.add_argument("--limit", type=int, help="Bounded canary size")
    p_migrate_enrich.add_argument(
        "--all", action="store_true", help="Process the complete pending backfill"
    )
    p_migrate_enrich.add_argument(
        "--jobs", type=int, choices=range(1, 5), default=1,
        help="Isolated enrichment processes (default: 1)",
    )
    p_migrate_enrich.add_argument(
        "--large-pages",
        type=int,
        default=legacy_migration.DEFAULT_LARGE_PDF_PAGES,
        help="Treat PDFs with at least this many pages as large (default: 300)",
    )
    p_migrate_enrich.add_argument(
        "--large-mib",
        type=float,
        default=legacy_migration.DEFAULT_LARGE_PDF_BYTES / 1024**2,
        help="Treat PDFs at least this large as large (default: 64 MiB)",
    )
    p_migrate_enrich.add_argument("--fail-fast", action="store_true")
    p_migrate_enrich.set_defaults(func=cmd_migrate_enrich)
    p_migrate_enrich_status = migrate_subparsers.add_parser(
        "enrich-status", help="Show current enrichment recipe and coverage"
    )
    p_migrate_enrich_status.add_argument(
        "--workspace", default=str(legacy_migration.DEFAULT_WORKSPACE)
    )
    p_migrate_enrich_status.set_defaults(func=cmd_migrate_enrich_status)
    p_migrate_enrich_verify = migrate_subparsers.add_parser(
        "enrich-verify", help="Validate enriched derivatives and catalog lineage"
    )
    p_migrate_enrich_verify.add_argument(
        "--workspace", default=str(legacy_migration.DEFAULT_WORKSPACE)
    )
    p_migrate_enrich_verify.add_argument("--limit", type=int)
    p_migrate_enrich_verify.set_defaults(func=cmd_migrate_enrich_verify)
    p_migrate_report = migrate_subparsers.add_parser(
        "report", help="Export a checksummed migration manifest"
    )
    p_migrate_report.add_argument(
        "--workspace", default=str(legacy_migration.DEFAULT_WORKSPACE)
    )
    p_migrate_report.add_argument("--output")
    p_migrate_report.set_defaults(func=cmd_migrate_report)
    p_migrate_verify = migrate_subparsers.add_parser(
        "verify", help="Validate generated MDAFs and cross-check the catalog"
    )
    p_migrate_verify.add_argument(
        "--workspace", default=str(legacy_migration.DEFAULT_WORKSPACE)
    )
    p_migrate_verify.add_argument("--limit", type=int)
    p_migrate_verify.set_defaults(func=cmd_migrate_verify)
    p_migrate_stage = migrate_subparsers.add_parser(
        "stage", help="Build a verified local tree using the proposed S3 v2 keys"
    )
    p_migrate_stage.add_argument(
        "--workspace", default=str(legacy_migration.DEFAULT_WORKSPACE)
    )
    p_migrate_stage.add_argument("--output")
    p_migrate_stage.add_argument("--run-id", default="legacy-mdaf-v1")
    p_migrate_stage.set_defaults(func=cmd_migrate_stage)
    p_migrate_import = migrate_subparsers.add_parser(
        "import-local", help="Import a verified v2 stage into local server storage"
    )
    p_migrate_import.add_argument("--stage", required=True)
    p_migrate_import.add_argument("--data-dir", required=True)
    p_migrate_import.add_argument("--run-id", default="legacy-mdaf-v1")
    p_migrate_import.add_argument(
        "--execute", action="store_true", help="Write objects and SQLite rows"
    )
    p_migrate_import.set_defaults(func=cmd_migrate_import_local)
    p_migrate_sources = migrate_subparsers.add_parser(
        "import-legacy-sources",
        help="Import and queue raw sources absent from the MDAF stage",
    )
    p_migrate_sources.add_argument(
        "--workspace", default=str(legacy_migration.DEFAULT_WORKSPACE)
    )
    p_migrate_sources.add_argument("--data-dir", required=True)
    p_migrate_sources.add_argument(
        "--execute", action="store_true", help="Write missing objects and SQLite rows"
    )
    p_migrate_sources.set_defaults(func=cmd_migrate_import_sources)

    p_evaluate = subparsers.add_parser(
        "evaluate", help="Run an isolated converter and package a comparable MDAF"
    )
    p_evaluate.add_argument(
        "engine",
        choices=(
            "poppler",
            "marker1",
            "marker2",
            "docling",
            "mistral",
            "mistral-wiki",
            "mistral-wiki-v2",
            "mistral-wiki-v3",
            "datalab",
            "datalab-wiki",
        ),
    )
    p_evaluate.add_argument("path", help="Source PDF")
    p_evaluate.add_argument("--output", "-o", help="Destination .mdaf")
    p_evaluate.add_argument("--timeout", type=int, default=86_400)
    p_evaluate.add_argument("--no-ocr", action="store_true")
    p_evaluate.add_argument("--no-tables", action="store_true")
    p_evaluate.add_argument("--no-images", action="store_true")
    p_evaluate.add_argument("--max-pages", type=int, help="Hard API page ceiling")
    p_evaluate.add_argument("--max-cost-usd", type=float, help="Hard API list-price ceiling")
    p_evaluate.add_argument("--model", help="Explicit provider model identifier")
    p_evaluate.add_argument(
        "--response-cache",
        help="Durable hosted-provider response cache",
    )
    p_evaluate.add_argument(
        "--plan",
        action="store_true",
        help="Print hosted-provider limits and readiness without an API call",
    )
    p_evaluate.add_argument(
        "--confirm-api-rights",
        action="store_true",
        help="Confirm the source may be submitted to the selected hosted API",
    )
    p_evaluate.set_defaults(func=cmd_evaluate_converter)

    p_route = subparsers.add_parser(
        "route-plan",
        help="Resolve a born-digital rulebook to an exact recipe without changing jobs",
    )
    p_route.add_argument("path", help="Source PDF")
    p_route.add_argument("--language", default="und", help="BCP 47 primary language")
    p_route.add_argument(
        "--native-text-ratio",
        type=float,
        default=1.0,
        help="Measured fraction of pages with usable native text",
    )
    p_route.add_argument("--quality-tier", default="quality")
    p_route.add_argument("--layout-class", default="standard")
    p_route.add_argument("--complex-tables", action="store_true")
    p_route.add_argument("--equations", action="store_true")
    p_route.add_argument("--max-cost-usd", type=float)
    p_route.add_argument("--recipe-override", help="Exact tagged recipe digest")
    p_route.add_argument("--apply-job", help="Apply and audit the decision for this source key")
    p_route.add_argument("--coordinator-url")
    p_route.add_argument("--token")
    p_route.add_argument(
        "--confirm-api-rights",
        action="store_true",
        help="Confirm this source may be sent to the hosted candidate",
    )
    p_route.add_argument(
        "--allow-canary",
        action="store_true",
        help="Allow a candidate that has not passed the production holdout gate",
    )
    p_route.set_defaults(func=cmd_route_plan)

    p_corpus = subparsers.add_parser(
        "corpus", help="Create reproducible converter-evaluation corpora"
    )
    corpus_subparsers = p_corpus.add_subparsers(dest="corpus_command", required=True)
    p_corpus_inventory = corpus_subparsers.add_parser(
        "inventory", help="Hash PDFs and record page counts"
    )
    p_corpus_inventory.add_argument("path", help="Corpus directory")
    p_corpus_inventory.add_argument("--output", "-o", required=True)
    p_corpus_inventory.set_defaults(func=cmd_corpus_inventory)

    p_compare = subparsers.add_parser(
        "compare-mdaf", help="Measure comparable structural signals in MDAFs"
    )
    p_compare.add_argument("artifacts", nargs="+")
    p_compare.add_argument("--output", "-o")
    p_compare.set_defaults(func=cmd_compare_mdaf)

    p_reprocess = subparsers.add_parser(
        "reprocess",
        help="Upgrade an MDAF from retained native evidence without rerunning extraction",
    )
    p_reprocess.add_argument("parent", help="Existing parent .mdaf")
    p_reprocess.add_argument("--recipe", required=True, help="Target lifecycle recipe JSON")
    p_reprocess.add_argument("--output", "-o", required=True, help="New derivative .mdaf")
    p_reprocess.add_argument(
        "--recipe-root",
        help="Immutable recipe registry used only when the parent predates embedded recipes",
    )
    p_reprocess.set_defaults(func=cmd_reprocess_mdaf)

    p_reprocess_plan = subparsers.add_parser(
        "reprocess-plan",
        help="Plan or queue coordinator upgrades from existing MDAF artifacts",
    )
    p_reprocess_plan.add_argument("--source-recipe", required=True)
    p_reprocess_plan.add_argument("--target-recipe", required=True)
    p_reprocess_plan.add_argument(
        "--source-key",
        action="append",
        help="Limit to one source key; repeat to select multiple sources",
    )
    p_reprocess_plan.add_argument(
        "--priority", choices=("1_urgent", "2_high", "3_normal", "4_low")
    )
    p_reprocess_plan.add_argument(
        "--execute", action="store_true", help="Queue the eligible derivatives"
    )
    p_reprocess_plan.add_argument("--coordinator-url")
    p_reprocess_plan.add_argument("--token")
    p_reprocess_plan.set_defaults(func=cmd_reprocess_plan)

    p_review = subparsers.add_parser(
        "review-bundle", help="Build a blinded page-by-page MDAF review bundle"
    )
    p_review.add_argument("source", help="Source PDF shared by every artifact")
    p_review.add_argument("artifacts", nargs="+", help="Two or more comparable MDAFs")
    p_review.add_argument("--output", "-o", required=True, help="New review directory")
    p_review.add_argument(
        "--pages", help="One-based pages/ranges to review, for example 1,3-5"
    )
    seed_options = p_review.add_mutually_exclusive_group()
    seed_options.add_argument(
        "--seed", default="blobforge-review-v2", help="Private label-shuffle seed"
    )
    seed_options.add_argument(
        "--random-seed",
        action="store_true",
        help="Generate a private random label seed stored only in the key",
    )
    p_review.add_argument(
        "--key-output", help="Private candidate-to-engine key JSON destination"
    )
    p_review.set_defaults(func=cmd_review_bundle)

    p_review_summary = subparsers.add_parser(
        "review-summarize",
        help="Validate and unblind an exported review result",
    )
    p_review_summary.add_argument("result", help="Browser-exported review JSON")
    p_review_summary.add_argument("--key", required=True, help="Private campaign key")
    p_review_summary.add_argument("--output", "-o", help="New summary JSON destination")
    p_review_summary.set_defaults(func=cmd_review_summarize)
    
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
    p_worker.add_argument("--run-once", action="store_true", help="Process one job and exit")
    p_worker.add_argument(
        "--run-window",
        action="append",
        default=[],
        help="Local-time run window HH:MM-HH:MM. May be repeated or comma-separated."
    )
    p_worker.add_argument(
        "--abort-outside-window",
        action="store_true",
        help="Abort and requeue active conversions when a run window closes."
    )
    p_worker.add_argument(
        "--isolate-conversion",
        action="store_true",
        help="Run marker conversion in a child process so native crashes do not kill the worker."
    )
    p_worker.add_argument(
        "--coordinator-url",
        help="Coordinator base URL. Can also be set with BLOBFORGE_COORDINATOR_URL."
    )
    p_worker.add_argument(
        "--token",
        help="Worker enrollment token created in the management UI. Can also be set with BLOBFORGE_COORDINATOR_TOKEN."
    )
    p_worker.set_defaults(func=cmd_worker)

    p_recipe_worker = subparsers.add_parser(
        "recipe-worker",
        help="Start an isolated exact-recipe MDAF worker (canary)",
    )
    p_recipe_worker.add_argument("--run-once", action="store_true")
    p_recipe_worker.add_argument(
        "--provider", choices=("mistral", "datalab"), default="mistral"
    )
    p_recipe_worker.add_argument("--coordinator-url")
    p_recipe_worker.add_argument("--token")
    p_recipe_worker.add_argument("--max-pages", type=int, required=True)
    p_recipe_worker.add_argument("--max-cost-usd", type=float, required=True)
    p_recipe_worker.add_argument(
        "--billing-currency",
        default="USD",
        help="ISO currency for quota amounts (legacy field names still contain micro_usd)",
    )
    p_recipe_worker.add_argument("--timeout", type=int, default=86_400)
    p_recipe_worker.add_argument("--heartbeat-interval", type=float, default=30.0)
    p_recipe_worker.add_argument("--idle-sleep", type=float, default=10.0)
    p_recipe_worker.add_argument(
        "--response-cache",
        default=None,
        help="Durable provider response cache (provider-specific default when omitted)",
    )
    p_recipe_worker.add_argument("--confirm-api-rights", action="store_true")
    p_recipe_worker.add_argument(
        "--provider-account",
        default=os.environ.get("BLOBFORGE_PROVIDER_ACCOUNT") or None,
        help="Logical coordinator quota account; never contains credentials",
    )
    p_recipe_worker.add_argument(
        "--cache-only",
        action="store_true",
        help="Never contact the provider; fail jobs whose response is not cached",
    )
    p_recipe_worker.set_defaults(func=cmd_recipe_worker)

    p_serve = subparsers.add_parser(
        "serve", help="Run the self-hosted SQLite/filesystem backend"
    )
    p_serve.add_argument("--host", default="0.0.0.0")
    p_serve.add_argument("--port", type=int, default=8080)
    p_serve.add_argument(
        "--log-level", default="info",
        choices=("critical", "error", "warning", "info", "debug", "trace"),
    )
    p_serve.set_defaults(func=cmd_serve)
    
    # Dashboard
    p_dash = subparsers.add_parser("dashboard", help="Show system status dashboard")
    p_dash.add_argument("--verbose", "-v", action="store_true", help="Show detailed info")
    p_dash.add_argument("--coordinator-url", help="Coordinator base URL")
    p_dash.add_argument("--token", help="Worker or client API token")
    p_dash.set_defaults(func=cmd_dashboard)
    
    # Config
    p_config = subparsers.add_parser("config", help="View coordinator configuration")
    p_config.set_defaults(func=cmd_config)
    
    # Workers
    p_workers = subparsers.add_parser("workers", help="List registered workers")
    p_workers.add_argument("--active", action="store_true", help="Show only active workers")
    p_workers.add_argument("--verbose", "-v", action="store_true", help="Show detailed info")
    p_workers.add_argument("--coordinator-url", help="Coordinator base URL")
    p_workers.add_argument("--token", help="Worker or client API token")
    p_workers.set_defaults(func=cmd_workers)
    
    # Test S3
    p_test_s3 = subparsers.add_parser("test-s3", help="Test S3 endpoint capabilities")
    p_test_s3.set_defaults(func=cmd_test_s3)
    
    # Watch
    p_watch = subparsers.add_parser("watch", help="Watch system status in real-time")
    p_watch.add_argument("--interval", "-i", type=int, default=10, help="Refresh interval in seconds")
    p_watch.add_argument("--verbose", "-v", action="store_true", help="Show detailed info")
    p_watch.set_defaults(func=cmd_watch)
    
    # Download
    p_download = subparsers.add_parser("download", help="Download completed job results")
    p_download.add_argument("hash", help="SHA256 hash of the PDF")
    p_download.add_argument("--output", "-o", help="Output path (default: <hash>.zip)")
    p_download.add_argument("--recipe-digest", type=_recipe_digest_arg,
                            help="Download a retained recipe instead of the selected artifact")
    p_download.add_argument("--coordinator-url", help="Coordinator base URL")
    p_download.add_argument("--token", help="Admin token for the coordinator")
    p_download.set_defaults(func=cmd_download)
    
    # Preview
    p_preview = subparsers.add_parser("preview", help="Preview completed job content")
    p_preview.add_argument("hash", help="SHA256 hash of the PDF")
    p_preview.add_argument("--lines", "-n", type=int, default=50, help="Lines of markdown to show")
    p_preview.add_argument("--recipe-digest", type=_recipe_digest_arg,
                           help="Preview a retained recipe instead of the selected artifact")
    p_preview.add_argument("--coordinator-url", help="Coordinator base URL")
    p_preview.add_argument("--token", help="Admin token for the coordinator")
    p_preview.set_defaults(func=cmd_preview)

    # Recipe-aware artifacts
    p_artifacts = subparsers.add_parser("artifacts", help="List retained conversion artifacts")
    p_artifacts.add_argument("hash", help="SHA256 hash of the PDF")
    p_artifacts.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    p_artifacts.add_argument("--coordinator-url", help="Coordinator base URL")
    p_artifacts.add_argument("--token", help="Admin token for the coordinator")
    p_artifacts.set_defaults(func=cmd_artifacts)

    p_request_conversion = subparsers.add_parser(
        "request-conversion",
        help="Select or queue an exact conversion recipe",
    )
    p_request_conversion.add_argument("hash", help="SHA256 hash of the PDF")
    p_request_conversion.add_argument("recipe_digest", nargs="?", type=_recipe_digest_arg,
                                      help="Exact recipe digest advertised by a compatible worker")
    p_request_conversion.add_argument("--backend", help="Backend name, if exactly one active recipe matches")
    p_request_conversion.add_argument("--dry-run", action="store_true",
                                      help="Show whether the recipe would be selected or queued")
    p_request_conversion.add_argument("--coordinator-url", help="Coordinator base URL")
    p_request_conversion.add_argument("--token", help="Admin token for the coordinator")
    p_request_conversion.set_defaults(func=cmd_request_conversion)
    
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
    try:
        result = args.func(args)
    except KeyboardInterrupt:
        print("Interrupted.")
        sys.exit(130)
    except CoordinatorError as exc:
        print(f"Error: {exc}")
        sys.exit(1)
    except Exception as exc:
        if os.getenv("BLOBFORGE_DEBUG"):
            raise
        print(f"Error: {type(exc).__name__}: {exc}")
        sys.exit(1)
    sys.exit(result if result else 0)


if __name__ == "__main__":
    main()
