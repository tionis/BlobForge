"""
BlobForge Status - Display queue statistics and job status.

Uses the PostgreSQL database when available, falls back to S3.
"""

import os
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from .config import (
    S3_BUCKET,
    S3_PREFIX_TODO,
    S3_PREFIX_PROCESSING,
    S3_PREFIX_DONE,
    S3_PREFIX_FAILED,
    S3_PREFIX_DEAD,
    PRIORITIES,
    STALE_TIMEOUT_MINUTES,
    get_stale_timeout_minutes,
    DATABASE_URL,
)
from .s3_client import S3Client
from .db import DatabaseClient


def format_size(size_bytes: int) -> str:
    """Format bytes as human-readable size."""
    if size_bytes is None or size_bytes == 0:
        return "-"
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def format_duration(td: timedelta) -> str:
    """Format timedelta as human-readable duration."""
    total_seconds = int(td.total_seconds())
    if total_seconds < 60:
        return f"{total_seconds}s"
    elif total_seconds < 3600:
        mins = total_seconds // 60
        secs = total_seconds % 60
        return f"{mins}m {secs}s"
    else:
        hours = total_seconds // 3600
        mins = (total_seconds % 3600) // 60
        return f"{hours}h {mins}m"


def show_status(verbose: bool = False):
    """Display comprehensive status of the processing system."""
    s3 = S3Client()
    db = DatabaseClient()
    use_db = db.available
    stale_timeout = get_stale_timeout_minutes()

    print(f"{'=' * 60}")
    print(f"  BlobForge Status Dashboard")
    print(f"{'=' * 60}")
    print(f"  Bucket: s3://{S3_BUCKET}")
    print(f"  Database: {'connected' if use_db else 'not configured (using S3)'}")
    print(f"  Stale timeout: {stale_timeout} minutes")

    if use_db:
        _show_status_db(db, s3, verbose, stale_timeout)
    else:
        _show_status_s3(s3, verbose, stale_timeout)


def _show_status_db(
    db: DatabaseClient, s3: S3Client, verbose: bool, stale_timeout: int
):
    """Show status using PostgreSQL (fast, single queries)."""
    counts = db.get_queue_counts()
    todo_by_prio = counts.get("todo", {})
    total_todo = counts.get("todo_total", 0)
    processing_count = counts.get("processing", 0)
    done_count = counts.get("done", 0)
    failed_count = counts.get("failed", 0)
    dead_count = counts.get("dead", 0)

    print(f"\n{'─' * 60}")
    print("[QUEUE]")
    print(f"{'─' * 60}")
    for prio in PRIORITIES:
        count = todo_by_prio.get(prio, 0)
        bar = "█" * min(count, 40) if count > 0 else ""
        print(f"  {prio:<15}: {count:>5} {bar}")
    print(f"  {'TOTAL':<15}: {total_todo:>5}")

    print(f"\n{'─' * 60}")
    print("[PROCESSING]")
    print(f"{'─' * 60}")
    print(f"  Active jobs : {processing_count}")

    processing_jobs = db.get_processing_jobs()
    stale_count = 0
    now = datetime.now(tz=None)
    for job in processing_jobs:
        updated = job.get("updated_at")
        if updated:
            if isinstance(updated, str):
                try:
                    updated = datetime.fromisoformat(updated)
                except ValueError:
                    updated = None
            if updated:
                age = now - updated.replace(tzinfo=None) if updated else timedelta(0)
                if age > timedelta(minutes=stale_timeout):
                    stale_count += 1

    if processing_count > 0:
        print(f"  Stale (>{stale_timeout}m): {stale_count}")
        print(f"\n  {'Worker':<14} {'Hash':<18} {'Priority':<15} {'Updated':<20}")
        print(f"  {'-' * 14} {'-' * 18} {'-' * 15} {'-' * 20}")
        for job in processing_jobs:
            worker = (
                (job.get("worker_id", "?")[:12] + "...")
                if len(job.get("worker_id", "?")) > 12
                else job.get("worker_id", "?")
            )
            job_hash = job.get("file_hash", "?")[:14] + "..."
            priority = job.get("priority", "?")
            updated = job.get("updated_at", "-")
            if isinstance(updated, datetime):
                updated = updated.isoformat()[:19]
            elif updated and isinstance(updated, str):
                updated = updated[:19]
            progress = job.get("progress", {}) or {}
            stage = progress.get("stage", "-")
            print(
                f"  {worker:<14} {job_hash:<18} {priority:<15} {str(updated):<20} {stage}"
            )

    print(f"\n{'─' * 60}")
    print("[RESULTS]")
    print(f"{'─' * 60}")
    print(f"  ✅ Completed   : {done_count}")
    print(
        f"  ⏳ Failed      : {failed_count} {'(will be retried by janitor)' if failed_count else ''}"
    )
    print(
        f"  ❌ Dead-letter : {dead_count} {'(exceeded max retries)' if dead_count else ''}"
    )

    total_jobs = total_todo + processing_count + done_count + failed_count + dead_count
    if total_jobs > 0:
        completed_pct = (done_count / total_jobs) * 100
        remaining = total_todo + processing_count
        print(f"\n{'─' * 60}")
        print("[PROGRESS]")
        print(f"{'─' * 60}")
        bar_width = 40
        filled = int(bar_width * done_count / total_jobs)
        bar = "█" * filled + "░" * (bar_width - filled)
        print(f"  [{bar}] {completed_pct:.1f}%")
        print(f"  Completed: {done_count}/{total_jobs}")
        if remaining > 0:
            print(f"  Remaining: {remaining}")
        if failed_count + dead_count > 0:
            print(
                f"  Failed:    {failed_count + dead_count} ({((failed_count + dead_count) / total_jobs) * 100:.1f}%)"
            )

    if verbose:
        print(f"\n{'─' * 60}")
        print("[WORKERS]")
        print(f"{'─' * 60}")
        workers = db.get_active_workers(stale_minutes=stale_timeout)
        if workers:
            print(f"  Active workers: {len(workers)}")
            for w in workers:
                worker_id = w.get("worker_id", "?")[:12]
                hostname = w.get("hostname", "?")
                metrics = w.get("metrics", {})
                jobs_completed = (
                    metrics.get("jobs_completed", 0) if isinstance(metrics, dict) else 0
                )
                jobs_per_hour = (
                    metrics.get("jobs_per_hour", 0) if isinstance(metrics, dict) else 0
                )
                system = w.get("system", {})
                cpu = (
                    system.get("cpu_percent", "-") if isinstance(system, dict) else "-"
                )
                mem = (
                    system.get("memory_percent", "-")
                    if isinstance(system, dict)
                    else "-"
                )
                print(
                    f"  🟢 {worker_id} ({hostname}): {jobs_completed} jobs, {jobs_per_hour:.1f}/hr, CPU:{cpu}%, MEM:{mem}%"
                )
        else:
            print("  No active workers")

    print(f"\n{'=' * 60}")


def _show_status_s3(s3: S3Client, verbose: bool, stale_timeout: int):
    """Show status using S3 (fallback, slower)."""
    todo_prefixes = [f"{S3_PREFIX_TODO}/{prio}/" for prio in PRIORITIES]
    result_prefixes = [
        f"{S3_PREFIX_DONE}/",
        f"{S3_PREFIX_FAILED}/",
        f"{S3_PREFIX_DEAD}/",
    ]
    all_prefixes = todo_prefixes + result_prefixes

    with ThreadPoolExecutor(max_workers=8) as executor:
        count_futures = {
            prefix: executor.submit(s3.count_prefix, prefix) for prefix in all_prefixes
        }
        processing_future = executor.submit(s3.scan_processing_detailed)

        workers = None
        if verbose:
            workers_future = executor.submit(
                s3.get_active_workers, stale_minutes=stale_timeout
            )

        todo_counts = []
        for prefix in todo_prefixes:
            todo_counts.append(count_futures[prefix].result())

        done_count = count_futures[f"{S3_PREFIX_DONE}/"].result()
        failed_count = count_futures[f"{S3_PREFIX_FAILED}/"].result()
        dead_count = count_futures[f"{S3_PREFIX_DEAD}/"].result()

        active_jobs = processing_future.result()

        if verbose:
            workers = workers_future.result()

    stale_count = sum(1 for j in active_jobs if j.get("stale", False))
    total_todo = sum(todo_counts)

    print(f"\n{'─' * 60}")
    print("[QUEUE]")
    print(f"{'─' * 60}")
    for prio, count in zip(PRIORITIES, todo_counts):
        bar = "█" * min(count, 40) if count > 0 else ""
        print(f"  {prio:<15}: {count:>5} {bar}")
    print(f"  {'TOTAL':<15}: {total_todo:>5}")

    print(f"\n{'─' * 60}")
    print("[PROCESSING]")
    print(f"{'─' * 60}")
    print(f"  Active jobs : {len(active_jobs)}")
    print(
        f"  Stale (>{stale_timeout}m): {stale_count} {'⚠️  (run janitor to recover)' if stale_count else '✓'}"
    )

    if active_jobs:
        print(
            f"\n  {'Status':<8} {'Hash':<18} {'File':<50} {'Worker':<14} {'Elapsed':<10} {'Stage ETA':<10} {'Stage':<40} {'Details'}"
        )
        print(
            f"  {'-' * 8} {'-' * 18} {'-' * 50} {'-' * 14} {'-' * 10} {'-' * 10} {'-' * 40} {'-' * 20}"
        )

        for job in sorted(
            active_jobs, key=lambda x: x.get("age", timedelta(0)), reverse=True
        ):
            status = "🔴 STALE" if job.get("stale") else "🟢 OK"
            job_hash = job["hash"][:14] + "..."
            worker = (
                (job.get("worker", "?")[:12] + "...")
                if len(job.get("worker", "?")) > 12
                else job.get("worker", "?")
            )

            progress = job.get("progress", {}) or {}
            stage = progress.get("stage", "-")

            elapsed = progress.get(
                "elapsed_formatted", format_duration(job.get("age", timedelta(0)))
            )
            filename = progress.get("original_filename", "-")
            if len(filename) > 48:
                filename = filename[:45] + "..."

            marker_progress = progress.get("marker", {})
            eta_str = ""
            if marker_progress:
                tqdm_stage = marker_progress.get("tqdm_stage", "")
                tqdm_current = marker_progress.get("tqdm_current", 0)
                tqdm_total = marker_progress.get("tqdm_total", 0)
                tqdm_eta = marker_progress.get("tqdm_eta")

                if tqdm_stage and tqdm_total:
                    stage = f"{tqdm_stage}: {tqdm_current}/{tqdm_total}"
                    if len(stage) > 38:
                        stage = stage[:35] + "..."
                elif tqdm_stage:
                    stage = tqdm_stage[:38] if len(tqdm_stage) > 38 else tqdm_stage

                if tqdm_eta is not None and tqdm_eta > 0:
                    if tqdm_eta < 60:
                        eta_str = f"~{int(tqdm_eta)}s"
                    elif tqdm_eta < 3600:
                        eta_str = f"~{int(tqdm_eta // 60)}m{int(tqdm_eta % 60)}s"
                    else:
                        hours = int(tqdm_eta // 3600)
                        mins = int((tqdm_eta % 3600) // 60)
                        eta_str = f"~{hours}h{mins}m"

            details_parts = []
            page_count = progress.get("page_count")
            if page_count:
                details_parts.append(f"{page_count}pg")
            file_size = progress.get("file_size_formatted", "")
            if file_size:
                details_parts.append(file_size)
            image_count = progress.get("image_count")
            if image_count is not None:
                details_parts.append(f"{image_count}img")
            system = progress.get("system", {})
            cpu = system.get("cpu_percent")
            if cpu is not None:
                details_parts.append(f"CPU:{cpu}%")

            details_str = " | ".join(details_parts) if details_parts else "-"

            print(
                f"  {status:<7} {job_hash:<18} {filename:<50} {worker:<14} {elapsed:<10} {eta_str:<10} {stage:<40} {details_str}"
            )

    print(f"\n{'─' * 60}")
    print("[RESULTS]")
    print(f"{'─' * 60}")

    print(f"  ✅ Completed   : {done_count}")
    print(
        f"  ⏳ Failed      : {failed_count} {'(will be retried by janitor)' if failed_count else ''}"
    )
    print(
        f"  ❌ Dead-letter : {dead_count} {'(exceeded max retries)' if dead_count else ''}"
    )

    unique_todo = max(0, total_todo - len(active_jobs))
    total_jobs = unique_todo + len(active_jobs) + done_count + failed_count + dead_count
    if total_jobs > 0:
        completed_pct = (done_count / total_jobs) * 100
        failed_pct = ((failed_count + dead_count) / total_jobs) * 100
        remaining = unique_todo + len(active_jobs)

        print(f"\n{'─' * 60}")
        print("[PROGRESS]")
        print(f"{'─' * 60}")

        bar_width = 40
        filled = int(bar_width * done_count / total_jobs)
        bar = "█" * filled + "░" * (bar_width - filled)
        print(f"  [{bar}] {completed_pct:.1f}%")
        print(f"  Completed: {done_count}/{total_jobs}")
        if remaining > 0:
            print(f"  Remaining: {remaining}")
        if failed_count + dead_count > 0:
            print(f"  Failed:    {failed_count + dead_count} ({failed_pct:.1f}%)")

    if verbose:
        print(f"\n{'─' * 60}")
        print("[WORKERS]")
        print(f"{'─' * 60}")

        if workers:
            print(f"  Active workers: {len(workers)}")
            for w in workers:
                worker_id = w.get("worker_id", "?")[:12]
                hostname = w.get("hostname", "?")
                metrics = w.get("metrics", {})
                jobs_completed = metrics.get("jobs_completed", 0)
                jobs_per_hour = metrics.get("jobs_per_hour", 0)
                system = w.get("system", {})
                cpu = system.get("cpu_percent", "-")
                mem = system.get("memory_percent", "-")
                print(
                    f"  🟢 {worker_id} ({hostname}): {jobs_completed} jobs, {jobs_per_hour:.1f}/hr, CPU:{cpu}%, MEM:{mem}%"
                )
        else:
            print("  No active workers")

    print(f"\n{'=' * 60}")


def main():
    parser = argparse.ArgumentParser(description="BlobForge Status")
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Show detailed job information"
    )
    args = parser.parse_args()

    show_status(verbose=args.verbose)


if __name__ == "__main__":
    main()
