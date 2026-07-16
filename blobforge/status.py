"""
BlobForge Status - Display queue statistics and job status.
"""
import argparse
from datetime import datetime, timedelta

from .config import S3_BUCKET, PRIORITIES
from .coordinator_client import CoordinatorClient


def format_size(size_bytes: int) -> str:
    """Format bytes as human-readable size."""
    if size_bytes is None or size_bytes == 0:
        return "-"
    for unit in ['B', 'KB', 'MB', 'GB']:
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
    coordinator_candidate = CoordinatorClient()
    if coordinator_candidate.available:
        return _show_coordinator_status(coordinator_candidate, verbose)

    raise RuntimeError(
        "Dashboard requires BLOBFORGE_COORDINATOR_URL and "
        "BLOBFORGE_COORDINATOR_TOKEN"
    )

def _show_coordinator_status(coordinator: CoordinatorClient, verbose: bool = False):
    """Display the Bunny coordinator snapshot without consulting S3 state."""
    snapshot = coordinator.snapshot()
    counts = snapshot.get("counts", {})
    priority = snapshot.get("priority", {})
    jobs = snapshot.get("jobs", [])
    workers = snapshot.get("workers", [])

    print(f"{'=' * 60}")
    print("  BlobForge Status Dashboard")
    print(f"{'=' * 60}")
    print("  Coordination: Bunny Edge Script + Bunny Database")
    print(f"  Blob store:   s3://{S3_BUCKET}")

    print(f"\n{'─' * 60}\n[QUEUE]\n{'─' * 60}")
    for name in PRIORITIES:
        count = int(priority.get(name, 0))
        print(f"  {name:<15}: {count:>5} {'█' * min(count, 40)}")
    print(f"  {'TOTAL':<15}: {int(counts.get('todo', 0)) + int(counts.get('failed', 0)):>5}")

    processing = [job for job in jobs if job.get("status") == "processing"]
    print(f"\n{'─' * 60}\n[PROCESSING]\n{'─' * 60}")
    print(f"  Active jobs : {len(processing)}")
    for job in processing:
        progress = job.get("progress") or {}
        filename = job.get("original_name") or "unknown.pdf"
        stage = progress.get("stage", "processing")
        worker = job.get("worker_id") or "?"
        print(f"  {job['hash'][:16]}...  {filename[:38]:<38} {worker:<14} {stage}")

    print(f"\n{'─' * 60}\n[RESULTS]\n{'─' * 60}")
    print(f"  ✅ Completed   : {int(counts.get('done', 0))}")
    print(f"  ⏳ Failed      : {int(counts.get('failed', 0))}")
    print(f"  ❌ Dead-letter : {int(counts.get('dead', 0))}")

    total = sum(int(counts.get(state, 0)) for state in ("todo", "processing", "failed", "dead", "done"))
    done = int(counts.get("done", 0))
    if total:
        width = 40
        filled = int(width * done / total)
        print(f"\n  [{'█' * filled}{'░' * (width - filled)}] {done / total * 100:.1f}%")
        print(f"  Completed: {done}/{total}")

    if verbose:
        print(f"\n{'─' * 60}\n[WORKERS]\n{'─' * 60}")
        for worker in workers:
            last = worker.get("last_heartbeat")
            if last:
                age = datetime.now().timestamp() - float(last) / 1000
                last_text = format_duration(timedelta(seconds=max(0, age))) + " ago"
            else:
                last_text = "never"
            print(
                f"  {worker.get('worker_id', '?'):<14} "
                f"{worker.get('hostname', '?'):<24} "
                f"{worker.get('status', '?'):<12} {last_text}"
            )
    print(f"\n{'=' * 60}")


def main():
    parser = argparse.ArgumentParser(description="BlobForge Status")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed job information")
    args = parser.parse_args()
    
    show_status(verbose=args.verbose)


if __name__ == "__main__":
    main()
