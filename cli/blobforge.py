#!/usr/bin/env python3
"""
BlobForge CLI - Submit jobs and manage the queue

Usage:
    blobforge submit <path> [--type TYPE] [--priority N]
    blobforge status [<job_id>]
    blobforge workers
    blobforge stats
"""

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

import httpx


def get_client():
    """Get HTTP client for BlobForge server."""
    server_url = os.environ.get("BLOBFORGE_SERVER_URL", "http://localhost:8080")
    return httpx.Client(base_url=server_url, timeout=30)


def compute_hash(file_path: Path) -> str:
    """Compute SHA-256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def submit_file(client: httpx.Client, file_path: Path, job_type: str, priority: int) -> dict:
    """Submit a file for processing."""
    # Compute hash
    file_hash = compute_hash(file_path)
    file_size = file_path.stat().st_size

    # Check if job already exists or create new one
    resp = client.post(
        "/api/jobs",
        json={
            "type": job_type,
            "source_hash": file_hash,
            "source_path": str(file_path),
            "source_size": file_size,
            "priority": priority,
        },
    )
    resp.raise_for_status()
    job_info = resp.json()

    # If job already exists, just return it
    if job_info.get("existing"):
        return {"job_id": job_info["job_id"], "status": job_info["status"], "existing": True}

    # Get upload URL for new job
    resp = client.post(
        "/api/files/upload-url",
        json={
            "key": f"sources/{file_hash}",
            "content_type": "application/pdf",
        },
    )
    resp.raise_for_status()
    upload_url = resp.json()["url"]

    # Upload the file
    with open(file_path, "rb") as f:
        upload_resp = httpx.put(
            upload_url,
            content=f,
            headers={"Content-Type": "application/pdf"},
            timeout=300,
        )
        upload_resp.raise_for_status()

    return {"job_id": job_info["job_id"], "status": "pending", "existing": False}


def cmd_submit(args):
    """Handle submit command."""
    path = Path(args.path)
    if not path.exists():
        print(f"Error: {path} does not exist", file=sys.stderr)
        sys.exit(1)

    client = get_client()
    try:
        if path.is_file():
            result = submit_file(client, path, args.type, args.priority)
            if result["existing"]:
                print(f"Job already exists: #{result['job_id']} ({result['status']})")
            else:
                print(f"Submitted job #{result['job_id']}")
        elif path.is_dir():
            # Submit all PDFs in directory
            pdf_files = list(path.rglob("*.pdf"))
            if not pdf_files:
                print(f"No PDF files found in {path}")
                return

            print(f"Found {len(pdf_files)} PDF files")
            for pdf_path in pdf_files:
                try:
                    result = submit_file(client, pdf_path, args.type, args.priority)
                    status = "exists" if result["existing"] else "submitted"
                    print(f"  [{status}] {pdf_path.name} -> #{result['job_id']}")
                except Exception as e:
                    print(f"  [error] {pdf_path.name}: {e}", file=sys.stderr)
    finally:
        client.close()


def cmd_status(args):
    """Handle status command."""
    client = get_client()
    try:
        if args.job_id:
            # Get specific job
            resp = client.get(f"/api/jobs/{args.job_id}")
            resp.raise_for_status()
            job = resp.json()
            print(json.dumps(job, indent=2))
        else:
            # List recent jobs
            resp = client.get("/api/jobs", params={"limit": 20})
            resp.raise_for_status()
            data = resp.json()
            print(f"Total jobs: {data['total']}")
            print()
            for job in data["jobs"]:
                worker = job.get("worker_id", "-")[:8] if job.get("worker_id") else "-"
                print(f"#{job['id']:4d}  {job['status']:10s}  {job['type']:15s}  worker:{worker}")
    finally:
        client.close()


def cmd_workers(args):
    """Handle workers command."""
    client = get_client()
    try:
        resp = client.get("/api/workers")
        resp.raise_for_status()
        workers = resp.json()

        if not workers:
            print("No workers registered")
            return

        print(f"{'ID':<18} {'Type':<15} {'Status':<10} {'Job':<6} {'Last Heartbeat'}")
        print("-" * 70)
        for w in workers:
            job = f"#{w['current_job_id']}" if w.get("current_job_id") else "-"
            hb = w.get("last_heartbeat", "never")
            print(f"{w['id'][:16]:<18} {w['type']:<15} {w['status']:<10} {job:<6} {hb}")
    finally:
        client.close()


def cmd_stats(args):
    """Handle stats command."""
    client = get_client()
    try:
        resp = client.get("/api/stats")
        resp.raise_for_status()
        stats = resp.json()

        print("Job Statistics:")
        print(f"  Total:     {stats['total_jobs']}")
        print(f"  Pending:   {stats['pending_jobs']}")
        print(f"  Running:   {stats['running_jobs']}")
        print(f"  Completed: {stats['completed_jobs']}")
        print(f"  Failed:    {stats['failed_jobs']}")
        print(f"  Dead:      {stats['dead_jobs']}")
        print()
        print("Workers:")
        print(f"  Total:   {stats['total_workers']}")
        print(f"  Online:  {stats['online_workers']}")
        print(f"  Offline: {stats['offline_workers']}")
    finally:
        client.close()


def main():
    parser = argparse.ArgumentParser(description="BlobForge CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Submit command
    submit_parser = subparsers.add_parser("submit", help="Submit files for processing")
    submit_parser.add_argument("path", help="File or directory to submit")
    submit_parser.add_argument("--type", "-t", default="pdf-convert", help="Job type")
    submit_parser.add_argument("--priority", "-p", type=int, default=3, help="Job priority (1=highest)")
    submit_parser.set_defaults(func=cmd_submit)

    # Status command
    status_parser = subparsers.add_parser("status", help="Get job status")
    status_parser.add_argument("job_id", nargs="?", type=int, help="Specific job ID")
    status_parser.set_defaults(func=cmd_status)

    # Workers command
    workers_parser = subparsers.add_parser("workers", help="List workers")
    workers_parser.set_defaults(func=cmd_workers)

    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Show statistics")
    stats_parser.set_defaults(func=cmd_stats)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
