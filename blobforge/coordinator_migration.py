"""One-time import of the legacy S3 coordination state into Bunny Database."""

from __future__ import annotations

import os
from pathlib import PurePath
from typing import Any, Dict, Iterable, List

from .config import (
    DEFAULT_PRIORITY,
    PRIORITIES,
    S3_PREFIX_DEAD,
    S3_PREFIX_FAILED,
    S3_PREFIX_PROCESSING,
    S3_PREFIX_TODO,
    get_remote_config,
)
from .coordinator_client import CoordinatorClient, CoordinatorError
from .s3_client import S3Client


def _hash_from_key(key: str) -> str:
    return key.rstrip("/").rsplit("/", 1)[-1]


def _marker(s3: S3Client, key: str) -> Dict[str, Any]:
    return s3.get_object_json(key) or {}


def _batches(items: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    for offset in range(0, len(items), size):
        yield items[offset : offset + size]


def migrate(*, dry_run: bool = False, batch_size: int = 200) -> int:
    """Import all legacy queue states, preserving terminal state and retries.

    In-flight S3 locks deliberately become queued jobs. Their old lock tokens
    cannot safely fence work after the coordination backend changes.
    """
    s3 = S3Client()
    manifest = s3.get_manifest().get("entries", {})
    states: Dict[str, Dict[str, Any]] = {}

    for priority in PRIORITIES:
        prefix = f"{S3_PREFIX_TODO}/{priority}/"
        for key in s3.list_keys(prefix):
            job_hash = _hash_from_key(key)
            marker = _marker(s3, key)
            states[job_hash] = {
                "status": "todo",
                "priority": priority,
                "retry_count": int(marker.get("retries", 0) or 0),
            }

    for key in s3.list_keys(f"{S3_PREFIX_PROCESSING}/"):
        job_hash = _hash_from_key(key)
        marker = _marker(s3, key)
        states[job_hash] = {
            "status": "processing",
            "priority": marker.get("priority", states.get(job_hash, {}).get("priority", DEFAULT_PRIORITY)),
            "retry_count": int(marker.get("retries", 0) or 0),
        }

    for prefix, status in ((S3_PREFIX_FAILED, "failed"), (S3_PREFIX_DEAD, "dead")):
        for key in s3.list_keys(f"{prefix}/"):
            job_hash = _hash_from_key(key)
            marker = _marker(s3, key)
            retries = marker.get("total_retries", marker.get("retries", 0))
            states[job_hash] = {
                "status": status,
                "priority": marker.get("priority", states.get(job_hash, {}).get("priority", DEFAULT_PRIORITY)),
                "retry_count": int(retries or 0),
                "error": marker.get("error"),
            }

    for job_hash in s3.list_done_hashes():
        states[job_hash] = {
            "status": "done",
            "priority": states.get(job_hash, {}).get("priority", DEFAULT_PRIORITY),
            "retry_count": int(states.get(job_hash, {}).get("retry_count", 0)),
        }

    default_max_retries = int(get_remote_config().get("max_retries", 3))
    items: List[Dict[str, Any]] = []
    for job_hash, state in states.items():
        entry = manifest.get(job_hash, {})
        paths = entry.get("paths", [])
        original_name = PurePath(paths[0]).name if paths else None
        items.append(
            {
                "hash": job_hash,
                "original_name": original_name,
                "size_bytes": int(entry.get("size", 0) or 0),
                "paths": paths,
                "tags": entry.get("tags", []),
                "source": entry.get("source"),
                "max_retries": default_max_retries,
                **state,
            }
        )

    counts: Dict[str, int] = {}
    for item in items:
        status = str(item["status"])
        counts[status] = counts.get(status, 0) + 1
    summary = ", ".join(f"{key}={value}" for key, value in sorted(counts.items())) or "empty"
    print(f"Discovered {len(items)} legacy jobs ({summary}).")
    if dry_run:
        print("Dry run: no Bunny Database state was changed.")
        return 0

    token = os.getenv("BLOBFORGE_MIGRATION_TOKEN", "")
    client = CoordinatorClient()
    if not client.base_url:
        raise CoordinatorError("Set BLOBFORGE_COORDINATOR_URL before migration")
    if not token:
        raise CoordinatorError("Set BLOBFORGE_MIGRATION_TOKEN before migration")
    imported = 0
    for batch in _batches(items, min(max(1, batch_size), 250)):
        result = client.import_items(batch, token)
        imported += int(result.get("imported", 0))
        print(f"Imported {imported}/{len(items)} jobs...")
    print("Migration complete. Start only coordinator-configured workers from this point onward.")
    return 0
