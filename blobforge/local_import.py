"""Offline import of a verified BlobForge v2 stage into local server storage."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .mdaf import validate_mdaf
from .mdaf.digest import blake3_bytes, canonical_json_bytes
from .server.database import Database, now_ms
from .server.storage import LocalStorage


@dataclass(frozen=True)
class ImportSummary:
    checked: int
    imported: int
    skipped: int


def import_legacy_sources(
    workspace: str | Path,
    data_dir: str | Path,
    *,
    dry_run: bool = True,
) -> ImportSummary:
    """Import raw sources absent from the completed-artifact stage.

    The old catalog has no reliable original paths/tags for every source, so
    missing records are queued at normal priority with an explicit diagnostic.
    """
    workspace_root = Path(workspace).resolve()
    catalog = workspace_root / "catalog.sqlite3"
    connection = sqlite3.connect(catalog)
    connection.row_factory = sqlite3.Row
    try:
        rows = list(connection.execute(
            "SELECT legacy_sha256,raw_path,size_bytes,blake3,sha256_verified FROM sources ORDER BY legacy_sha256"
        ))
    finally:
        connection.close()
    checked: list[tuple[sqlite3.Row, Path]] = []
    for row in rows:
        source = Path(row["raw_path"]).resolve()
        if workspace_root not in source.parents or not source.is_file():
            raise ValueError(f"legacy source is missing or outside workspace: {row['legacy_sha256']}")
        if source.stat().st_size != int(row["size_bytes"]):
            raise ValueError(f"legacy source size mismatch: {row['legacy_sha256']}")
        checked.append((row, source))
    if dry_run:
        return ImportSummary(len(checked), 0, 0)

    root = Path(data_dir)
    database = Database(root / "blobforge.sqlite3", lease_seconds=900, max_retries=3)
    storage = LocalStorage(root)
    imported = skipped = 0
    for row, source in checked:
        key = str(row["legacy_sha256"])
        try:
            database.get_job(key)
            skipped += 1
            continue
        except KeyError:
            pass
        inspected = LocalStorage.inspect(source)
        if inspected.sha256 != key:
            raise ValueError(f"legacy SHA-256 mismatch: {key}")
        catalog_blake3 = row["blake3"]
        if catalog_blake3 and inspected.blake3 != catalog_blake3:
            raise ValueError(f"catalog BLAKE3 mismatch: {key}")
        target = storage.source_path("blake3", inspected.blake3)
        if not target.exists():
            with source.open("rb") as stream:
                stored = storage.atomic_stream(stream, target)
            if stored.blake3 != inspected.blake3:
                raise ValueError(f"copied source BLAKE3 mismatch: {key}")
        timestamp = now_ms()
        with database.transaction() as db:
            db.execute("""INSERT INTO sources(source_key,digest_algorithm,digest,media_type,original_name,size_bytes,source,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?)""", (key, "blake3", inspected.blake3, "application/pdf", f"{key}.pdf", inspected.size, "legacy-raw-import", timestamp, timestamp))
            db.execute("INSERT INTO source_aliases VALUES(?,?,?)", ("blake3", inspected.blake3, key))
            db.execute("INSERT INTO source_aliases VALUES(?,?,?)", ("sha256", key, key))
            db.execute("""INSERT INTO jobs(source_key,status,priority,paths_json,tags_json,retry_count,created_at,updated_at,error_message)
                VALUES(?,?,?,?,?,?,?,?,?)""", (key, "todo", "3_normal", "[]", '["legacy-import","metadata-unavailable"]', 0, timestamp, timestamp, "Imported without historical coordinator metadata"))
        imported += 1
    return ImportSummary(len(checked), imported, skipped)


def import_stage(
    stage: str | Path,
    data_dir: str | Path,
    *,
    run_id: str = "legacy-mdaf-v1",
    dry_run: bool = True,
) -> ImportSummary:
    stage_root = Path(stage).resolve()
    manifest_path = stage_root / "store" / "v2" / "migrations" / run_id / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    body = {key: value for key, value in manifest.items() if key != "manifest_digest"}
    if manifest.get("format") != "blobforge-v2-local-stage" or manifest.get("version") != 1:
        raise ValueError("unsupported local stage manifest")
    if manifest.get("manifest_digest") != blake3_bytes(canonical_json_bytes(body)):
        raise ValueError("local stage manifest digest mismatch")

    entries = manifest.get("entries")
    if not isinstance(entries, list):
        raise ValueError("local stage entries must be an array")
    checked: list[tuple[dict, Path, Path]] = []
    for entry in entries:
        source = (stage_root / entry["source_key"]).resolve()
        artifact = (stage_root / entry["artifact_key"]).resolve()
        if stage_root not in source.parents or stage_root not in artifact.parents:
            raise ValueError("stage manifest path escapes its root")
        source_digest = str(entry["source_digest"])
        if not source_digest.startswith("blake3:") or LocalStorage.inspect(source).blake3 != source_digest[7:]:
            raise ValueError(f"source digest mismatch: {entry.get('legacy_sha256')}")
        validation = validate_mdaf(artifact)
        if validation.identity != entry["artifact_identity"]:
            raise ValueError(f"MDAF identity mismatch: {entry.get('legacy_sha256')}")
        checked.append((entry, source, artifact))
    if dry_run:
        return ImportSummary(len(checked), 0, 0)

    root = Path(data_dir)
    database = Database(root / "blobforge.sqlite3", lease_seconds=900, max_retries=3)
    storage = LocalStorage(root)
    imported = skipped = 0
    for entry, source, artifact in checked:
        key = str(entry["legacy_sha256"])
        source_digest = str(entry["source_digest"])[7:]
        recipe = str(entry["recipe_digest"])
        identity = str(entry["artifact_identity"])
        if database.artifact(key, recipe):
            skipped += 1
            continue
        source_target = storage.source_path("blake3", source_digest)
        if not source_target.exists():
            with source.open("rb") as stream:
                storage.atomic_stream(stream, source_target)
        artifact_target = storage.artifact_path(key, recipe, identity)
        if not artifact_target.exists():
            with artifact.open("rb") as stream:
                stored = storage.atomic_stream(stream, artifact_target)
        else:
            stored = storage.inspect(artifact_target)
        timestamp = now_ms()
        with database.transaction() as db:
            db.execute("""INSERT INTO sources(source_key,digest_algorithm,digest,media_type,original_name,size_bytes,source,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(source_key) DO NOTHING""",
                (key, "blake3", source_digest, "application/pdf", f"{key}.pdf", source.stat().st_size, "legacy-mdaf-import", timestamp, timestamp))
            db.execute("INSERT OR IGNORE INTO source_aliases VALUES(?,?,?)", ("blake3", source_digest, key))
            db.execute("INSERT OR IGNORE INTO source_aliases VALUES(?,?,?)", ("sha256", key, key))
            done_seq = int(db.execute("SELECT COALESCE(MAX(done_seq),0)+1 FROM jobs").fetchone()[0])
            db.execute("""INSERT INTO jobs(source_key,status,priority,paths_json,tags_json,retry_count,recipe_digest,created_at,updated_at,completed_at,done_seq)
                VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(source_key) DO UPDATE SET status='done',recipe_digest=excluded.recipe_digest,
                completed_at=excluded.completed_at,done_seq=excluded.done_seq,updated_at=excluded.updated_at""",
                (key, "done", "3_normal", "[]", '["legacy-import"]', 0, recipe, timestamp, timestamp, timestamp, done_seq))
            provenance = {
                "legacy": True,
                "migration_run": run_id,
                "attempt_id": entry.get("attempt_id"),
                "historical_format": "blobforge-zip-v0",
                "converter": {"name": "marker-pdf", "version": "unavailable"},
                "mapping_strategy": "page-anchors-and-exact-toc-heading-alignment",
                "metadata_recovery": "partial",
                "recipe_digest": recipe,
            }
            db.execute("""INSERT INTO artifacts(source_key,recipe_digest,identity,storage_path,media_type,artifact_type,size_bytes,sha256,blake3,provenance_json,created_at,legacy,converter_backend,converter_version)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (key, recipe, identity, str(artifact_target.relative_to(root)), "application/zip", "mdaf/v1", stored.size, stored.sha256, stored.blake3, json.dumps(provenance), timestamp, 1, "marker", "unavailable"))
        imported += 1
    return ImportSummary(len(checked), imported, skipped)
