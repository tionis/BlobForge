"""Resumable, local-only migration of legacy BlobForge ZIPs to MDAF v1.

The remote mirror is treated as immutable input. Generated artifacts and the
SQLite catalog live in a separate local tree until an explicit publication
phase is implemented and approved.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import unicodedata
import zipfile
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path, PurePosixPath
from typing import Any, Iterable

from .mdaf import MdafMemberInput, MdafSource, blake3_bytes, blake3_file, build_mdaf, validate_mdaf
from .mdaf.builder import activity
from .mdaf.digest import canonical_json_bytes
from .object_layout import artifact_key, migration_manifest_key, recipe_key, source_key

DEFAULT_WORKSPACE = Path(".blobforge-migration")
LEGACY_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
PAGE_ANCHOR_RE = re.compile(
    r'<span\s+id=["\']page-(\d+)-\d+["\'][^>]*>\s*</span>', re.IGNORECASE
)
HEADING_RE = re.compile(r"^(#{1,6})[ \t]+(.+?)\s*$", re.MULTILINE)
SECRET_KEY_RE = re.compile(
    r"(?:^|[_-])(?:access[_-]?token|api[_-]?key|secret|password|authorization|credential|signed[_-]?url)(?:$|[_-])",
    re.I,
)


@dataclass(frozen=True)
class MigrationSummary:
    sources: int
    legacy_artifacts: int
    paired: int
    converted: int
    failed: int


@dataclass(frozen=True)
class VerificationSummary:
    checked: int
    valid: int
    errors: tuple[str, ...]


@dataclass(frozen=True)
class StageSummary:
    sources: int
    artifacts: int
    recipe_digest: str
    root: Path


def _connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS sources (
            legacy_sha256 TEXT PRIMARY KEY,
            raw_path TEXT NOT NULL,
            size_bytes INTEGER NOT NULL,
            blake3 TEXT,
            sha256_verified INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS legacy_artifacts (
            legacy_sha256 TEXT PRIMARY KEY,
            archive_path TEXT NOT NULL,
            size_bytes INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            output_path TEXT,
            mdaf_identity TEXT,
            error TEXT,
            FOREIGN KEY (legacy_sha256) REFERENCES sources(legacy_sha256)
        );
        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY,
            operation TEXT NOT NULL,
            started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            finished_at TEXT,
            details_json TEXT NOT NULL DEFAULT '{}'
        );
        """
    )
    return connection


def inventory(workspace: str | Path = DEFAULT_WORKSPACE) -> MigrationSummary:
    root = Path(workspace)
    mirror = root / "remote" / "pdf" / "store"
    raw_root, out_root = mirror / "raw", mirror / "out"
    connection = _connect(root / "catalog.sqlite3")
    try:
        with connection:
            for path in sorted(raw_root.glob("*.pdf")):
                digest = path.stem
                if LEGACY_SHA_RE.fullmatch(digest):
                    connection.execute(
                        """INSERT INTO sources(legacy_sha256, raw_path, size_bytes)
                           VALUES (?, ?, ?)
                           ON CONFLICT(legacy_sha256) DO UPDATE SET
                             raw_path=excluded.raw_path, size_bytes=excluded.size_bytes""",
                        (digest, str(path.resolve()), path.stat().st_size),
                    )
            for path in sorted(out_root.glob("*.zip")):
                digest = path.stem
                if LEGACY_SHA_RE.fullmatch(digest):
                    connection.execute(
                        """INSERT INTO legacy_artifacts(legacy_sha256, archive_path, size_bytes)
                           VALUES (?, ?, ?)
                           ON CONFLICT(legacy_sha256) DO UPDATE SET
                             archive_path=excluded.archive_path, size_bytes=excluded.size_bytes""",
                        (digest, str(path.resolve()), path.stat().st_size),
                    )
        return _summary(connection)
    finally:
        connection.close()


def _summary(connection: sqlite3.Connection) -> MigrationSummary:
    sources = connection.execute("SELECT count(*) FROM sources").fetchone()[0]
    artifacts = connection.execute("SELECT count(*) FROM legacy_artifacts").fetchone()[0]
    paired = connection.execute(
        "SELECT count(*) FROM legacy_artifacts a JOIN sources s USING(legacy_sha256)"
    ).fetchone()[0]
    converted = connection.execute(
        "SELECT count(*) FROM legacy_artifacts WHERE status='converted'"
    ).fetchone()[0]
    failed = connection.execute(
        "SELECT count(*) FROM legacy_artifacts WHERE status='failed'"
    ).fetchone()[0]
    return MigrationSummary(sources, artifacts, paired, converted, failed)


def _safe_legacy_members(archive: zipfile.ZipFile) -> list[zipfile.ZipInfo]:
    seen: set[str] = set()
    result = []
    for item in archive.infolist():
        if item.is_dir():
            continue
        path = item.filename
        pure = PurePosixPath(path)
        if (
            not path
            or path.startswith(("/", "\\"))
            or "\\" in path
            or pure.is_absolute()
            or any(part in {"", ".", ".."} for part in pure.parts)
            or item.flag_bits & 1
        ):
            raise ValueError(f"unsafe legacy ZIP member: {path!r}")
        folded = unicodedata.normalize("NFC", path).casefold()
        if folded in seen:
            raise ValueError(f"duplicate legacy ZIP member: {path!r}")
        seen.add(folded)
        result.append(item)
    if "content.md" not in {item.filename for item in result}:
        raise ValueError("legacy ZIP has no content.md")
    return result


def _byte_offset(text: str, character_offset: int) -> int:
    return len(text[:character_offset].encode("utf-8"))


def _strip_page_anchors(markdown: str) -> tuple[str, list[dict[str, Any]]]:
    """Remove anchors while retaining exact page-to-final-byte mappings."""
    matches = list(PAGE_ANCHOR_RE.finditer(markdown))
    if not matches:
        return markdown, []
    output: list[str] = []
    mappings: list[dict[str, Any]] = []
    final_bytes = 0
    cursor = 0
    current_page: int | None = None
    page_start = 0
    for match in matches:
        segment = markdown[cursor : match.start()]
        output.append(segment)
        final_bytes += len(segment.encode("utf-8"))
        if current_page is not None and final_bytes > page_start:
            mappings.append(_page_mapping(page_start, final_bytes, current_page, 1.0, "page-anchor"))
        current_page = int(match.group(1))
        page_start = final_bytes
        cursor = match.end()
    tail = markdown[cursor:]
    output.append(tail)
    final_bytes += len(tail.encode("utf-8"))
    if current_page is not None and final_bytes > page_start:
        mappings.append(_page_mapping(page_start, final_bytes, current_page, 1.0, "page-anchor"))
    return "".join(output), mappings


def _page_mapping(start: int, end: int, page: int, confidence: float, suffix: str) -> dict[str, Any]:
    return {
        "document": {"start": start, "end": end},
        "source": {
            "source_id": "document",
            "selectors": [{"type": "interval", "unit": "page", "start": page, "end": page + 1}],
        },
        "confidence": confidence,
        "method": f"dev.tionis.blobforge/{suffix}",
    }


def _normalized_title(value: str) -> str:
    value = re.sub(r"<[^>]+>|[*_`\[\]()]", "", value)
    return " ".join(value.casefold().replace("\u00ad", "").split()).strip(" .,:;!?")


def _heading_evidence(markdown: str, marker_meta: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[int, dict[str, Any]]]:
    """Align exact/fuzzy TOC titles to Markdown headings and PDF page locators."""
    headings = list(HEADING_RE.finditer(markdown))
    toc = marker_meta.get("table_of_contents", []) if isinstance(marker_meta, dict) else []
    mappings: list[dict[str, Any]] = []
    locators: dict[int, dict[str, Any]] = {}
    unused = set(range(len(headings)))
    for entry in toc if isinstance(toc, list) else []:
        if not isinstance(entry, dict) or not isinstance(entry.get("page_id"), int):
            continue
        title = _normalized_title(str(entry.get("title", "")))
        candidates = [index for index in unused if _normalized_title(headings[index].group(2)) == title]
        if not candidates:
            continue
        index = candidates[0]
        unused.remove(index)
        heading = headings[index]
        start, end = _byte_offset(markdown, heading.start()), _byte_offset(markdown, heading.end())
        selectors: list[dict[str, Any]] = [
            {"type": "interval", "unit": "page", "start": entry["page_id"], "end": entry["page_id"] + 1}
        ]
        polygon = entry.get("polygon")
        if isinstance(polygon, list) and len(polygon) >= 3:
            points = [
                {"x": point[0], "y": point[1]}
                for point in polygon
                if isinstance(point, list) and len(point) == 2
            ]
            if len(points) >= 3:
                selectors.append({"type": "polygon", "unit": "point", "points": points})
        locator = {"source_id": "document", "selectors": selectors}
        locators[index] = locator
        mappings.append(
            {
                "document": {"start": start, "end": end},
                "source": locator,
                "confidence": 1,
                "method": "dev.tionis.blobforge/legacy-marker-toc-title",
            }
        )
    return mappings, locators


def _outline(markdown: str, locators: dict[int, dict[str, Any]]) -> dict[str, Any]:
    headings = list(HEADING_RE.finditer(markdown))
    usable = [
        (index, heading, re.sub(r"<[^>]+>", "", heading.group(2)).strip())
        for index, heading in enumerate(headings)
    ]
    usable = [item for item in usable if item[2]]
    nodes: list[dict[str, Any]] = []
    parents: list[tuple[int, str]] = []
    document_end = len(markdown.encode("utf-8"))
    for position, (source_index, heading, title) in enumerate(usable):
        level = len(heading.group(1))
        while parents and parents[-1][0] >= level:
            parents.pop()
        node_id = f"heading-{position + 1}"
        start = _byte_offset(markdown, heading.start())
        heading_end = _byte_offset(markdown, heading.end())
        section_end = document_end
        for _, following, _ in usable[position + 1 :]:
            if len(following.group(1)) <= level:
                section_end = _byte_offset(markdown, following.start())
                break
        node: dict[str, Any] = {
            "id": node_id,
            "parent": parents[-1][1] if parents else None,
            "level": level,
            "title": title,
            "heading": {"start": start, "end": heading_end},
            "section": {"start": start, "end": section_end},
        }
        if source_index in locators:
            node["source"] = locators[source_index]
        nodes.append(node)
        parents.append((level, node_id))
    return {"nodes": nodes}


def _contains_secret(value: Any) -> bool:
    if isinstance(value, dict):
        return any(SECRET_KEY_RE.search(str(key)) or _contains_secret(item) for key, item in value.items())
    if isinstance(value, list):
        return any(_contains_secret(item) for item in value)
    return False


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def convert_one(legacy_sha256: str, workspace: str | Path = DEFAULT_WORKSPACE) -> Path:
    root = Path(workspace)
    connection = _connect(root / "catalog.sqlite3")
    try:
        row = connection.execute(
            """SELECT s.*, a.archive_path FROM sources s
               JOIN legacy_artifacts a USING(legacy_sha256) WHERE legacy_sha256=?""",
            (legacy_sha256,),
        ).fetchone()
        if row is None:
            raise ValueError(f"no paired source/artifact for {legacy_sha256}")
        raw_path, archive_path = Path(row["raw_path"]), Path(row["archive_path"])
        if _sha256_file(raw_path) != legacy_sha256:
            raise ValueError("raw PDF does not match its legacy SHA-256 key")
        source_digest = blake3_file(raw_path)
        with zipfile.ZipFile(archive_path) as archive:
            members = _safe_legacy_members(archive)
            legacy_markdown_bytes = archive.read("content.md")
            legacy_markdown = legacy_markdown_bytes.decode("utf-8")
            info_bytes = archive.read("info.json") if "info.json" in archive.namelist() else b"{}\n"
            marker_info = json.loads(info_bytes)
            if _contains_secret(marker_info):
                raise ValueError("legacy info.json contains secret-like fields; manual redaction required")
            marker_meta = marker_info.get("marker_meta", {})
            markdown, mappings = _strip_page_anchors(legacy_markdown)
            toc_mappings, locators = _heading_evidence(markdown, marker_meta)
            mappings.extend(toc_mappings)
            extra = [
                MdafMemberInput(
                    "renditions/dev.tionis.blobforge.legacy/content.md",
                    legacy_markdown_bytes,
                    "rendition",
                    "activity:legacy-convert",
                    "text/markdown",
                    namespace="dev.tionis.blobforge.legacy",
                ),
                MdafMemberInput(
                    "renditions/dev.tionis.blobforge.legacy/info.json",
                    info_bytes,
                    "rendition",
                    "activity:migrate",
                    "application/json",
                    namespace="dev.tionis.blobforge.legacy",
                ),
            ]
            for item in members:
                if item.filename.startswith("assets/"):
                    extra.append(
                        MdafMemberInput(
                            item.filename,
                            archive.read(item),
                            "asset",
                            "activity:legacy-convert",
                        )
                    )

        original_name = marker_info.get("original_filename") or f"{legacy_sha256}.pdf"
        parameters = {
            "legacy_format": "blobforge-zip-v0",
            "marker_version": "unavailable",
            "page_mapping": "anchors-and-exact-toc-heading-alignment",
            "source_embedded": False,
        }
        conversion_activity = activity(
            activity_id="activity:legacy-convert",
            kind="document-extraction",
            tools=[{"name": "marker-pdf", "version": "unavailable"}],
            models=[{"provider": "marker", "identifier": "unavailable", "resolution": "unavailable"}],
            inputs=["source:document"],
            outputs=["renditions/dev.tionis.blobforge.legacy/content.md"]
            + [item.path for item in extra if item.created_by == "activity:legacy-convert"
               and item.path != "renditions/dev.tionis.blobforge.legacy/content.md"],
            parameters={"recovered_provenance": True},
        )
        migrate_activity = activity(
            activity_id="activity:migrate",
            kind="artifact-migration",
            tools=[{"name": "blobforge", "version": version("blobforge")}],
            inputs=["source:document", "renditions/dev.tionis.blobforge.legacy/content.md"],
            outputs=[
                "text.md",
                "source-map.json",
                "outline.json",
                "provenance.json",
                "renditions/dev.tionis.blobforge.legacy/info.json",
            ],
            parameters=parameters,
            depends_on=["activity:legacy-convert"],
        )
        digest_hex = source_digest.removeprefix("blake3:")
        destination = root / "generated" / digest_hex[:2] / f"{digest_hex}.legacy.mdaf"
        result = build_mdaf(
            destination,
            text=markdown,
            title=Path(str(original_name)).stem,
            sources=[
                MdafSource(
                    "document",
                    "application/pdf",
                    source_digest,
                    (f"sha256:{legacy_sha256}",),
                    str(original_name),
                )
            ],
            activities=[conversion_activity, migrate_activity],
            producer={"name": "blobforge", "version": version("blobforge")},
            extra_members=extra,
            source_map={"mappings": mappings, "references": []},
            outline=_outline(markdown, locators),
            primary_created_by="activity:migrate",
        )
        validated = validate_mdaf(result.path)
        if validated.identity != result.identity:
            raise ValueError("post-write MDAF identity mismatch")
        with connection:
            connection.execute(
                """UPDATE sources SET blake3=?, sha256_verified=1 WHERE legacy_sha256=?""",
                (source_digest, legacy_sha256),
            )
            connection.execute(
                """UPDATE legacy_artifacts SET status='converted', output_path=?,
                   mdaf_identity=?, error=NULL WHERE legacy_sha256=?""",
                (str(destination.resolve()), result.identity, legacy_sha256),
            )
        return destination
    except Exception as exc:
        with connection:
            connection.execute(
                "UPDATE legacy_artifacts SET status='failed', error=? WHERE legacy_sha256=?",
                (str(exc), legacy_sha256),
            )
        raise
    finally:
        connection.close()


def pending_hashes(workspace: str | Path = DEFAULT_WORKSPACE, limit: int | None = None) -> list[str]:
    connection = _connect(Path(workspace) / "catalog.sqlite3")
    try:
        query = """SELECT a.legacy_sha256 FROM legacy_artifacts a
                   JOIN sources s USING(legacy_sha256)
                   WHERE a.status != 'converted' ORDER BY a.legacy_sha256"""
        params: tuple[Any, ...] = ()
        if limit is not None:
            query += " LIMIT ?"
            params = (limit,)
        return [row[0] for row in connection.execute(query, params)]
    finally:
        connection.close()


def verify_outputs(
    workspace: str | Path = DEFAULT_WORKSPACE,
    limit: int | None = None,
) -> VerificationSummary:
    """Read back converted artifacts and cross-check them against the catalog.

    Verification is deliberately read-only: a failed audit must not silently
    rewrite resumability state while another migration process may be running.
    """
    root = Path(workspace)
    connection = _connect(root / "catalog.sqlite3")
    try:
        query = """SELECT a.legacy_sha256, s.blake3 AS source_digest,
                          a.output_path, a.mdaf_identity
                   FROM legacy_artifacts a JOIN sources s USING(legacy_sha256)
                   WHERE a.status='converted' ORDER BY a.legacy_sha256"""
        params: tuple[Any, ...] = ()
        if limit is not None:
            query += " LIMIT ?"
            params = (limit,)
        rows = list(connection.execute(query, params))
    finally:
        connection.close()

    errors: list[str] = []
    for row in rows:
        digest = row["legacy_sha256"]
        try:
            if not row["output_path"]:
                raise ValueError("catalog has no output path")
            result = validate_mdaf(row["output_path"])
            if result.identity != row["mdaf_identity"]:
                raise ValueError(
                    f"identity differs from catalog: {result.identity} != {row['mdaf_identity']}"
                )
            sources = result.manifest.get("sources", [])
            if len(sources) != 1 or sources[0].get("digest") != row["source_digest"]:
                raise ValueError("source BLAKE3 digest differs from catalog")
            alternate = sources[0].get("alternate_digests", [])
            if f"sha256:{digest}" not in alternate:
                raise ValueError("legacy SHA-256 alias is absent")
        except Exception as exc:
            errors.append(f"{digest}: {exc}")
    return VerificationSummary(len(rows), len(rows) - len(errors), tuple(errors))


def _link_or_copy_verified(source: Path, destination: Path, expected_digest: str | None = None) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if destination.stat().st_size != source.stat().st_size:
            raise ValueError(f"staged object has wrong size: {destination}")
        if expected_digest and blake3_file(destination) != expected_digest:
            raise ValueError(f"staged object has wrong digest: {destination}")
        if not expected_digest and blake3_file(destination) != blake3_file(source):
            raise ValueError(f"staged object differs from input: {destination}")
        return
    try:
        os.link(source, destination)
    except OSError:
        shutil.copy2(source, destination)


def stage_v2(
    workspace: str | Path = DEFAULT_WORKSPACE,
    output: str | Path | None = None,
    run_id: str = "legacy-mdaf-v1",
) -> StageSummary:
    """Materialize a verified, rclone-shaped local v2 tree without publishing it."""
    root = Path(workspace)
    verification = verify_outputs(root)
    if verification.errors:
        raise ValueError(
            f"refusing to stage {len(verification.errors)} invalid converted artifact(s)"
        )
    connection = _connect(root / "catalog.sqlite3")
    try:
        rows = list(
            connection.execute(
                """SELECT a.legacy_sha256, s.raw_path, s.blake3 AS source_digest,
                          a.output_path, a.mdaf_identity
                   FROM legacy_artifacts a JOIN sources s USING(legacy_sha256)
                   WHERE a.status='converted' ORDER BY a.legacy_sha256"""
            )
        )
        paired = connection.execute(
            "SELECT count(*) FROM legacy_artifacts a JOIN sources s USING(legacy_sha256)"
        ).fetchone()[0]
    finally:
        connection.close()
    if len(rows) != paired:
        raise ValueError(f"refusing partial stage: {len(rows)} of {paired} pairs are converted")

    destination = Path(output) if output else root / "staged-v2"
    recipe = {
        "schema": "dev.tionis.blobforge.recipe/v2",
        "pipeline": "legacy-zip-to-mdaf-v1",
        "historical_converter": {"name": "marker-pdf", "version": "unavailable"},
        "source_mapping": "page-anchors-and-exact-toc-heading-alignment",
        "primary_markdown": "legacy-content-md",
    }
    recipe_bytes = canonical_json_bytes(recipe)
    recipe_digest = blake3_bytes(recipe_bytes)
    recipe_path = destination / recipe_key(recipe_digest)
    recipe_path.parent.mkdir(parents=True, exist_ok=True)
    if recipe_path.exists() and recipe_path.read_bytes() != recipe_bytes:
        raise ValueError(f"staged recipe differs from canonical content: {recipe_path}")
    recipe_path.write_bytes(recipe_bytes)

    entries: list[dict[str, str]] = []
    for row in rows:
        source_object = source_key(row["source_digest"])
        attempt_id = f"legacy-{row['legacy_sha256'][:16]}"
        artifact_object = artifact_key(row["mdaf_identity"], attempt_id)
        _link_or_copy_verified(
            Path(row["raw_path"]), destination / source_object, row["source_digest"]
        )
        _link_or_copy_verified(Path(row["output_path"]), destination / artifact_object)
        entries.append(
            {
                "legacy_sha256": row["legacy_sha256"],
                "source_digest": row["source_digest"],
                "source_key": source_object,
                "recipe_digest": recipe_digest,
                "artifact_identity": row["mdaf_identity"],
                "artifact_key": artifact_object,
                "attempt_id": attempt_id,
            }
        )
    stage_body = {
        "format": "blobforge-v2-local-stage",
        "version": 1,
        "run_id": run_id,
        "entries": entries,
    }
    stage_manifest = {
        **stage_body,
        "manifest_digest": blake3_bytes(canonical_json_bytes(stage_body)),
    }
    manifest_path = destination / migration_manifest_key(run_id)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(stage_manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return StageSummary(len(rows), len(rows), recipe_digest, destination)


def export_manifest(
    workspace: str | Path = DEFAULT_WORKSPACE,
    output: str | Path | None = None,
) -> Path:
    """Export the resumable catalog as a checksummed publication input."""
    root = Path(workspace)
    connection = _connect(root / "catalog.sqlite3")
    try:
        rows = [
            dict(row)
            for row in connection.execute(
                """SELECT a.legacy_sha256, s.blake3 AS source_digest,
                          s.size_bytes AS source_size_bytes, a.size_bytes AS legacy_size_bytes,
                          a.status, a.output_path, a.mdaf_identity, a.error
                   FROM legacy_artifacts a JOIN sources s USING(legacy_sha256)
                   ORDER BY a.legacy_sha256"""
            )
        ]
    finally:
        connection.close()
    body = {
        "format": "blobforge-legacy-mdaf-migration",
        "version": 1,
        "entries": rows,
    }
    value = {**body, "manifest_digest": blake3_bytes(canonical_json_bytes(body))}
    destination = Path(output) if output else root / "migration-manifest.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return destination
