"""Shared deterministic MDAF v1 builder used by every converter adapter."""

from __future__ import annotations

import mimetypes
import os
import re
import tempfile
import unicodedata
import zipfile
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Mapping

from .digest import blake3_bytes, json_document_bytes, logical_identity, parameters_digest

PROVENANCE_SCHEMA = "https://vulcan.tionis.dev/schemas/mdaf/v1/provenance.schema.json"
SOURCE_MAP_SCHEMA = "https://vulcan.tionis.dev/schemas/mdaf/v1/source-map.schema.json"
OUTLINE_SCHEMA = "https://vulcan.tionis.dev/schemas/mdaf/v1/outline.schema.json"
MARKDOWN_HEADING_RE = re.compile(r"^(#{1,6})[ \t]+(.+?)\s*$", re.MULTILINE)


@dataclass(frozen=True)
class MdafSource:
    id: str
    media_type: str
    digest: str
    alternate_digests: tuple[str, ...] = ()
    name: str | None = None
    embedded_path: str | None = None


@dataclass(frozen=True)
class MdafMemberInput:
    path: str
    data: bytes
    role: str
    created_by: str
    media_type: str | None = None
    schema: str | None = None
    namespace: str | None = None


@dataclass(frozen=True)
class MdafBuildResult:
    path: Path
    identity: str
    size: int
    member_count: int
    manifest: Mapping[str, Any] = field(repr=False)


def _validate_member_path(path: str) -> None:
    pure = PurePosixPath(path)
    if not path or path.startswith(("/", "\\")) or "\\" in path:
        raise ValueError(f"unsafe MDAF member path: {path!r}")
    if pure.is_absolute() or any(part in {"", ".", ".."} for part in pure.parts):
        raise ValueError(f"unsafe MDAF member path: {path!r}")
    if unicodedata.normalize("NFC", path) != path or any(ord(ch) < 32 for ch in path):
        raise ValueError(f"non-normalized MDAF member path: {path!r}")
    if len(pure.parts[0]) == 2 and pure.parts[0][1:] == ":":
        raise ValueError(f"Windows drive path is forbidden: {path!r}")


def _member_media_type(path: str) -> str:
    if path.endswith(".md"):
        return "text/markdown"
    guessed, _ = mimetypes.guess_type(path)
    return guessed or "application/octet-stream"


def activity(
    *,
    activity_id: str,
    kind: str,
    tools: list[Mapping[str, Any]],
    inputs: list[str],
    outputs: list[str],
    parameters: Mapping[str, Any],
    models: list[Mapping[str, Any]] | None = None,
    depends_on: list[str] | None = None,
    started_at: str | None = None,
    ended_at: str | None = None,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "id": activity_id,
        "kind": kind,
        "tools": tools,
        "models": models or [],
        "inputs": inputs,
        "outputs": outputs,
        "depends_on": depends_on or [],
        "parameters": dict(parameters),
        "parameters_digest": parameters_digest(parameters),
    }
    if started_at:
        value["started_at"] = started_at
    if ended_at:
        value["ended_at"] = ended_at
    return value


def markdown_outline(text: str, *, skip_fences: bool = False) -> dict[str, Any]:
    """Derive a conservative byte-aligned outline from ATX headings."""
    candidates = []
    fenced_spans = []
    if skip_fences:
        offset = 0
        fence = None
        for line in text.splitlines(keepends=True):
            match = re.match(r"^ {0,3}(`{3,}|~{3,})(.*)$", line.rstrip("\r\n"))
            if match:
                marker, tail = match.groups()
                if fence is None and not (marker[0] == "`" and "`" in tail):
                    fence = (marker[0], len(marker), offset)
                elif fence and marker[0] == fence[0] and len(marker) >= fence[1] and not tail.strip():
                    fenced_spans.append((fence[2], offset + len(line)))
                    fence = None
            offset += len(line)
        if fence:
            fenced_spans.append((fence[2], len(text)))
    for heading in MARKDOWN_HEADING_RE.finditer(text):
        if any(start <= heading.start() < end for start, end in fenced_spans):
            continue
        title = re.sub(r"<[^>]+>", "", heading.group(2))
        title = re.sub(r"!?\[([^]]*)\]\([^)]*\)", r"\1", title)
        title = re.sub(r"[*_`~]", "", title).strip()
        if title:
            candidates.append((heading, title))
    nodes: list[dict[str, Any]] = []
    parents: list[tuple[int, str]] = []
    document_end = len(text.encode("utf-8"))
    for index, (heading, title) in enumerate(candidates):
        level = len(heading.group(1))
        while parents and parents[-1][0] >= level:
            parents.pop()
        node_id = f"heading-{index + 1}"
        start = len(text[: heading.start()].encode("utf-8"))
        heading_end = len(text[: heading.end()].encode("utf-8"))
        section_end = document_end
        for following, _ in candidates[index + 1 :]:
            if len(following.group(1)) <= level:
                section_end = len(text[: following.start()].encode("utf-8"))
                break
        nodes.append(
            {
                "id": node_id,
                "parent": parents[-1][1] if parents else None,
                "level": level,
                "title": title,
                "heading": {"start": start, "end": heading_end},
                "section": {"start": start, "end": section_end},
            }
        )
        parents.append((level, node_id))
    return {"nodes": nodes}


def build_mdaf(
    output_path: str | Path,
    *,
    text: str,
    sources: Iterable[MdafSource],
    activities: Iterable[Mapping[str, Any]],
    producer: Mapping[str, str],
    title: str | None = None,
    extra_members: Iterable[MdafMemberInput] = (),
    source_map: Mapping[str, Any] | None = None,
    outline: Mapping[str, Any] | None = None,
    redactions: Iterable[Mapping[str, Any]] = (),
    markdown_variant: str | None = "CommonMark",
    markdown_features: Iterable[str] = (),
    derived_from: Iterable[str] = (),
    primary_created_by: str | None = None,
) -> MdafBuildResult:
    """Build an atomic deterministic ZIP-form MDAF.

    Converter adapters provide finalized Markdown and native evidence; only this
    function owns control-document serialization, member digests, and packaging.
    """
    destination = Path(output_path)
    if destination.suffix != ".mdaf":
        raise ValueError("MDAF ZIP output must end in .mdaf")

    activity_values = [dict(item) for item in activities]
    activity_ids = {item.get("id") for item in activity_values}
    if None in activity_ids or len(activity_ids) != len(activity_values):
        raise ValueError("activity IDs must be present and unique")
    primary_activity = primary_created_by or activity_values[0]["id"]
    if primary_activity not in activity_ids:
        raise ValueError(f"unknown primary activity {primary_activity!r}")

    text_bytes = text.encode("utf-8")
    text_digest = blake3_bytes(text_bytes)
    members = [
        MdafMemberInput(
            "text.md", text_bytes, "primary", primary_activity, "text/markdown"
        )
    ]

    capabilities: list[str] = []
    if source_map is not None:
        source_map_value = dict(source_map)
        source_map_value["version"] = 1
        source_map_value["document_digest"] = text_digest
        source_map_value.setdefault("mappings", [])
        source_map_value.setdefault("references", [])
        members.append(
            MdafMemberInput(
                "source-map.json",
                json_document_bytes(source_map_value),
                "source-map",
                activity_values[-1]["id"],
                "application/json",
                SOURCE_MAP_SCHEMA,
            )
        )
        capabilities.append("source-map")
    if outline is not None:
        outline_value = dict(outline)
        outline_value["version"] = 1
        outline_value["document_digest"] = text_digest
        outline_value.setdefault("nodes", [])
        members.append(
            MdafMemberInput(
                "outline.json",
                json_document_bytes(outline_value),
                "outline",
                activity_values[-1]["id"],
                "application/json",
                OUTLINE_SCHEMA,
            )
        )
        capabilities.append("outline")

    extras = list(extra_members)
    members.extend(extras)
    if any(item.role == "rendition" for item in extras):
        capabilities.append("native-renditions")
    if any(item.role == "source" for item in extras):
        capabilities.append("embedded-sources")
    if any(item.role == "environment" for item in extras):
        capabilities.append("environments")
    if any(item.role == "extension" for item in extras):
        capabilities.append("extensions")

    provenance = {
        "version": 1,
        "activities": activity_values,
        "redactions": list(redactions),
    }
    members.append(
        MdafMemberInput(
            "provenance.json",
            json_document_bytes(provenance),
            "provenance",
            activity_values[-1]["id"],
            "application/json",
            PROVENANCE_SCHEMA,
        )
    )

    folded: set[str] = set()
    member_values: list[dict[str, Any]] = []
    member_bytes: dict[str, bytes] = {}
    for item in members:
        _validate_member_path(item.path)
        key = item.path.casefold()
        if key in folded:
            raise ValueError(f"duplicate or case-fold-colliding member: {item.path}")
        folded.add(key)
        if item.created_by not in activity_ids:
            raise ValueError(f"unknown creating activity {item.created_by!r}")
        value: dict[str, Any] = {
            "path": item.path,
            "role": item.role,
            "media_type": item.media_type or _member_media_type(item.path),
            "size": len(item.data),
            "digest": blake3_bytes(item.data),
            "created_by": item.created_by,
        }
        if item.schema:
            value["schema"] = item.schema
        if item.namespace:
            value["namespace"] = item.namespace
        member_values.append(value)
        member_bytes[item.path] = item.data

    source_values = []
    for source in sources:
        value: dict[str, Any] = {
            "id": source.id,
            "media_type": source.media_type,
            "digest": source.digest,
        }
        if source.alternate_digests:
            value["alternate_digests"] = list(source.alternate_digests)
        if source.name is not None:
            value["name"] = source.name
        if source.embedded_path is not None:
            value["embedded_path"] = source.embedded_path
        source_values.append(value)

    markdown: dict[str, Any] = {
        "path": "text.md",
        "digest": text_digest,
        "media_type": "text/markdown",
    }
    if markdown_variant:
        markdown["variant"] = markdown_variant
    features = sorted(set(markdown_features))
    if features:
        markdown["features"] = features

    manifest: dict[str, Any] = {
        "format": "mdaf",
        "version": 1,
        "markdown": markdown,
        "producer": dict(producer),
        "members": sorted(member_values, key=lambda item: item["path"].encode("utf-8")),
        "sources": source_values,
        "capabilities": sorted(set(capabilities)),
    }
    if title is not None:
        manifest["title"] = title
    parents = sorted(set(derived_from))
    if parents:
        manifest["derived_from"] = parents
    member_bytes["info.json"] = json_document_bytes(manifest)

    records = [
        (path, len(data), blake3_bytes(data)) for path, data in member_bytes.items()
    ]
    identity = logical_identity(records)

    destination.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
    )
    os.close(file_descriptor)
    temporary = Path(temporary_name)
    try:
        with zipfile.ZipFile(
            temporary, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6
        ) as archive:
            for path in sorted(member_bytes, key=lambda value: value.encode("utf-8")):
                info = zipfile.ZipInfo(path, date_time=(1980, 1, 1, 0, 0, 0))
                info.compress_type = zipfile.ZIP_DEFLATED
                info.create_system = 3
                info.external_attr = 0o100644 << 16
                archive.writestr(info, member_bytes[path])
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)

    return MdafBuildResult(
        path=destination,
        identity=identity,
        size=destination.stat().st_size,
        member_count=len(member_bytes),
        manifest=manifest,
    )
