"""Fail-closed structural and semantic checks for generated MDAF v1 ZIPs."""

from __future__ import annotations

import json
import re
import unicodedata
import zipfile
from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files
from pathlib import Path, PurePosixPath
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

from .digest import blake3_bytes, logical_identity, parameters_digest

MAX_FILES = 100_000
MAX_TOTAL_BYTES = 8 * 1024**3
MAX_MEMBER_BYTES = 2 * 1024**3
MAX_NON_ASSET_BYTES = 512 * 1024**2
MAX_RATIO = 1_000
MARKDOWN_LINK_RE = re.compile(r"!?\[[^\]]*\]\(([^)\s]+)\)")


class MdafValidationError(ValueError):
    pass


@dataclass(frozen=True)
class ValidationResult:
    identity: str
    member_count: int
    size: int
    manifest: dict[str, Any]


def _path_is_safe(path: str) -> bool:
    pure = PurePosixPath(path)
    return bool(
        path
        and not path.startswith(("/", "\\"))
        and "\\" not in path
        and not pure.is_absolute()
        and all(part not in {"", ".", ".."} for part in pure.parts)
        and unicodedata.normalize("NFC", path) == path
        and not any(ord(ch) < 32 for ch in path)
    )


def _json_member(archive: zipfile.ZipFile, path: str) -> dict[str, Any]:
    try:
        value = json.loads(archive.read(path))
    except (KeyError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MdafValidationError(f"invalid or missing {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise MdafValidationError(f"{path} must contain a JSON object")
    return value


@lru_cache(maxsize=None)
def _schema_validator(name: str) -> Draft202012Validator:
    schema_text = files("blobforge.mdaf.schemas").joinpath(name).read_text(encoding="utf-8")
    schema = json.loads(schema_text)
    return Draft202012Validator(schema, format_checker=FormatChecker())


def _validate_schema(name: str, value: dict[str, Any]) -> None:
    errors = sorted(
        _schema_validator(name).iter_errors(value),
        key=lambda item: list(item.absolute_path),
    )
    if errors:
        error = errors[0]
        location = "/".join(str(item) for item in error.absolute_path) or "<root>"
        raise MdafValidationError(f"{name} schema error at {location}: {error.message}")


def _validate_span(span: Any, text: bytes, location: str) -> None:
    if not isinstance(span, dict) or set(span) != {"start", "end"}:
        raise MdafValidationError(f"invalid byte span at {location}")
    start, end = span["start"], span["end"]
    if not isinstance(start, int) or not isinstance(end, int) or not 0 <= start <= end <= len(text):
        raise MdafValidationError(f"out-of-bounds byte span at {location}")
    # The complete document has already decoded as UTF-8. A byte offset splits
    # a code point iff it lands on a continuation byte (10xxxxxx); checking the
    # two boundaries is constant-time and avoids quadratic prefix decoding for
    # documents with hundreds of outline/source-map spans.
    for offset in (start, end):
        if offset < len(text) and text[offset] & 0xC0 == 0x80:
            raise MdafValidationError(f"byte span splits UTF-8 at {location}")


def validate_mdaf(path: str | Path) -> ValidationResult:
    artifact = Path(path)
    if artifact.suffix != ".mdaf":
        raise MdafValidationError("ZIP-form artifact must end in .mdaf")
    with zipfile.ZipFile(artifact) as archive:
        entries = archive.infolist()
        if len(entries) > MAX_FILES:
            raise MdafValidationError("artifact exceeds member-count limit")
        folded: set[str] = set()
        observed: dict[str, tuple[int, str]] = {}
        total = 0
        for entry in entries:
            path_value = entry.filename
            if entry.is_dir():
                continue
            if not _path_is_safe(path_value):
                raise MdafValidationError(f"unsafe member path: {path_value!r}")
            folded_path = path_value.casefold()
            if folded_path in folded:
                raise MdafValidationError(f"duplicate member path: {path_value}")
            folded.add(folded_path)
            if entry.flag_bits & 1:
                raise MdafValidationError(f"encrypted member: {path_value}")
            limit = MAX_MEMBER_BYTES if path_value.startswith("assets/") else MAX_NON_ASSET_BYTES
            if entry.file_size > limit:
                raise MdafValidationError(f"oversized member: {path_value}")
            if entry.file_size and (
                entry.compress_size == 0 or entry.file_size // max(entry.compress_size, 1) > MAX_RATIO
            ):
                raise MdafValidationError(f"excessive compression ratio: {path_value}")
            total += entry.file_size
            if total > MAX_TOTAL_BYTES:
                raise MdafValidationError("artifact exceeds expanded-size limit")
            data = archive.read(entry)
            observed[path_value] = (len(data), blake3_bytes(data))

        required = {"info.json", "text.md", "provenance.json"}
        if not required <= observed.keys():
            raise MdafValidationError(f"missing required members: {sorted(required - observed.keys())}")
        manifest = _json_member(archive, "info.json")
        _validate_schema("info.schema.json", manifest)
        if manifest.get("format") != "mdaf" or manifest.get("version") != 1:
            raise MdafValidationError("unsupported MDAF format/version")
        declared = manifest.get("members")
        if not isinstance(declared, list):
            raise MdafValidationError("info.json.members must be an array")
        by_path: dict[str, dict[str, Any]] = {}
        for member in declared:
            if not isinstance(member, dict) or not isinstance(member.get("path"), str):
                raise MdafValidationError("invalid declared member")
            member_path = member["path"]
            if member_path == "info.json" or member_path in by_path:
                raise MdafValidationError(f"invalid duplicate declaration: {member_path}")
            by_path[member_path] = member
            actual = observed.get(member_path)
            if actual is None or actual != (member.get("size"), member.get("digest")):
                raise MdafValidationError(f"member digest/size mismatch: {member_path}")
        if set(by_path) != set(observed) - {"info.json"}:
            raise MdafValidationError("declared and observed member sets differ")

        text = archive.read("text.md")
        markdown_text = text.decode("utf-8")
        for target in MARKDOWN_LINK_RE.findall(markdown_text):
            lowered = target.casefold()
            if target.startswith(("/", "\\")) or lowered.startswith("file:") or re.match(
                r"^[a-zA-Z]:[\\/]", target
            ):
                raise MdafValidationError(f"absolute local Markdown target is forbidden: {target}")
            if target.startswith("assets/") and target.split("#", 1)[0].split("?", 1)[0] not in observed:
                raise MdafValidationError(f"Markdown references missing asset: {target}")
        markdown = manifest.get("markdown", {})
        if markdown.get("path") != "text.md" or markdown.get("digest") != blake3_bytes(text):
            raise MdafValidationError("Markdown binding mismatch")
        source_ids = {item.get("id") for item in manifest.get("sources", []) if isinstance(item, dict)}

        provenance = _json_member(archive, "provenance.json")
        _validate_schema("provenance.schema.json", provenance)
        activities = provenance.get("activities")
        if not isinstance(activities, list) or not activities:
            raise MdafValidationError("provenance requires at least one activity")
        activity_ids = {item.get("id") for item in activities if isinstance(item, dict)}
        if len(activity_ids) != len(activities) or None in activity_ids:
            raise MdafValidationError("activity IDs must be unique")
        for item in activities:
            if item.get("parameters_digest") != parameters_digest(item.get("parameters", {})):
                raise MdafValidationError(f"parameters digest mismatch: {item.get('id')}")
        activity_outputs = {
            item["id"]: set(item.get("outputs", [])) for item in activities
        }
        for member in declared:
            if member.get("created_by") not in activity_ids:
                raise MdafValidationError(f"unknown creating activity: {member.get('path')}")
            if member.get("path") not in activity_outputs[member["created_by"]]:
                raise MdafValidationError(
                    f"member is not emitted by its activity: {member.get('path')}"
                )

        if "source-map.json" in observed:
            source_map = _json_member(archive, "source-map.json")
            _validate_schema("source-map.schema.json", source_map)
            if source_map.get("document_digest") != blake3_bytes(text):
                raise MdafValidationError("source-map document binding mismatch")
            for index, mapping in enumerate(source_map.get("mappings", [])):
                _validate_span(mapping.get("document"), text, f"mappings[{index}]")
                if mapping.get("source", {}).get("source_id") not in source_ids:
                    raise MdafValidationError(f"unknown source in mapping {index}")
            for index, reference in enumerate(source_map.get("references", [])):
                _validate_span(reference.get("document"), text, f"references[{index}]")
                if reference.get("target", {}).get("source_id") not in source_ids:
                    raise MdafValidationError(f"unknown source in reference {index}")

        if "outline.json" in observed:
            outline = _json_member(archive, "outline.json")
            _validate_schema("outline.schema.json", outline)
            if outline.get("document_digest") != blake3_bytes(text):
                raise MdafValidationError("outline document binding mismatch")
            for index, node in enumerate(outline.get("nodes", [])):
                _validate_span(node.get("heading"), text, f"nodes[{index}].heading")
                _validate_span(node.get("section"), text, f"nodes[{index}].section")

        records = [(name, size, digest) for name, (size, digest) in observed.items()]
        return ValidationResult(
            identity=logical_identity(records),
            member_count=len(observed),
            size=artifact.stat().st_size,
            manifest=manifest,
        )
