"""Filesystem ABI shared by local and hosted converter adapters."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Mapping

CONTRACT = "dev.tionis.blobforge.converter-bundle/v1"


def _relative_path(root: Path, value: str) -> Path:
    pure = PurePosixPath(value)
    if not value or pure.is_absolute() or any(part in {"", ".", ".."} for part in pure.parts):
        raise ValueError(f"unsafe converter bundle path: {value!r}")
    candidate = (root / Path(*pure.parts)).resolve()
    if not candidate.is_relative_to(root.resolve()):
        raise ValueError(f"converter bundle path escapes output: {value!r}")
    return candidate


@dataclass(frozen=True)
class ConversionRequest:
    source_path: Path
    output_dir: Path
    parameters: Mapping[str, Any]

    def as_json(self) -> dict[str, Any]:
        return {
            "contract": CONTRACT,
            "source_path": str(self.source_path.resolve()),
            "output_dir": str(self.output_dir.resolve()),
            "parameters": dict(self.parameters),
        }


@dataclass(frozen=True)
class BundleMember:
    artifact_path: str
    file_path: Path
    role: str
    media_type: str
    schema: str | None
    namespace: str | None


@dataclass(frozen=True)
class ConversionBundle:
    root: Path
    text_path: Path
    members: tuple[BundleMember, ...]
    source_map: Mapping[str, Any] | None
    outline: Mapping[str, Any] | None
    tools: tuple[Mapping[str, Any], ...]
    models: tuple[Mapping[str, Any], ...]
    parameters: Mapping[str, Any]
    diagnostics: tuple[Mapping[str, Any], ...]
    markdown_variant: str | None
    markdown_features: tuple[str, ...]


def load_bundle(root: str | Path) -> ConversionBundle:
    bundle_root = Path(root).resolve()
    manifest_path = bundle_root / "bundle.json"
    value = json.loads(manifest_path.read_text(encoding="utf-8"))
    if value.get("contract") != CONTRACT:
        raise ValueError("unsupported converter bundle contract")
    text_path = _relative_path(bundle_root, value.get("text_path", ""))
    if not text_path.is_file():
        raise ValueError("converter bundle text is missing")
    text = text_path.read_bytes()
    text.decode("utf-8")
    if b"\x00" in text:
        raise ValueError("converter Markdown contains a forbidden NUL byte")

    members = []
    artifact_paths: set[str] = set()
    for item in value.get("members", []):
        artifact_path = item.get("path", "")
        if artifact_path in artifact_paths:
            raise ValueError(f"duplicate adapter member: {artifact_path}")
        artifact_paths.add(artifact_path)
        file_path = _relative_path(bundle_root, item.get("file", ""))
        if not file_path.is_file():
            raise ValueError(f"adapter member is missing: {file_path}")
        members.append(
            BundleMember(
                artifact_path=artifact_path,
                file_path=file_path,
                role=item["role"],
                media_type=item.get("media_type", "application/octet-stream"),
                schema=item.get("schema"),
                namespace=item.get("namespace"),
            )
        )

    def optional_json(key: str) -> Mapping[str, Any] | None:
        path_value = value.get(key)
        if path_value is None:
            return None
        parsed = json.loads(_relative_path(bundle_root, path_value).read_text(encoding="utf-8"))
        if not isinstance(parsed, dict):
            raise ValueError(f"{key} must identify a JSON object")
        return parsed

    def exact_tool(tool: Any) -> Mapping[str, Any]:
        if not isinstance(tool, dict) or not tool.get("name") or not tool.get("version"):
            raise ValueError("converter bundle needs exact tool names and versions")
        return tool

    tools = [exact_tool(value.get("tool"))]
    additional_tools = value.get("additional_tools", [])
    if not isinstance(additional_tools, list):
        raise ValueError("additional_tools must be an array")
    tools.extend(exact_tool(tool) for tool in additional_tools)

    markdown_variant = value.get("markdown_variant", "CommonMark")
    if markdown_variant is not None and (
        not isinstance(markdown_variant, str) or not markdown_variant
    ):
        raise ValueError("markdown_variant must be a non-empty string or null")
    markdown_features = value.get("markdown_features", [])
    if (
        not isinstance(markdown_features, list)
        or any(not isinstance(item, str) or not item for item in markdown_features)
        or len(set(markdown_features)) != len(markdown_features)
    ):
        raise ValueError("markdown_features must contain unique non-empty strings")
    return ConversionBundle(
        root=bundle_root,
        text_path=text_path,
        members=tuple(members),
        source_map=optional_json("source_map"),
        outline=optional_json("outline"),
        tools=tuple(tools),
        models=tuple(value.get("models", [])),
        parameters=value.get("parameters", {}),
        diagnostics=tuple(value.get("diagnostics", [])),
        markdown_variant=markdown_variant,
        markdown_features=tuple(markdown_features),
    )
