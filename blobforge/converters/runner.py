"""Subprocess runner and shared MDAF packager for converter adapters."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
import time
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..mdaf import MdafMemberInput, MdafSource, blake3_file, build_mdaf, validate_mdaf
from ..mdaf.builder import activity, markdown_outline
from .contract import ConversionRequest, load_bundle


@dataclass(frozen=True)
class ConverterRunResult:
    artifact_path: Path
    identity: str
    elapsed_seconds: float
    diagnostics: tuple[Mapping[str, Any], ...]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def run_converter(
    command: Sequence[str],
    source_path: str | Path,
    output_path: str | Path,
    *,
    parameters: Mapping[str, Any] | None = None,
    timeout_seconds: int = 86_400,
    environment: Mapping[str, str] | None = None,
) -> ConverterRunResult:
    """Run one isolated adapter and package its validated bundle as MDAF."""
    source = Path(source_path).resolve()
    if not source.is_file():
        raise ValueError(f"source is not a file: {source}")
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="blobforge-converter-") as temporary:
        root = Path(temporary)
        bundle_root = root / "bundle"
        bundle_root.mkdir()
        request = ConversionRequest(source, bundle_root, parameters or {})
        request_path = root / "request.json"
        request_path.write_text(json.dumps(request.as_json()), encoding="utf-8")
        process_environment = os.environ.copy()
        if environment:
            process_environment.update(environment)
        completed = subprocess.run(
            [*command, str(request_path)],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            env=process_environment,
        )
        if completed.returncode:
            raise RuntimeError(
                f"converter exited {completed.returncode}: {completed.stderr[-4000:]}"
            )
        bundle = load_bundle(bundle_root)
        text = bundle.text_path.read_text(encoding="utf-8")
        effective_outline = bundle.outline or markdown_outline(text)
        conversion = activity(
            activity_id="activity:convert",
            kind="document-extraction",
            tools=[bundle.tool],
            models=list(bundle.models),
            inputs=["source:document"],
            outputs=["text.md", "provenance.json"]
            + [member.artifact_path for member in bundle.members]
            + (["source-map.json"] if bundle.source_map is not None else [])
            + ["outline.json"],
            parameters=bundle.parameters,
        )
        extra_members = [
            MdafMemberInput(
                member.artifact_path,
                member.file_path.read_bytes(),
                member.role,
                "activity:convert",
                member.media_type,
                member.schema,
                member.namespace,
            )
            for member in bundle.members
        ]
        result = build_mdaf(
            output_path,
            text=text,
            title=source.stem,
            sources=[
                MdafSource(
                    "document",
                    "application/pdf",
                    blake3_file(source),
                    (f"sha256:{_sha256_file(source)}",),
                    source.name,
                )
            ],
            activities=[conversion],
            producer={"name": "blobforge", "version": version("blobforge")},
            extra_members=extra_members,
            source_map=bundle.source_map,
            outline=effective_outline,
        )
        validated = validate_mdaf(result.path)
        if validated.identity != result.identity:
            raise RuntimeError("MDAF changed during post-build validation")
        return ConverterRunResult(
            artifact_path=result.path,
            identity=result.identity,
            elapsed_seconds=time.monotonic() - started,
            diagnostics=bundle.diagnostics,
        )
