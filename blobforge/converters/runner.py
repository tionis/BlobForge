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

from ..mdaf import (
    MdafMemberInput,
    MdafSource,
    blake3_file,
    build_mdaf,
    canonical_json_bytes,
    validate_mdaf,
)
from ..mdaf.builder import activity, markdown_outline
from ..recipe_lifecycle import (
    RECIPE_MEMBER_PATH,
    parse_recipe_lifecycle,
    recipe_digest,
)
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
    recipe: Mapping[str, Any] | None = None,
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
        lifecycle = None
        recipe_member = None
        if recipe is not None and recipe.get("schema") == "dev.tionis.blobforge.recipe/v3":
            lifecycle = parse_recipe_lifecycle(recipe)
            if bundle.parameters.get("recipe_digest") != lifecycle.digest:
                raise ValueError("adapter bundle recipe digest does not match embedded recipe")
            recipe_member = MdafMemberInput(
                RECIPE_MEMBER_PATH,
                canonical_json_bytes(recipe),
                "extension",
                "activity:postprocess",
                "application/json",
                namespace="dev.tionis.blobforge",
            )

        generated_paths = [member.artifact_path for member in bundle.members]
        generated_paths += ["source-map.json"] if bundle.source_map is not None else []
        generated_paths += ["outline.json"]
        if lifecycle is None:
            activities = [
                activity(
                    activity_id="activity:convert",
                    kind="document-extraction",
                    tools=list(bundle.tools),
                    models=list(bundle.models),
                    inputs=["source:document"],
                    outputs=["text.md", "provenance.json", *generated_paths],
                    parameters=bundle.parameters,
                )
            ]
            primary_activity = "activity:convert"
        else:
            if len(bundle.tools) < 2:
                raise ValueError(
                    "lifecycle recipe requires distinct extraction and post-processing tools"
                )
            native_paths = set(lifecycle.native_members)
            observed_paths = {member.artifact_path for member in bundle.members}
            missing_native = native_paths - observed_paths
            if missing_native:
                raise ValueError(
                    f"adapter omitted lifecycle native evidence: {sorted(missing_native)}"
                )
            extraction_parameters = {
                key: value
                for key, value in bundle.parameters.items()
                if key not in {"normalization_profile", "recipe_digest"}
            }
            extraction_parameters["recipe_digest"] = (
                lifecycle.extraction_recipe_digest
            )
            postprocess_paths = [
                path for path in generated_paths if path not in native_paths
            ]
            postprocess_paths.extend([RECIPE_MEMBER_PATH, "text.md", "provenance.json"])
            activities = [
                activity(
                    activity_id="activity:extract",
                    kind="document-extraction",
                    tools=[dict(bundle.tools[0])],
                    models=list(bundle.models),
                    inputs=["source:document"],
                    outputs=sorted(native_paths),
                    parameters=extraction_parameters,
                ),
                activity(
                    activity_id="activity:postprocess",
                    kind="document-normalization",
                    tools=[dict(tool) for tool in bundle.tools[1:]],
                    inputs=sorted(native_paths),
                    outputs=sorted(set(postprocess_paths)),
                    parameters={
                        "recipe_digest": recipe_digest(recipe),
                        "recipe_version": (
                            f"{lifecycle.version.major}."
                            f"{lifecycle.version.minor}."
                            f"{lifecycle.version.patch}"
                        ),
                        "profile": lifecycle.postprocessing_profile,
                        "postprocessing_version": (
                            f"{lifecycle.postprocessing_version.major}."
                            f"{lifecycle.postprocessing_version.minor}."
                            f"{lifecycle.postprocessing_version.patch}"
                        ),
                    },
                    depends_on=["activity:extract"],
                ),
            ]
            primary_activity = "activity:postprocess"
        extra_members = [
            MdafMemberInput(
                member.artifact_path,
                member.file_path.read_bytes(),
                member.role,
                (
                    "activity:extract"
                    if lifecycle is not None
                    and member.artifact_path in set(lifecycle.native_members)
                    else primary_activity
                ),
                member.media_type,
                member.schema,
                member.namespace,
            )
            for member in bundle.members
        ]
        if recipe_member is not None:
            extra_members.append(recipe_member)
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
            activities=activities,
            producer={"name": "blobforge", "version": version("blobforge")},
            extra_members=extra_members,
            source_map=bundle.source_map,
            outline=effective_outline,
            markdown_variant=bundle.markdown_variant,
            markdown_features=bundle.markdown_features,
            primary_created_by=primary_activity,
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
