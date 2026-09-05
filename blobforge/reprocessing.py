"""Offline immutable MDAF derivatives from retained native extraction evidence."""

from __future__ import annotations

import json
import zipfile
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from dataclasses import replace
from typing import Any, Mapping

from .mdaf import (
    MdafMemberInput,
    MdafSource,
    build_mdaf,
    canonical_json_bytes,
    validate_mdaf,
)
from .mdaf.builder import activity, markdown_outline
from .converters.runner import source_display_name
from .normalization import referenced_asset_names, render_mistral_response
from .recipe_lifecycle import (
    PARENT_INFO_PATH,
    PARENT_PROVENANCE_PATH,
    PREVIOUS_RECIPE_PATH,
    RECIPE_MEMBER_PATH,
    assert_reprocessable,
    load_known_recipe,
    recipe_digest,
)


@dataclass(frozen=True)
class ReprocessResult:
    path: Path
    identity: str
    parent_identity: str
    source_recipe_digest: str
    target_recipe_digest: str
    normalization_stats: Mapping[str, int] | None


def _json(data: bytes, path: str) -> dict[str, Any]:
    try:
        value = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid JSON in retained member {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"retained member {path} must contain a JSON object")
    return value


def _source_recipe(
    archive: zipfile.ZipFile,
    provenance: Mapping[str, Any],
    *,
    recipe_root: str | Path | None,
) -> dict[str, Any]:
    if RECIPE_MEMBER_PATH in archive.namelist():
        return _json(archive.read(RECIPE_MEMBER_PATH), RECIPE_MEMBER_PATH)
    digests = {
        parameters.get("recipe_digest")
        for item in provenance.get("activities", [])
        if isinstance(item, dict)
        and isinstance((parameters := item.get("parameters")), dict)
        and isinstance(parameters.get("recipe_digest"), str)
    }
    if len(digests) != 1:
        raise ValueError("parent MDAF does not identify exactly one source recipe")
    return load_known_recipe(next(iter(digests)), recipe_root)


def _sources(manifest: Mapping[str, Any]) -> list[MdafSource]:
    result = []
    for source in manifest.get("sources", []):
        if not isinstance(source, dict):
            raise ValueError("parent MDAF has a malformed source declaration")
        result.append(
            MdafSource(
                id=str(source["id"]),
                media_type=str(source["media_type"]),
                digest=str(source["digest"]),
                alternate_digests=tuple(source.get("alternate_digests", [])),
                name=source.get("name"),
                embedded_path=source.get("embedded_path"),
            )
        )
    return result


def reprocess_mdaf(
    parent_path: str | Path,
    target_recipe_path: str | Path | Mapping[str, Any],
    output_path: str | Path,
    *,
    recipe_root: str | Path | None = None,
    source_name: str | None = None,
) -> ReprocessResult:
    """Re-run post-processing from an MDAF without source or provider access."""
    parent = Path(parent_path)
    destination = Path(output_path)
    if destination.exists():
        raise FileExistsError(f"refusing to overwrite existing artifact: {destination}")
    validated = validate_mdaf(parent)
    target_recipe = (
        dict(target_recipe_path)
        if isinstance(target_recipe_path, Mapping)
        else json.loads(Path(target_recipe_path).read_text(encoding="utf-8"))
    )
    if not isinstance(target_recipe, dict):
        raise ValueError("target recipe must contain a JSON object")

    with zipfile.ZipFile(parent) as archive:
        info_bytes = archive.read("info.json")
        provenance_bytes = archive.read("provenance.json")
        manifest = _json(info_bytes, "info.json")
        parent_provenance = _json(provenance_bytes, "provenance.json")
        source_recipe = _source_recipe(
            archive, parent_provenance, recipe_root=recipe_root
        )
        source_recipe_digest, lifecycle = assert_reprocessable(
            source_recipe, target_recipe
        )
        missing = [
            path for path in lifecycle.native_members if path not in archive.namelist()
        ]
        if missing:
            raise ValueError(f"parent MDAF lacks required native evidence: {missing}")
        if lifecycle.family != "mistral-ocr-wiki":
            raise ValueError(f"unsupported reprocessor family: {lifecycle.family}")
        native_path = lifecycle.native_members[0]
        native_bytes = archive.read(native_path)
        native = _json(native_bytes, native_path)
        sources = _sources(manifest)
        if len(sources) != 1:
            raise ValueError("Mistral OCR reprocessing requires exactly one source")
        if source_name is not None:
            name = source_display_name(Path(sources[0].name or "source"), source_name)
            sources = [replace(sources[0], name=name)]
        rendered = render_mistral_response(
            native,
            normalization_profile=lifecycle.postprocessing_profile,
            source_id=sources[0].id,
        )

        parent_members = {
            item["path"]: item
            for item in manifest.get("members", [])
            if isinstance(item, dict) and isinstance(item.get("path"), str)
        }
        carry_paths = {
            path
            for path, member in parent_members.items()
            if member.get("role") in {"rendition", "environment", "source"}
            or (member.get("role") == "extension" and lifecycle.postprocessing_profile in {"wiki-v3", "wiki-v4"})
        }
        carry_paths.update(lifecycle.native_members)
        carry_paths.discard(RECIPE_MEMBER_PATH)
        replaced_paths = {PARENT_INFO_PATH, PARENT_PROVENANCE_PATH, PREVIOUS_RECIPE_PATH,
                          "extensions/dev.tionis.blobforge/hierarchy.json"}
        if lifecycle.postprocessing_profile not in {"wiki-v3", "wiki-v4"}:
            carry_paths.difference_update(replaced_paths)
        carried_members: list[MdafMemberInput] = []
        for path in sorted(carry_paths):
            member = parent_members.get(path)
            if member is None:
                raise ValueError(f"parent manifest does not declare retained member {path}")
            carried_members.append(
                MdafMemberInput(
                    path=(
                        f"extensions/dev.tionis.blobforge/ancestors/{validated.identity.split(':', 1)[1]}/{Path(path).name}"
                        if path in replaced_paths else path
                    ),
                    data=archive.read(path),
                    role=str(member["role"]),
                    created_by="activity:reuse-extraction",
                    media_type=member.get("media_type"),
                    schema=member.get("schema"),
                    namespace=member.get("namespace"),
                )
            )

    evidence_members = [
        *carried_members,
        MdafMemberInput(
            PARENT_INFO_PATH,
            info_bytes,
            "extension",
            "activity:reuse-extraction",
            "application/json",
            namespace="dev.tionis.blobforge",
        ),
        MdafMemberInput(
            PARENT_PROVENANCE_PATH,
            provenance_bytes,
            "extension",
            "activity:reuse-extraction",
            "application/json",
            namespace="dev.tionis.blobforge",
        ),
        MdafMemberInput(
            PREVIOUS_RECIPE_PATH,
            canonical_json_bytes(source_recipe),
            "extension",
            "activity:reuse-extraction",
            "application/json",
            namespace="dev.tionis.blobforge",
        ),
        MdafMemberInput(
            RECIPE_MEMBER_PATH,
            canonical_json_bytes(target_recipe),
            "extension",
            "activity:postprocess",
            "application/json",
            namespace="dev.tionis.blobforge",
        ),
    ]
    referenced = referenced_asset_names(rendered.text)
    if rendered.hierarchy_report is not None:
        evidence_members.append(MdafMemberInput(
            "extensions/dev.tionis.blobforge/hierarchy.json",
            canonical_json_bytes(rendered.hierarchy_report), "extension",
            "activity:postprocess", "application/json", namespace="dev.tionis.blobforge",
        ))
    for name, (data, media_type) in sorted(rendered.assets.items()):
        if name in referenced:
            evidence_members.append(
                MdafMemberInput(
                    f"assets/{name}",
                    data,
                    "asset",
                    "activity:postprocess",
                    media_type,
                )
            )

    reused_outputs = [
        member.path
        for member in evidence_members
        if member.created_by == "activity:reuse-extraction"
    ]
    postprocess_outputs = [
        "text.md",
        "provenance.json",
        "source-map.json",
        "outline.json",
        *[
            member.path
            for member in evidence_members
            if member.created_by == "activity:postprocess"
        ],
    ]
    parent_models = []
    for parent_activity in parent_provenance.get("activities", []):
        if isinstance(parent_activity, dict) and parent_activity.get("models"):
            parent_models = list(parent_activity["models"])
            break
    source_inputs = [f"source:{source.id}" for source in sources]
    tool_version = version("blobforge")
    activities = [
        activity(
            activity_id="activity:reuse-extraction",
            kind="retained-extraction-evidence",
            tools=[{"name": "blobforge-mdaf-reprocessor", "version": tool_version}],
            models=parent_models,
            inputs=source_inputs,
            outputs=sorted(set(reused_outputs)),
            parameters={
                "network_access": False,
                "parent_identity": validated.identity,
                "source_recipe_digest": source_recipe_digest,
                **({"source_name_override": sources[0].name} if source_name is not None else {}),
                "extraction_recipe_digest": lifecycle.extraction_recipe_digest,
            },
        ),
        activity(
            activity_id="activity:postprocess",
            kind="document-normalization",
            tools=[
                {
                    "name": "blobforge-wiki-normalizer",
                    "version": (
                        f"{lifecycle.postprocessing_version.major}."
                        f"{lifecycle.postprocessing_version.minor}."
                        f"{lifecycle.postprocessing_version.patch}"
                    ),
                }
            ],
            inputs=list(lifecycle.native_members),
            outputs=sorted(set(postprocess_outputs)),
            parameters={
                "network_access": False,
                "profile": lifecycle.postprocessing_profile,
                "recipe_digest": lifecycle.digest,
                "recipe_version": (
                    f"{lifecycle.version.major}."
                    f"{lifecycle.version.minor}."
                    f"{lifecycle.version.patch}"
                ),
                "source_recipe_digest": source_recipe_digest,
            },
            depends_on=["activity:reuse-extraction"],
        ),
    ]
    markdown = target_recipe.get("markdown")
    features = markdown.get("features", []) if isinstance(markdown, dict) else []
    redactions = parent_provenance.get("redactions", [])
    result = build_mdaf(
        destination,
        text=rendered.text,
        title=Path(sources[0].name).stem if source_name is not None else manifest.get("title"),
        sources=sources,
        activities=activities,
        producer={"name": "blobforge", "version": tool_version},
        extra_members=evidence_members,
        source_map=rendered.source_map,
        outline=rendered.outline if rendered.outline is not None else markdown_outline(rendered.text),
        redactions=redactions if isinstance(redactions, list) else [],
        markdown_variant=manifest.get("markdown", {}).get("variant", "CommonMark"),
        markdown_features=features,
        derived_from=[validated.identity],
        primary_created_by="activity:postprocess",
    )
    validate_mdaf(result.path)
    return ReprocessResult(
        path=result.path,
        identity=result.identity,
        parent_identity=validated.identity,
        source_recipe_digest=source_recipe_digest,
        target_recipe_digest=recipe_digest(target_recipe),
        normalization_stats=rendered.normalization_stats,
    )
