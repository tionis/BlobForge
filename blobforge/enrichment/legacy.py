"""Create an enriched MDAF derived from an existing legacy MDAF and PDF."""

from __future__ import annotations

import json
import zipfile
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from typing import Any, Mapping

from ..mdaf import MdafMemberInput, MdafSource, blake3_file, build_mdaf, validate_mdaf
from ..mdaf.builder import activity, markdown_outline
from ..mdaf.digest import blake3_bytes, canonical_json_bytes, json_document_bytes
from .align import AlignmentResult, align_markdown_to_pdf
from .pdf import extract_pdf_evidence, poppler_version

CONTROL_MEMBERS = {"info.json", "text.md", "source-map.json", "outline.json", "provenance.json"}
BASE_NAMESPACE = "dev.tionis.blobforge.base"
ENRICHMENT_NAMESPACE = "dev.tionis.blobforge.pdf-enrichment"


@dataclass(frozen=True)
class LegacyEnrichmentResult:
    path: Path
    identity: str
    recipe_digest: str
    alignment: AlignmentResult


def enrichment_recipe(extractor_version: str | None = None) -> dict[str, Any]:
    return {
        "schema": "dev.tionis.blobforge.recipe/v2",
        "pipeline": "legacy-mdaf-pdf-enrichment",
        "generation": 1,
        "base_artifact": "exact-mdaf-identity",
        "pdf_evidence": {
            "extractor": "poppler-pdftotext-bbox-layout",
            "version": extractor_version or poppler_version(),
            "ocr": False,
            "coordinates": {"unit": "point", "origin": "top-left"},
        },
        "markdown": {
            "content": "unchanged-from-base-artifact",
            "segmentation": "dev.tionis.blobforge/markdown-blocks-v1",
            "offset_unit": "utf-8-byte",
        },
        "alignment": {
            "algorithm": "dev.tionis.blobforge/poppler-monotonic-block-alignment-v1",
            "minimum_score": "0.72",
            "ambiguity_margin": "0.08",
            "maximum_pdf_blocks_per_mapping": 3,
            "maximum_unseeded_lookahead_blocks": 120,
            "candidate_index": "eight-rarest-long-tokens-v1",
            "maximum_candidate_starts": 200,
            "sequence_refinement": {
                "maximum_candidates": 12,
                "minimum_token_score": "0.35",
                "maximum_characters": 5000,
            },
        },
        "publication": {
            "unsupported_precision": "omit",
            "geometry": "clip-to-page-bounds-omit-empty-region",
            "retain_base_mappings": True,
            "outline": "complete-markdown-heading-forest",
            "native_evidence": "sanitized-json",
        },
        "artifact": {"format": "mdaf", "version": 1},
    }


def enrichment_recipe_digest(recipe: Mapping[str, Any]) -> str:
    return blake3_bytes(canonical_json_bytes(recipe))


def _source(value: Mapping[str, Any]) -> MdafSource:
    return MdafSource(
        str(value["id"]),
        str(value["media_type"]),
        str(value["digest"]),
        tuple(value.get("alternate_digests", [])),
        value.get("name"),
        value.get("embedded_path"),
    )


def _aligned_outline(markdown: str, mappings: list[Mapping[str, Any]]) -> dict[str, Any]:
    outline = markdown_outline(markdown)
    for node in outline["nodes"]:
        heading = node["heading"]
        candidates = [
            mapping
            for mapping in mappings
            if mapping["document"]["start"] <= heading["start"]
            and mapping["document"]["end"] >= heading["end"]
        ]
        if candidates:
            selected = min(
                candidates,
                key=lambda item: item["document"]["end"] - item["document"]["start"],
            )
            node["source"] = selected["source"]
    return outline


def enrich_legacy_mdaf(
    source_pdf: str | Path,
    base_mdaf: str | Path,
    output_path: str | Path,
) -> LegacyEnrichmentResult:
    """Build one append-only enriched artifact; never mutate either input."""
    source_path = Path(source_pdf)
    base_path = Path(base_mdaf)
    base_validation = validate_mdaf(base_path)
    with zipfile.ZipFile(base_path) as archive:
        base_info = json.loads(archive.read("info.json"))
        markdown = archive.read("text.md").decode("utf-8")
        base_source_map = (
            json.loads(archive.read("source-map.json"))
            if "source-map.json" in archive.namelist()
            else {"mappings": [], "references": []}
        )
        copied: list[MdafMemberInput] = []
        manifest_members = {item["path"]: item for item in base_info["members"]}
        for member_path, member in manifest_members.items():
            if member_path in CONTROL_MEMBERS:
                continue
            copied.append(
                MdafMemberInput(
                    member_path,
                    archive.read(member_path),
                    member["role"],
                    "activity:base-import",
                    member["media_type"],
                    member.get("schema"),
                    member.get("namespace"),
                )
            )
        for member_path in sorted(CONTROL_MEMBERS - {"text.md"}):
            if member_path not in archive.namelist():
                continue
            copied.append(
                MdafMemberInput(
                    f"renditions/{BASE_NAMESPACE}/{member_path}",
                    archive.read(member_path),
                    "rendition",
                    "activity:base-import",
                    "application/json",
                    namespace=BASE_NAMESPACE,
                )
            )

    source_digest = blake3_file(source_path)
    sources = [_source(item) for item in base_info["sources"]]
    if not any(item.digest == source_digest for item in sources):
        raise ValueError("source PDF BLAKE3 does not match the base artifact")

    recipe = enrichment_recipe()
    recipe_digest = enrichment_recipe_digest(recipe)
    evidence = extract_pdf_evidence(source_path)
    seed_mappings = list(base_source_map.get("mappings", []))
    alignment = align_markdown_to_pdf(markdown, evidence, seed_mappings=seed_mappings)
    mappings = [*seed_mappings, *alignment.mappings]
    references = list(base_source_map.get("references", []))
    evidence_path = f"renditions/org.freedesktop.poppler/{recipe_digest.removeprefix('blake3:')}.json"
    report_path = f"extensions/{ENRICHMENT_NAMESPACE}/report.json"
    copied.extend(
        [
            MdafMemberInput(
                evidence_path,
                json_document_bytes(evidence.as_json()),
                "rendition",
                "activity:enrich",
                "application/json",
                namespace="org.freedesktop.poppler",
            ),
            MdafMemberInput(
                report_path,
                json_document_bytes(alignment.report()),
                "extension",
                "activity:enrich",
                "application/json",
                namespace=ENRICHMENT_NAMESPACE,
            ),
        ]
    )
    base_import = activity(
        activity_id="activity:base-import",
        kind="artifact-derivation",
        tools=[{"name": "blobforge", "version": version("blobforge")}],
        inputs=["source:document"],
        outputs=[item.path for item in copied if item.created_by == "activity:base-import"],
        parameters={
            "base_artifact_identity": base_validation.identity,
            "primary_markdown_reused_unchanged": True,
        },
    )
    enrichment = activity(
        activity_id="activity:enrich",
        kind="document-enrichment",
        tools=[
            {"name": "blobforge", "version": version("blobforge")},
            {"name": "pdftotext", "version": evidence.extractor_version},
        ],
        inputs=["source:document", f"renditions/{BASE_NAMESPACE}/info.json"],
        outputs=["text.md", "source-map.json", "outline.json", "provenance.json", evidence_path, report_path],
        parameters={**recipe, "recipe_digest": recipe_digest},
        depends_on=["activity:base-import"],
    )
    result = build_mdaf(
        output_path,
        text=markdown,
        title=base_info.get("title"),
        sources=sources,
        activities=[base_import, enrichment],
        producer={"name": "blobforge", "version": version("blobforge")},
        extra_members=copied,
        source_map={"mappings": mappings, "references": references},
        outline=_aligned_outline(markdown, mappings),
        derived_from=[base_validation.identity],
        primary_created_by="activity:enrich",
    )
    validated = validate_mdaf(result.path)
    if validated.identity != result.identity:
        raise RuntimeError("enriched MDAF changed during post-build validation")
    return LegacyEnrichmentResult(result.path, result.identity, recipe_digest, alignment)
