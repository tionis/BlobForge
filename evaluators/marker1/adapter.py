"""Pinned Marker 1.10 compatibility adapter with explicit page boundaries."""

from __future__ import annotations

import json
import re
import sys
from importlib.metadata import version
from pathlib import Path

from blobforge.enrichment.align import (
    align_markdown_to_pdf,
    validate_alignment_publication,
)
from blobforge.enrichment.pdf import extract_pdf_evidence
from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict
from marker.output import text_from_rendered

CONTRACT = "dev.tionis.blobforge.converter-bundle/v1"
SEPARATOR = "<!-- blobforge-marker-page -->"
PAGE_RE = re.compile(r"\{(\d+)\}" + re.escape(SEPARATOR))
MARKDOWN_LINK_RE = re.compile(r"(!?\[[^\]]*\]\()([^\)\s]+)(\))")


def _extract_marker_meta(rendered) -> dict:
    metadata = getattr(rendered, "metadata", {})
    if hasattr(metadata, "model_dump"):
        return metadata.model_dump(mode="json")
    if isinstance(metadata, dict):
        return metadata
    return {"value": str(metadata)}


def _strip_pages(markdown: str) -> tuple[str, list[dict]]:
    matches = list(PAGE_RE.finditer(markdown))
    if not matches:
        return markdown, []
    output = ""
    mappings = []
    cursor = 0
    current_page = None
    page_start = 0
    for match in matches:
        chunk = markdown[cursor : match.start()]
        output += chunk
        end = len(output.encode("utf-8"))
        if current_page is not None and end > page_start:
            mappings.append(_mapping(current_page, page_start, end))
        current_page = int(match.group(1))
        page_start = end
        cursor = match.end()
    output += markdown[cursor:]
    end = len(output.encode("utf-8"))
    if current_page is not None and end > page_start:
        mappings.append(_mapping(current_page, page_start, end))
    return output, mappings


def _mapping(page: int, start: int, end: int) -> dict:
    return {
        "document": {"start": start, "end": end},
        "source": {
            "source_id": "document",
            "selectors": [{"type": "interval", "unit": "page", "start": page, "end": page + 1}],
        },
        "confidence": 1,
        "method": "dev.tionis.blobforge/marker1-pagination",
    }


def _rewrite_assets(markdown: str, names: set[str]) -> str:
    def replace(match: re.Match[str]) -> str:
        prefix, target, suffix = match.groups()
        name = target.rsplit("/", 1)[-1]
        return f"{prefix}assets/{name}{suffix}" if name in names else match.group(0)

    return MARKDOWN_LINK_RE.sub(replace, markdown)


def main() -> int:
    request = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    if request.get("contract") != CONTRACT:
        raise ValueError("unsupported converter request contract")
    source = Path(request["source_path"])
    output = Path(request["output_dir"])
    data = output / "data"
    assets = data / "assets"
    native = data / "native"
    assets.mkdir(parents=True)
    native.mkdir()
    parameters = request.get("parameters", {})
    config = {
        "paginate_output": True,
        "page_separator": SEPARATOR,
        "extract_images": bool(parameters.get("extract_images", True)),
    }
    rendered = PdfConverter(artifact_dict=create_model_dict(), config=config)(str(source))
    markdown, _extension, images = text_from_rendered(rendered)
    markdown = _rewrite_assets(markdown, {Path(name).name for name in images})
    markdown, mappings = _strip_pages(markdown)
    (data / "text.md").write_text(markdown, encoding="utf-8")
    (native / "raw.md").write_text(markdown, encoding="utf-8")
    marker_meta = _extract_marker_meta(rendered)
    (native / "marker.json").write_text(
        json.dumps(marker_meta, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    evidence = extract_pdf_evidence(source)
    alignment = align_markdown_to_pdf(markdown, evidence, seed_mappings=mappings)
    enriched_mappings = [*mappings, *alignment.mappings]
    publication_errors = validate_alignment_publication(
        {"mappings": enriched_mappings}, alignment.report()
    )
    if publication_errors:
        raise ValueError(
            "Marker enrichment publication invariants failed: "
            + "; ".join(publication_errors)
        )
    (native / "pdf-evidence.json").write_text(
        json.dumps(evidence.as_json(), ensure_ascii=False, sort_keys=True, indent=2)
        + "\n",
        encoding="utf-8",
    )
    (native / "enrichment-report.json").write_text(
        json.dumps(alignment.report(), ensure_ascii=False, sort_keys=True, indent=2)
        + "\n",
        encoding="utf-8",
    )
    members = [
        {
            "path": "renditions/com.datalab.marker/metadata.json",
            "file": "data/native/marker.json",
            "role": "rendition",
            "media_type": "application/json",
            "namespace": "com.datalab.marker",
        },
        {
            "path": "renditions/com.datalab.marker/raw.md",
            "file": "data/native/raw.md",
            "role": "rendition",
            "media_type": "text/markdown",
            "namespace": "com.datalab.marker",
        },
        {
            "path": "renditions/org.freedesktop.poppler/pdf-evidence.json",
            "file": "data/native/pdf-evidence.json",
            "role": "rendition",
            "media_type": "application/json",
            "namespace": "org.freedesktop.poppler",
        },
        {
            "path": "extensions/dev.tionis.blobforge.pdf-enrichment/report.json",
            "file": "data/native/enrichment-report.json",
            "role": "extension",
            "media_type": "application/json",
            "namespace": "dev.tionis.blobforge.pdf-enrichment",
        },
    ]
    for name, image in sorted(images.items()):
        safe_name = Path(name).name
        image_path = assets / safe_name
        if hasattr(image, "mode") and image.mode != "RGB":
            image = image.convert("RGB")
        image.save(image_path)
        members.append(
            {
                "path": f"assets/{safe_name}",
                "file": f"data/assets/{safe_name}",
                "role": "asset",
                "media_type": "image/jpeg" if image_path.suffix.lower() in {".jpg", ".jpeg"} else "image/png",
            }
        )
    (data / "source-map.json").write_text(
        json.dumps({"mappings": enriched_mappings, "references": []}, indent=2)
        + "\n",
        encoding="utf-8",
    )
    bundle = {
        "contract": CONTRACT,
        "text_path": "data/text.md",
        "source_map": "data/source-map.json",
        "members": members,
        "tool": {"name": "marker-pdf", "version": version("marker-pdf")},
        "additional_tools": [
            {"name": "pdftotext", "version": evidence.extractor_version},
            {"name": "blobforge", "version": version("blobforge")},
        ],
        "models": [
            {
                "provider": "datalab",
                "identifier": "marker-1.10.2-default-models",
                "resolution": "mutable-alias",
            }
        ],
        "parameters": {
            **config,
            "normalization_profile": "pdf-enrichment-v1",
            "recipe_digest": parameters.get("recipe_digest"),
        },
        "diagnostics": [
            {
                "severity": "warning",
                "code": "mutable-model-alias",
                "message": "Marker/Surya model checkpoint checksums are not yet frozen.",
            }
        ],
    }
    (output / "bundle.json").write_text(
        json.dumps(bundle, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
