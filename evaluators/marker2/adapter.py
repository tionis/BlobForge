"""Marker 2.0 adapter for CPU fast and deterministic no-OCR evaluation."""

from __future__ import annotations

import json
import re
import sys
from importlib.metadata import version
from pathlib import Path

from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict
from marker.output import text_from_rendered

CONTRACT = "dev.tionis.blobforge.converter-bundle/v1"
SEPARATOR = "<!-- blobforge-marker2-page -->"
PAGE_RE = re.compile(r"\{(\d+)\}" + re.escape(SEPARATOR))
LINK_RE = re.compile(r"(!?\[[^\]]*\]\()([^\)\s]+)(\))")


def _strip_pages(markdown: str) -> tuple[str, list[dict]]:
    matches = list(PAGE_RE.finditer(markdown))
    output, mappings, cursor, page, page_start = "", [], 0, None, 0
    for match in matches:
        output += markdown[cursor : match.start()]
        end = len(output.encode("utf-8"))
        if page is not None and end > page_start:
            mappings.append(_mapping(page, page_start, end))
        page, page_start, cursor = int(match.group(1)), end, match.end()
    output += markdown[cursor:]
    end = len(output.encode("utf-8"))
    if page is not None and end > page_start:
        mappings.append(_mapping(page, page_start, end))
    return output, mappings


def _mapping(page: int, start: int, end: int) -> dict:
    return {
        "document": {"start": start, "end": end},
        "source": {
            "source_id": "document",
            "selectors": [{"type": "interval", "unit": "page", "start": page, "end": page + 1}],
        },
        "confidence": 1,
        "method": "dev.tionis.blobforge/marker2-pagination",
    }


def main() -> int:
    request = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    if request.get("contract") != CONTRACT:
        raise ValueError("unsupported converter request contract")
    source = Path(request["source_path"])
    output = Path(request["output_dir"])
    data, assets, native = output / "data", output / "data/assets", output / "data/native"
    assets.mkdir(parents=True)
    native.mkdir()
    parameters = request.get("parameters", {})
    do_ocr = bool(parameters.get("do_ocr", True))
    config = {
        "mode": str(parameters.get("mode") or "fast"),
        "disable_ocr": not do_ocr,
        "paginate_output": True,
        "page_separator": SEPARATOR,
        "disable_image_extraction": not bool(parameters.get("generate_picture_images", True)),
        "pdftext_workers": 1,
    }
    rendered = PdfConverter(artifact_dict=create_model_dict(), config=config)(str(source))
    markdown, _extension, images = text_from_rendered(rendered)
    image_names = {Path(name).name for name in images}
    markdown = LINK_RE.sub(
        lambda match: (
            f"{match.group(1)}assets/{Path(match.group(2)).name}{match.group(3)}"
            if Path(match.group(2)).name in image_names
            else match.group(0)
        ),
        markdown,
    )
    markdown, mappings = _strip_pages(markdown)
    (data / "text.md").write_text(markdown, encoding="utf-8")
    metadata = getattr(rendered, "metadata", {})
    if hasattr(metadata, "model_dump"):
        metadata = metadata.model_dump(mode="json")
    (native / "marker.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8"
    )
    members = [
        {
            "path": "renditions/com.datalab.marker2/metadata.json",
            "file": "data/native/marker.json",
            "role": "rendition",
            "media_type": "application/json",
            "namespace": "com.datalab.marker2",
        }
    ]
    for name, image in sorted(images.items()):
        safe_name = Path(name).name
        path = assets / safe_name
        if hasattr(image, "mode") and image.mode != "RGB":
            image = image.convert("RGB")
        image.save(path)
        members.append(
            {
                "path": f"assets/{safe_name}",
                "file": f"data/assets/{safe_name}",
                "role": "asset",
                "media_type": "image/jpeg" if path.suffix.lower() in {".jpg", ".jpeg"} else "image/png",
            }
        )
    (data / "source-map.json").write_text(
        json.dumps({"mappings": mappings, "references": []}, indent=2) + "\n", encoding="utf-8"
    )
    models = [] if not do_ocr else [
        {
            "provider": "datalab",
            "identifier": "marker-2.0.0-fast-default-models",
            "resolution": "mutable-alias",
        }
    ]
    bundle = {
        "contract": CONTRACT,
        "text_path": "data/text.md",
        "source_map": "data/source-map.json",
        "members": members,
        "tool": {"name": "marker-pdf", "version": version("marker-pdf")},
        "models": models,
        "parameters": config,
        "diagnostics": ([] if not do_ocr else [{
            "severity": "warning",
            "code": "mutable-model-alias",
            "message": "Marker 2 fast-mode model checksums are not yet frozen.",
        }]),
    }
    (output / "bundle.json").write_text(
        json.dumps(bundle, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
