"""Docling 2 adapter for BlobForge's converter-bundle v1 filesystem ABI."""

from __future__ import annotations

import json
import re
import sys
from importlib.metadata import version
from pathlib import Path

from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling_core.types.doc import ImageRefMode

CONTRACT = "dev.tionis.blobforge.converter-bundle/v1"
PAGE_BREAK = "<!-- blobforge-docling-page-break -->"
LINK_RE = re.compile(r"(!?\[[^\]]*\]\()([^\)\s]+)(\))")


def _strip_page_breaks(markdown: str) -> tuple[str, list[dict]]:
    chunks = markdown.split(PAGE_BREAK)
    output = ""
    mappings = []
    for page, chunk in enumerate(chunks):
        start = len(output.encode("utf-8"))
        output += chunk
        end = len(output.encode("utf-8"))
        if end > start:
            mappings.append(
                {
                    "document": {"start": start, "end": end},
                    "source": {
                        "source_id": "document",
                        "selectors": [
                            {"type": "interval", "unit": "page", "start": page, "end": page + 1}
                        ],
                    },
                    "confidence": 1,
                    "method": "dev.tionis.blobforge/docling-page-break",
                }
            )
    return output, mappings


def main() -> int:
    request = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    if request.get("contract") != CONTRACT:
        raise ValueError("unsupported converter request contract")
    source = Path(request["source_path"])
    output = Path(request["output_dir"])
    output.mkdir(parents=True, exist_ok=True)
    data_dir = output / "data"
    assets_dir = data_dir / "assets"
    native_dir = data_dir / "native"
    data_dir.mkdir()
    assets_dir.mkdir()
    native_dir.mkdir()

    parameters = request.get("parameters", {})
    pipeline = PdfPipelineOptions()
    pipeline.do_ocr = bool(parameters.get("do_ocr", True))
    pipeline.do_table_structure = bool(parameters.get("do_table_structure", True))
    pipeline.generate_picture_images = bool(parameters.get("generate_picture_images", True))
    pipeline.images_scale = float(parameters.get("images_scale", 1.0))
    converter = DocumentConverter(
        allowed_formats=[InputFormat.PDF],
        format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline)},
    )
    result = converter.convert(source)
    raw_markdown_path = data_dir / "docling-with-page-markers.md"
    result.document.save_as_markdown(
        raw_markdown_path,
        image_mode=ImageRefMode.REFERENCED,
        artifacts_dir=assets_dir,
        page_break_placeholder=PAGE_BREAK,
    )
    raw_markdown = raw_markdown_path.read_text(encoding="utf-8")
    raw_markdown_path.unlink()
    asset_names = {
        path.name: path.relative_to(assets_dir).as_posix()
        for path in assets_dir.rglob("*")
        if path.is_file()
    }
    raw_markdown = LINK_RE.sub(
        lambda match: (
            f"{match.group(1)}assets/{asset_names[Path(match.group(2)).name]}{match.group(3)}"
            if Path(match.group(2)).name in asset_names
            else match.group(0)
        ),
        raw_markdown,
    )
    if "\x00" in raw_markdown:
        raise ValueError("Docling emitted NUL bytes; refusing silently truncated Markdown")
    markdown, mappings = _strip_page_breaks(raw_markdown)
    (data_dir / "text.md").write_text(markdown, encoding="utf-8")
    native = result.document.export_to_dict()
    (native_dir / "docling.json").write_text(
        json.dumps(native, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (data_dir / "source-map.json").write_text(
        json.dumps({"mappings": mappings, "references": []}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    members = [
        {
            "path": "renditions/org.docling/docling-document.json",
            "file": "data/native/docling.json",
            "role": "rendition",
            "media_type": "application/json",
            "namespace": "org.docling",
        }
    ]
    for path in sorted(assets_dir.rglob("*")):
        if path.is_file():
            relative = path.relative_to(assets_dir).as_posix()
            members.append(
                {
                    "path": f"assets/{relative}",
                    "file": path.relative_to(output).as_posix(),
                    "role": "asset",
                    "media_type": "image/png" if path.suffix.lower() == ".png" else "application/octet-stream",
                }
            )
    bundle = {
        "contract": CONTRACT,
        "text_path": "data/text.md",
        "source_map": "data/source-map.json",
        "members": members,
        "tool": {"name": "docling", "version": version("docling")},
        "models": [
            {
                "provider": "docling-project",
                "identifier": "default-standard-pipeline-models",
                "resolution": "mutable-alias",
            }
        ],
        "parameters": {
            "do_ocr": pipeline.do_ocr,
            "do_table_structure": pipeline.do_table_structure,
            "generate_picture_images": pipeline.generate_picture_images,
            "images_scale": str(pipeline.images_scale),
            "page_break_placeholder": PAGE_BREAK,
        },
        "diagnostics": [
            {
                "severity": "warning",
                "code": "mutable-model-alias",
                "message": "Docling's default model bundle must be frozen by downloaded checksums before production.",
            }
        ],
    }
    (output / "bundle.json").write_text(
        json.dumps(bundle, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
