"""Mistral OCR 4.1 API adapter with mandatory page and spend ceilings."""

from __future__ import annotations

import base64
import json
import os
import re
import sys
from importlib.metadata import version
from pathlib import Path

from mistralai.client import Mistral
from pypdf import PdfReader

CONTRACT = "dev.tionis.blobforge.converter-bundle/v1"
PRICE_PER_PAGE_USD = 0.004
LINK_RE = re.compile(r"(!?\[[^\]]*\]\()([^\)\s]+)(\))")


def _model_dump(value):
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", exclude_none=True)
    if isinstance(value, dict):
        return value
    raise TypeError(f"cannot serialize {type(value).__name__}")


def _decode_image(value: str) -> bytes:
    encoded = value.split(",", 1)[1] if value.startswith("data:") else value
    return base64.b64decode(encoded, validate=True)


def main() -> int:
    request = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    if request.get("contract") != CONTRACT:
        raise ValueError("unsupported converter request contract")
    api_key = os.environ.get("MISTRAL_API_KEY")
    if not api_key:
        raise ValueError("MISTRAL_API_KEY is required")
    source = Path(request["source_path"])
    output = Path(request["output_dir"])
    data = output / "data"
    assets = data / "assets"
    native_dir = data / "native"
    assets.mkdir(parents=True)
    native_dir.mkdir()
    parameters = request.get("parameters", {})
    page_count = len(PdfReader(source).pages)
    max_pages = int(parameters.get("max_pages") or 0)
    max_cost = float(parameters.get("max_cost_usd") or 0)
    expected_cost = page_count * PRICE_PER_PAGE_USD
    if max_pages <= 0 or page_count > max_pages:
        raise ValueError(f"page ceiling rejected {page_count} pages (limit {max_pages})")
    if max_cost <= 0 or expected_cost > max_cost:
        raise ValueError(f"spend ceiling rejected estimated ${expected_cost:.4f} (limit ${max_cost:.4f})")
    model = str(parameters.get("model") or "mistral-ocr-4-1")

    uploaded_id = None
    with Mistral(api_key=api_key) as client:
        try:
            uploaded = client.files.upload(
                file={"file_name": source.name, "content": source.read_bytes()},
                purpose="ocr",
            )
            uploaded_id = uploaded.id
            signed = client.files.get_signed_url(file_id=uploaded.id, expiry=60)
            response = client.ocr.process(
                model=model,
                document={"type": "document_url", "document_url": signed.url},
                include_image_base64=True,
                include_blocks=True,
                confidence_scores_granularity="block",
            )
        finally:
            if uploaded_id is not None:
                client.files.delete(file_id=uploaded_id)

    native = _model_dump(response)
    (native_dir / "response.json").write_text(
        json.dumps(native, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    markdown = ""
    mappings = []
    image_names: set[str] = set()
    pages = native.get("pages", [])
    for fallback_page, page in enumerate(pages):
        page_number = int(page.get("index", fallback_page))
        page_markdown = str(page.get("markdown", ""))
        for image_index, image in enumerate(page.get("images", []) or []):
            original_id = str(image.get("id") or f"page-{page_number}-image-{image_index}.png")
            name = Path(original_id).name
            if not Path(name).suffix:
                name += ".png"
            image_data = image.get("image_base64")
            if image_data:
                (assets / name).write_bytes(_decode_image(image_data))
                image_names.add(name)
        page_markdown = LINK_RE.sub(
            lambda match: (
                f"{match.group(1)}assets/{Path(match.group(2)).name}{match.group(3)}"
                if Path(match.group(2)).name in image_names
                else match.group(0)
            ),
            page_markdown,
        )
        if markdown:
            markdown += "\n\n"
        start = len(markdown.encode("utf-8"))
        markdown += page_markdown
        end = len(markdown.encode("utf-8"))
        if end > start:
            mapping = {
                    "document": {"start": start, "end": end},
                    "source": {
                        "source_id": "document",
                        "selectors": [
                            {"type": "interval", "unit": "page", "start": page_number, "end": page_number + 1}
                        ],
                    },
                    "method": "dev.tionis.blobforge/mistral-ocr-page",
                }
            confidence = page.get("confidence")
            if isinstance(confidence, (int, float)) and 0 <= confidence <= 1:
                mapping["confidence"] = confidence
            mappings.append(mapping)
    (data / "text.md").write_text(markdown, encoding="utf-8")
    (data / "source-map.json").write_text(
        json.dumps({"mappings": mappings, "references": []}, indent=2) + "\n",
        encoding="utf-8",
    )
    members = [
        {
            "path": "renditions/ai.mistral/ocr-response.json",
            "file": "data/native/response.json",
            "role": "rendition",
            "media_type": "application/json",
            "namespace": "ai.mistral",
        }
    ]
    for path in sorted(assets.iterdir()):
        if path.is_file():
            members.append(
                {
                    "path": f"assets/{path.name}",
                    "file": f"data/assets/{path.name}",
                    "role": "asset",
                    "media_type": "image/png",
                }
            )
    returned_model = native.get("model")
    bundle = {
        "contract": CONTRACT,
        "text_path": "data/text.md",
        "source_map": "data/source-map.json",
        "members": members,
        "tool": {"name": "mistralai", "version": version("mistralai")},
        "models": [
            {
                "provider": "mistral-ai",
                "identifier": model,
                **({"returned_identifier": returned_model} if returned_model else {}),
                # An echoed API model name is not evidence of an immutable
                # checkpoint. Preserve it, but fail closed on provenance.
                "resolution": "mutable-alias",
            }
        ],
        "parameters": {
            "model": model,
            "include_blocks": True,
            "confidence_scores_granularity": "block",
            "include_image_base64": True,
            "page_count": page_count,
            "list_price_usd": f"{expected_cost:.6f}",
            "max_pages": max_pages,
            "max_cost_usd": f"{max_cost:.6f}",
        },
        "diagnostics": [
            {
                "level": "warning",
                "message": "Mistral does not expose an immutable OCR checkpoint digest in this response.",
            }
        ],
    }
    (output / "bundle.json").write_text(
        json.dumps(bundle, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
