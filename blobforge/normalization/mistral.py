"""Pure, replayable rendering of retained Mistral OCR native evidence."""

from __future__ import annotations

import base64
import mimetypes
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .wiki import normalize_mistral_pages
from .hierarchy import book_outline, page_labels, page_references

LINK_RE = re.compile(r"(!?\[[^\]]*\]\()([^\)\s]+)(\))")
SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


@dataclass(frozen=True)
class MistralRendered:
    text: str
    source_map: Mapping[str, Any]
    assets: Mapping[str, tuple[bytes, str]]
    normalization_stats: Mapping[str, int] | None
    outline: Mapping[str, Any] | None = None
    hierarchy_report: Mapping[str, Any] | None = None


def decode_image(value: str) -> tuple[bytes, str | None]:
    media_type = None
    encoded = value
    if value.startswith("data:"):
        header, encoded = value.split(",", 1)
        media_type = header[5:].split(";", 1)[0] or None
    return base64.b64decode(encoded, validate=True), media_type


def image_media_type(data: bytes, declared: str | None) -> str:
    signatures = (
        (b"\x89PNG\r\n\x1a\n", "image/png"),
        (b"\xff\xd8\xff", "image/jpeg"),
        (b"GIF87a", "image/gif"),
        (b"GIF89a", "image/gif"),
    )
    detected = next(
        (media for prefix, media in signatures if data.startswith(prefix)), None
    )
    if detected is None and data.startswith(b"RIFF") and data[8:12] == b"WEBP":
        detected = "image/webp"
    if detected is None:
        raise ValueError("Mistral image payload is not a supported raster image")
    if declared and declared != detected:
        raise ValueError(
            f"Mistral image media type mismatch: declared {declared}, "
            f"detected {detected}"
        )
    return detected


def asset_name(
    page_number: int, image_index: int, original_id: str, media_type: str | None
) -> str:
    original = SAFE_NAME_RE.sub("-", Path(original_id).name).strip(".-") or "image"
    suffix = mimetypes.guess_extension(media_type) if media_type else None
    suffix = suffix or Path(original).suffix or ".bin"
    stem = Path(original).stem[:80] or "image"
    return f"page-{page_number:04d}-{image_index:03d}-{stem}{suffix.lower()}"


def page_confidence(page: Mapping[str, Any]) -> float | None:
    scores = page.get("confidence_scores")
    if not isinstance(scores, dict):
        return None
    confidence = scores.get("average_page_confidence_score")
    if isinstance(confidence, (int, float)) and not isinstance(confidence, bool):
        value = float(confidence)
        if 0 <= value <= 1:
            return value
    return None


def validate_response(
    native: Mapping[str, Any], source_pages: int | None = None
) -> list[dict[str, Any]]:
    if not isinstance(native.get("model"), str) or not native["model"]:
        raise ValueError("Mistral response is missing the returned model identity")
    pages = native.get("pages")
    if not isinstance(pages, list):
        raise ValueError("Mistral response pages must be an array")
    expected_pages = len(pages) if source_pages is None else source_pages
    indices = []
    for fallback_page, page in enumerate(pages):
        if not isinstance(page, dict) or not isinstance(page.get("markdown"), str):
            raise ValueError(f"Mistral page {fallback_page} is malformed")
        index = page.get("index", fallback_page)
        if isinstance(index, bool) or not isinstance(index, int):
            raise ValueError(f"Mistral page {fallback_page} has an invalid index")
        indices.append(index)
    if indices != list(range(expected_pages)):
        raise ValueError(
            "Mistral response page indices do not exactly cover the source: "
            f"expected 0..{expected_pages - 1}, got {indices[:12]}"
        )
    usage = native.get("usage_info")
    processed = usage.get("pages_processed") if isinstance(usage, dict) else None
    if (
        isinstance(processed, bool)
        or not isinstance(processed, int)
        or processed != expected_pages
    ):
        raise ValueError("Mistral usage_info.pages_processed does not match the source")
    return pages


def render_mistral_response(
    native: Mapping[str, Any],
    *,
    normalization_profile: str | None,
    source_pages: int | None = None,
    source_id: str = "document",
) -> MistralRendered:
    """Create Markdown, page mappings, and assets without provider access."""
    if normalization_profile not in {None, "wiki-v1", "wiki-v2", "wiki-v3", "wiki-v4", "wiki-v5"}:
        raise ValueError("unsupported normalization_profile")
    pages = validate_response(native, source_pages)
    normalization_stats = None
    if normalization_profile in {"wiki-v1", "wiki-v2", "wiki-v3", "wiki-v4", "wiki-v5"}:
        normalized_pages, normalization_stats = normalize_mistral_pages(
            pages,
            normalize_lists=normalization_profile in {"wiki-v2", "wiki-v3", "wiki-v4", "wiki-v5"},
        )
    else:
        normalized_pages = [page["markdown"] for page in pages]

    markdown = ""
    mappings: list[dict[str, Any]] = []
    assets: dict[str, tuple[bytes, str]] = {}
    for page_number, page in enumerate(pages):
        page_markdown = normalized_pages[page_number]
        replacements: dict[str, str] = {}
        for image_index, image in enumerate(page.get("images", []) or []):
            if not isinstance(image, dict):
                raise ValueError(
                    f"Mistral page {page_number} contains a malformed image"
                )
            original_id = str(
                image.get("id") or f"page-{page_number}-image-{image_index}"
            )
            image_data = image.get("image_base64")
            if not image_data:
                continue
            decoded, declared_media_type = decode_image(str(image_data))
            media_type = image_media_type(decoded, declared_media_type)
            name = asset_name(page_number, image_index, original_id, media_type)
            assets[name] = (decoded, media_type)
            link_name = Path(original_id).name
            if link_name in replacements:
                raise ValueError(
                    f"Mistral page {page_number} repeats image id basename "
                    f"{link_name!r}"
                )
            replacements[link_name] = name
        page_markdown = LINK_RE.sub(
            lambda match: (
                f"{match.group(1)}assets/"
                f"{replacements[Path(match.group(2)).name]}{match.group(3)}"
                if Path(match.group(2)).name in replacements
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
            mapping: dict[str, Any] = {
                "document": {"start": start, "end": end},
                "source": {
                    "source_id": source_id,
                    "selectors": [
                        {
                            "type": "interval",
                            "unit": "page",
                            "start": page_number,
                            "end": page_number + 1,
                        }
                    ],
                },
                "method": "dev.tionis.blobforge/mistral-ocr-page",
            }
            confidence = page_confidence(page)
            if confidence is not None:
                mapping["confidence"] = confidence
            mappings.append(mapping)
    source_map = {"mappings": mappings, "references": []}
    outline = report = None
    if normalization_profile in {"wiki-v3", "wiki-v4", "wiki-v5"}:
        labels = page_labels(pages)
        for mapping in mappings:
            selector = mapping["source"]["selectors"][0]
            if selector["start"] in labels:
                selector["label_start"] = labels[selector["start"]]
        if normalization_profile in {"wiki-v4", "wiki-v5"}:
            from .book_structure import recover_book_structure
            outline, report = recover_book_structure(markdown, pages, source_map, reconcile_conflicts=normalization_profile == "wiki-v5")
        else:
            outline, report = book_outline(markdown, pages, source_map)
        source_map["references"] = page_references(markdown, labels, source_id)
        report["observed_page_labels"] = len(labels)
        report["source_references"] = len(source_map["references"])
    return MistralRendered(
        text=markdown,
        source_map=source_map,
        assets=assets,
        normalization_stats=normalization_stats,
        outline=outline,
        hierarchy_report=report,
    )
