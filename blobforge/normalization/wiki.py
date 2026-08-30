"""Evidence-driven cleanup for wiki-oriented converter recipes."""

from __future__ import annotations

import math
import re
import struct
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

from .lists import recover_typed_text_list_runs, strip_markdown_list_decorations
from .tables import markdown_table_to_html

IMAGE_BLOCK_RE = re.compile(r"^!\[([^]]*)\]\(([^)\s]+)\)$")
IMAGE_LINK_RE = re.compile(r"!\[[^]]*\]\(([^)\s]+)\)")
TOKEN_RE = re.compile(r"[^\W_]+", re.UNICODE)
GENERIC_IMAGE_TOKENS = {
    "image",
    "illustration",
    "graphic",
    "small",
    "dark",
    "black",
    "white",
    "bottom",
    "right",
    "corner",
    "featuring",
    "with",
    "from",
    "that",
    "this",
}


def referenced_asset_names(markdown: str) -> set[str]:
    return {Path(match.group(1)).name for match in IMAGE_LINK_RE.finditer(markdown)}


def raster_dimensions(data: bytes) -> tuple[int, int] | None:
    """Return dimensions for supported raster bytes without decoding pixels."""
    if data.startswith(b"\x89PNG\r\n\x1a\n") and len(data) >= 24:
        width, height = struct.unpack(">II", data[16:24])
        return (width, height) if width and height else None
    if data.startswith((b"GIF87a", b"GIF89a")) and len(data) >= 10:
        width, height = struct.unpack("<HH", data[6:10])
        return (width, height) if width and height else None
    if data.startswith(b"\xff\xd8"):
        offset = 2
        start_of_frame = {
            0xC0,
            0xC1,
            0xC2,
            0xC3,
            0xC5,
            0xC6,
            0xC7,
            0xC9,
            0xCA,
            0xCB,
            0xCD,
            0xCE,
            0xCF,
        }
        while offset + 4 <= len(data):
            if data[offset] != 0xFF:
                offset += 1
                continue
            while offset < len(data) and data[offset] == 0xFF:
                offset += 1
            if offset >= len(data):
                return None
            marker = data[offset]
            offset += 1
            if marker in {0x01, *range(0xD0, 0xDA)}:
                continue
            if offset + 2 > len(data):
                return None
            length = int.from_bytes(data[offset : offset + 2], "big")
            if length < 2 or offset + length > len(data):
                return None
            if marker in start_of_frame and length >= 7:
                height = int.from_bytes(data[offset + 3 : offset + 5], "big")
                width = int.from_bytes(data[offset + 5 : offset + 7], "big")
                return (width, height) if width and height else None
            offset += length
    if data.startswith(b"RIFF") and data[8:12] == b"WEBP" and len(data) >= 30:
        kind = data[12:16]
        if kind == b"VP8X":
            width = 1 + int.from_bytes(data[24:27], "little")
            height = 1 + int.from_bytes(data[27:30], "little")
            return width, height
    return None


def _number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"Mistral block {field} must be numeric")
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise ValueError(f"Mistral block {field} must be finite and non-negative")
    return number


def _is_footer_image(block: Mapping[str, Any], page_height: float) -> bool:
    top = _number(block.get("top_left_y"), "top_left_y")
    bottom = _number(block.get("bottom_right_y"), "bottom_right_y")
    if bottom < top or bottom > page_height * 1.02:
        raise ValueError("Mistral image geometry is outside the page")
    return top >= page_height * 0.90 and (bottom - top) <= page_height * 0.15


def normalize_mistral_pages(
    pages: Sequence[Mapping[str, Any]],
    *,
    normalize_lists: bool = False,
) -> tuple[list[str], dict[str, int]]:
    """Rebuild page Markdown from typed blocks, omitting proven page furniture."""
    normalized: list[str] = []
    stats = {
        "headers_removed": 0,
        "footers_removed": 0,
        "footer_images_removed": 0,
        "tables_converted": 0,
        "list_decorations_removed": 0,
        "text_list_items_recovered": 0,
    }
    for page_number, page in enumerate(pages):
        dimensions = page.get("dimensions")
        blocks = page.get("blocks")
        if not isinstance(dimensions, dict) or not isinstance(blocks, list):
            raise ValueError(
                f"Mistral wiki normalization requires dimensions and blocks on page {page_number}"
            )
        page_height = _number(dimensions.get("height"), "page height")
        if page_height <= 0:
            raise ValueError("Mistral page height must be positive")
        list_replacements: dict[int, str] = {}
        if normalize_lists:
            list_replacements, recovered = recover_typed_text_list_runs(blocks)
            stats["text_list_items_recovered"] += recovered
        output_blocks: list[str] = []
        for block_index, block in enumerate(blocks):
            if not isinstance(block, dict):
                raise ValueError(f"Mistral page {page_number} has a malformed block")
            block_type = block.get("type")
            content = block.get("content")
            if not isinstance(block_type, str) or not isinstance(content, str):
                raise ValueError(f"Mistral page {page_number} has an incomplete block")
            if block_type == "header":
                stats["headers_removed"] += 1
                continue
            if block_type == "footer":
                stats["footers_removed"] += 1
                continue
            if block_type == "image" and _is_footer_image(block, page_height):
                stats["footer_images_removed"] += 1
                continue
            value = list_replacements.get(block_index, content).strip()
            if not value:
                continue
            if block_type == "table":
                try:
                    value = markdown_table_to_html(value)
                except ValueError:
                    # Mistral may type captions, empty grids, or a header plus
                    # separator as tables. They are not semantic grids and the
                    # recipe's ambiguous-content contract requires retaining
                    # their original Markdown instead of failing packaging.
                    pass
                else:
                    stats["tables_converted"] += 1
            elif normalize_lists and block_type == "list":
                value, removed = strip_markdown_list_decorations(value)
                stats["list_decorations_removed"] += removed
            output_blocks.append(value)
        normalized.append("\n\n".join(output_blocks))
    return normalized, stats


def _paragraph_blocks(markdown: str) -> list[str]:
    return [block.strip() for block in re.split(r"\n[ \t]*\n+", markdown) if block.strip()]


def _normalized_text(value: str) -> str:
    return " ".join(value.split())


def _tokens(value: str) -> set[str]:
    return {
        token.casefold()
        for token in TOKEN_RE.findall(value)
        if len(token) >= 4 and token.casefold() not in GENERIC_IMAGE_TOKENS
    }


def _semantic_table(block: str) -> str | None:
    lines = [line.strip() for line in block.splitlines() if line.strip()]
    if len(lines) < 2 or any(not line.startswith("|") or not line.endswith("|") for line in lines):
        return None
    try:
        return markdown_table_to_html("\n".join(lines))
    except ValueError:
        return None


def normalize_datalab_pages(
    pages: Sequence[str],
    asset_dimensions: Mapping[str, tuple[int, int] | None],
) -> tuple[list[str], dict[str, int]]:
    """Isolate exact alt duplicates and recurring final small-image furniture."""
    cleaned: list[list[str]] = []
    stats = {
        "descriptions_isolated": 0,
        "footer_images_removed": 0,
        "tables_converted": 0,
    }
    for page in pages:
        blocks = _paragraph_blocks(page)
        output: list[str] = []
        index = 0
        while index < len(blocks):
            block = blocks[index]
            match = IMAGE_BLOCK_RE.fullmatch(block)
            output.append(block)
            if match and index + 1 < len(blocks):
                if _normalized_text(blocks[index + 1]) == _normalized_text(match.group(1)):
                    stats["descriptions_isolated"] += 1
                    index += 1
            elif not match:
                semantic_table = _semantic_table(block)
                if semantic_table is not None:
                    output[-1] = semantic_table
                    stats["tables_converted"] += 1
            index += 1
        cleaned.append(output)

    candidates: list[tuple[int, str, str, tuple[int, int]]] = []
    for page_number, blocks in enumerate(cleaned):
        if not blocks:
            continue
        match = IMAGE_BLOCK_RE.fullmatch(blocks[-1])
        if not match:
            continue
        name = Path(match.group(2)).name
        dimensions = asset_dimensions.get(name)
        if dimensions is None:
            continue
        width, height = dimensions
        if width <= 256 and height <= 256 and width * height <= 50_000:
            candidates.append((page_number, name, match.group(1), dimensions))

    threshold = max(3, math.ceil(len(pages) / 2))
    if len(candidates) >= threshold:
        widths = [item[3][0] for item in candidates]
        heights = [item[3][1] for item in candidates]
        token_counts = Counter(token for item in candidates for token in _tokens(item[2]))
        recurring_tokens = {
            token for token, count in token_counts.items() if count >= threshold
        }
        dimensionally_consistent = (
            max(widths) <= min(widths) * 1.25
            and max(heights) <= min(heights) * 1.25
        )
        if dimensionally_consistent and len(recurring_tokens) >= 2:
            for page_number, _name, _alt, _dimensions in candidates:
                cleaned[page_number].pop()
                stats["footer_images_removed"] += 1

    return ["\n\n".join(blocks) for blocks in cleaned], stats
