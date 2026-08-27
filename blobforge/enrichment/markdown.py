"""Loss-aware Markdown segmentation with final UTF-8 byte spans."""

from __future__ import annotations

import html
import re
import unicodedata

from .contract import MarkdownBlock

ATX_RE = re.compile(r"^ {0,3}#{1,6}(?:[ \t]+|$)")
FENCE_RE = re.compile(r"^ {0,3}(`{3,}|~{3,})")
LIST_RE = re.compile(r"^\s*(?:[-+*]|\d+[.)])[ \t]+")
HTML_TAG_RE = re.compile(r"<[^>]+>")
IMAGE_RE = re.compile(r"!\[([^]]*)\]\([^)]*\)")
LINK_RE = re.compile(r"\[([^]]+)\]\([^)]*\)")


def normalize_for_alignment(value: str) -> str:
    """Normalize representation noise without changing published Markdown."""
    value = html.unescape(value)
    value = IMAGE_RE.sub(r"\1", value)
    value = LINK_RE.sub(r"\1", value)
    value = HTML_TAG_RE.sub(" ", value)
    value = re.sub(r"^\s{0,3}#{1,6}\s*", "", value)
    value = re.sub(r"(?m)^\s*(?:>|[-+*]|\d+[.)])\s+", "", value)
    value = value.replace("|", " ").replace("`", " ")
    value = re.sub(r"[*_~]", "", value)
    value = unicodedata.normalize("NFKC", value).replace("\u00ad", "")
    value = re.sub(r"(?<=\w)-\s+(?=\w)", "", value)
    value = " ".join(value.casefold().split())
    return value.strip(" .,:;!?")


def _kind(lines: list[str]) -> str:
    first = lines[0].lstrip()
    if ATX_RE.match(lines[0]):
        return "heading"
    if FENCE_RE.match(lines[0]):
        return "code"
    if LIST_RE.match(lines[0]):
        return "list"
    if first.startswith("|") and sum("|" in line for line in lines) >= 2:
        return "table"
    if first.startswith("!["):
        return "image"
    if first.startswith(">"):
        return "quote"
    return "paragraph"


def segment_markdown(markdown: str) -> tuple[MarkdownBlock, ...]:
    """Return semantic chunks bound to the exact serialized UTF-8 bytes."""
    lines = markdown.splitlines(keepends=True)
    character_starts: list[int] = []
    cursor = 0
    for line in lines:
        character_starts.append(cursor)
        cursor += len(line)

    ranges: list[tuple[int, int]] = []
    index = 0
    while index < len(lines):
        if not lines[index].strip():
            index += 1
            continue
        start = index
        fence = FENCE_RE.match(lines[index])
        if fence:
            marker = fence.group(1)[0]
            minimum = len(fence.group(1))
            index += 1
            while index < len(lines):
                if re.match(rf"^ {{0,3}}{re.escape(marker)}{{{minimum},}}\s*$", lines[index]):
                    index += 1
                    break
                index += 1
        elif ATX_RE.match(lines[index]):
            index += 1
        else:
            list_like = bool(LIST_RE.match(lines[index]))
            table_like = lines[index].lstrip().startswith("|")
            index += 1
            while index < len(lines) and lines[index].strip():
                if ATX_RE.match(lines[index]) or FENCE_RE.match(lines[index]):
                    break
                if list_like != bool(LIST_RE.match(lines[index])):
                    break
                if table_like != lines[index].lstrip().startswith("|"):
                    break
                index += 1
        ranges.append((start, index))

    blocks: list[MarkdownBlock] = []
    for number, (line_start, line_end) in enumerate(ranges, 1):
        character_start = character_starts[line_start]
        raw = "".join(lines[line_start:line_end])
        visible = raw.rstrip()
        if not visible:
            continue
        character_end = character_start + len(visible)
        start = len(markdown[:character_start].encode("utf-8"))
        end = len(markdown[:character_end].encode("utf-8"))
        blocks.append(
            MarkdownBlock(
                id=f"md-{number:06d}",
                kind=_kind(lines[line_start:line_end]),
                start=start,
                end=end,
                text=visible,
                normalized_text=normalize_for_alignment(visible),
            )
        )
    return tuple(blocks)
