"""Evidence-backed normalization of decorative PDF list glyphs."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

DECORATIVE_GLYPHS = "◆♦❖•·"
MARKDOWN_DECORATION_RE = re.compile(
    rf"^(?P<prefix>[ \t]*(?:[-+*]|\d+[.)])[ \t]+)[{DECORATIVE_GLYPHS}][ \t]+",
    re.MULTILINE,
)
BARE_DECORATION_RE = re.compile(
    rf"^(?P<indent>[ \t]*)[{DECORATIVE_GLYPHS}][ \t]+(?P<body>\S[\s\S]*)$"
)


def strip_markdown_list_decorations(value: str) -> tuple[str, int]:
    """Remove a decorative glyph only after an existing Markdown list marker."""
    return MARKDOWN_DECORATION_RE.subn(r"\g<prefix>", value)


def recover_typed_text_list_runs(
    blocks: Sequence[Mapping[str, Any]],
) -> tuple[dict[int, str], int]:
    """Recover runs of two or more provider text blocks with a leading glyph.

    A single glyph-led text block is ambiguous and remains unchanged. Inline
    mechanics such as ``At ♦`` never match because the glyph is not line-first.
    """
    replacements: dict[int, str] = {}
    recovered = 0
    index = 0
    while index < len(blocks):
        run: list[tuple[int, re.Match[str]]] = []
        cursor = index
        while cursor < len(blocks):
            block = blocks[cursor]
            if block.get("type") != "text" or not isinstance(block.get("content"), str):
                break
            match = BARE_DECORATION_RE.fullmatch(block["content"].strip())
            if match is None:
                break
            run.append((cursor, match))
            cursor += 1
        if len(run) >= 2:
            for block_index, match in run:
                replacements[block_index] = f"{match.group('indent')}- {match.group('body')}"
                recovered += 1
        index = cursor if cursor > index else index + 1
    return replacements, recovered
