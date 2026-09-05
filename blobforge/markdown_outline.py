"""Dependency-free Markdown structure shared by normalization and MDAF packaging."""

from __future__ import annotations

import re
from typing import Any

MARKDOWN_HEADING_RE = re.compile(r"^(#{1,6})[ \t]+(.+?)\s*$", re.MULTILINE)


def markdown_outline(text: str, *, skip_fences: bool = False) -> dict[str, Any]:
    """Derive a conservative byte-aligned outline from ATX headings."""
    candidates = []
    fenced_spans = []
    if skip_fences:
        offset = 0
        fence = None
        for line in text.splitlines(keepends=True):
            match = re.match(r"^ {0,3}(`{3,}|~{3,})(.*)$", line.rstrip("\r\n"))
            if match:
                marker, tail = match.groups()
                if fence is None and not (marker[0] == "`" and "`" in tail):
                    fence = (marker[0], len(marker), offset)
                elif fence and marker[0] == fence[0] and len(marker) >= fence[1] and not tail.strip():
                    fenced_spans.append((fence[2], offset + len(line)))
                    fence = None
            offset += len(line)
        if fence:
            fenced_spans.append((fence[2], len(text)))
    for heading in MARKDOWN_HEADING_RE.finditer(text):
        if any(start <= heading.start() < end for start, end in fenced_spans):
            continue
        title = re.sub(r"<[^>]+>", "", heading.group(2))
        title = re.sub(r"!?\[([^]]*)\]\([^)]*\)", r"\1", title)
        title = re.sub(r"[*_`~]", "", title).strip()
        if title:
            candidates.append((heading, title))
    nodes: list[dict[str, Any]] = []
    parents: list[tuple[int, str]] = []
    document_end = len(text.encode("utf-8"))
    for index, (heading, title) in enumerate(candidates):
        level = len(heading.group(1))
        while parents and parents[-1][0] >= level:
            parents.pop()
        node_id = f"heading-{index + 1}"
        start = len(text[: heading.start()].encode("utf-8"))
        heading_end = len(text[: heading.end()].encode("utf-8"))
        section_end = document_end
        for following, _ in candidates[index + 1 :]:
            if len(following.group(1)) <= level:
                section_end = len(text[: following.start()].encode("utf-8"))
                break
        nodes.append(
            {
                "id": node_id,
                "parent": parents[-1][1] if parents else None,
                "level": level,
                "title": title,
                "heading": {"start": start, "end": heading_end},
                "section": {"start": start, "end": section_end},
            }
        )
        parents.append((level, node_id))
    return {"nodes": nodes}

