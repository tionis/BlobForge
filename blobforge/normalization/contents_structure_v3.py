"""Corroborated wrapped-title recovery layered over bounded contents v2."""

from __future__ import annotations

import bisect
import re
from itertools import pairwise

from .contents_structure_v2 import _alias_key, _wrapped_rows, recover_contents_topics_v2
from .hierarchy import _title


def _page_of(node, mappings, starts):
    index = bisect.bisect_right(starts, node["heading"]["start"]) - 1
    return mappings[index]["source"]["selectors"][0]["start"] if index >= 0 else None


def _joined_block_candidates(page):
    """Yield visually continuous wrapped contents rows, never arbitrary lines."""
    blocks = [
        block
        for block in page.get("blocks", [])
        if block.get("type") in {"text", "title"}
    ]
    for block in blocks:
        lines = [
            line.strip()
            for line in block.get("content", "").splitlines()
            if line.strip()
        ]
        if len(lines) >= 2 and re.fullmatch(r".+?\s+\d+\**", lines[-1]):
            yield lines
    width = page.get("dimensions", {}).get("width", 0)
    height = page.get("dimensions", {}).get("height", 0)
    if not width or not height:
        return
    for left, right in pairwise(blocks):
        first = left.get("content", "").strip()
        second = right.get("content", "").strip()
        if "\n" in first or "\n" in second:
            continue
        if not first or re.search(r"\d+\**$", first):
            continue
        if not re.fullmatch(r".+?\s+\d+\**", second):
            continue
        gap = right.get("top_left_y", 0) - left.get("bottom_right_y", 0)
        indent = right.get("top_left_x", 0) - left.get("top_left_x", 0)
        edge = abs(right.get("bottom_right_x", 0) - left.get("bottom_right_x", 0))
        if (
            -0.001 * height <= gap <= 0.004 * height
            and 0 <= indent <= 0.02 * width
            and edge <= 0.006 * width
            and _title(first).isupper()
            and _title(second.rsplit(" ", 1)[0]).isupper()
        ):
            yield [first, second]


def _replace_lines(markdown, parts):
    lines = markdown.splitlines()
    nonblank = [i for i, line in enumerate(lines) if line.strip()]
    clean = [_title(part) for part in parts]
    matches = []
    for position in range(len(nonblank) - len(clean) + 1):
        indexes = nonblank[position : position + len(clean)]
        if [_title(lines[index]) for index in indexes] != clean:
            continue
        suffix = re.search(r"\s+\d+\**$", lines[indexes[-1]].strip())
        if suffix is not None:
            matches.append(indexes)
    if len(matches) != 1:
        return markdown
    indexes = matches[0]
    joined = " ".join(_title(lines[index]) for index in indexes[:-1])
    joined += " " + _title(lines[indexes[-1]])
    lines[indexes[0]] = joined
    for index in indexes[1:]:
        lines[index] = ""
    return "\n".join(lines)


def _corroborated_joined_pages(outline, pages, source_map, report):
    mappings = source_map["mappings"]
    starts = [mapping["document"]["start"] for mapping in mappings]
    offset = report.get("alignment_offset")
    nodes = outline["nodes"]
    major_keys = {
        _alias_key(major["title"]) for major in report.get("major_sections", [])
    }
    result = []
    repaired = []
    for page in pages:
        if page["index"] not in report.get("toc_pages", []):
            result.append(page)
            continue
        markdown = page["markdown"]
        for parts in _joined_block_candidates(page):
            match = re.fullmatch(r"(.+?)\s+(\d+)\**", _title(parts[-1]))
            if match is None:
                continue
            title = " ".join([*map(_title, parts[:-1]), match[1]])
            alias = _alias_key(title)
            target = int(match[2]) + offset if offset is not None else None
            direct = [
                node
                for node in nodes
                if _alias_key(node["title"]) == alias
                and (target is None or _page_of(node, mappings, starts) == target)
            ]
            pairs = [
                (left, right)
                for left, right in pairwise(nodes)
                if _alias_key(f"{left['title']} {right['title']}") == alias
                and (target is None or _page_of(left, mappings, starts) == target)
                and _page_of(left, mappings, starts)
                == _page_of(right, mappings, starts)
            ]
            if alias not in major_keys and len(direct) + len(pairs) != 1:
                continue
            updated = _replace_lines(markdown, parts)
            if updated != markdown:
                markdown = updated
                repaired.append(title)
        result.append({**page, "markdown": markdown})
    return result, repaired


def _join_split_body_titles(text, outline, pages, source_map, report):
    rows = list(_wrapped_rows(pages, report.get("toc_pages", [])))
    offset = report.get("alignment_offset")
    if offset is None:
        return []
    mappings = source_map["mappings"]
    starts = [mapping["document"]["start"] for mapping in mappings]
    encoded = text.encode()
    nodes = outline["nodes"]
    repaired = []
    for row in rows:
        if row["label"] is None:
            continue
        target = row["label"] + offset
        direct = [node for node in nodes if _alias_key(node["title"]) == row["alias"]]
        if direct:
            continue
        pairs = []
        for left, right in pairwise(nodes):
            if (
                left["level"] == right["level"]
                and _page_of(left, mappings, starts) == target
                and _page_of(right, mappings, starts) == target
                and _alias_key(f"{left['title']} {right['title']}") == row["alias"]
                and not encoded[
                    left["heading"]["end"] : right["heading"]["start"]
                ].strip()
            ):
                pairs.append((left, right))
        if len(pairs) != 1:
            continue
        left, right = pairs[0]
        left["title"] = f"{left['title']} {right['title']}"
        repaired.append(left["title"])
    return repaired


def recover_contents_topics_v3(text, outline, pages, source_map, report):
    """Repair only wrapped titles corroborated by aligned body structure."""
    repaired_pages, toc_repairs = _corroborated_joined_pages(
        outline, pages, source_map, report
    )
    body_repairs = _join_split_body_titles(
        text, outline, repaired_pages, source_map, report
    )
    outline = recover_contents_topics_v2(outline, repaired_pages, source_map, report)
    report["topic_hierarchy"]["method"] = "corroborated-wrapped-contents-v3"
    if toc_repairs or body_repairs:
        report["wrapped_title_repairs"] = {
            "contents": toc_repairs,
            "body": body_repairs,
        }
        report["diagnostics"].append(
            "wrapped_titles_reconciled_by_page_and_geometry; review_required"
        )
    return outline
