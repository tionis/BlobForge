"""Recover corroborated chapter boundaries and topic tiers from retained layout."""

from __future__ import annotations

import bisect
import statistics

from ..markdown_outline import markdown_outline
from .book_structure import _entries
from .hierarchy import _key
from .topic_geometry import heading_scores


def _compact(title):
    return _key(title).replace(" ", "")


def _near(left, right):
    if left == right:
        return True
    if min(len(left), len(right)) < 8 or abs(len(left) - len(right)) > 1:
        return False
    if len(left) == len(right):
        return sum(a != b for a, b in zip(left, right)) == 1
    short, long = sorted((left, right), key=len)
    return any(short == long[:i] + long[i + 1 :] for i in range(len(long)))


def rebuild_sections(nodes, end):
    stack = []
    for node in nodes:
        start = node["heading"]["start"]
        while stack and stack[-1]["level"] >= node["level"]:
            stack.pop()["section"]["end"] = start
        node["parent"] = stack[-1]["id"] if stack else None
        node["section"] = {"start": start, "end": end}
        stack.append(node)


def repair_major_boundaries(text, pages, source_map, outline, report):
    """Require title, aligned page AND established chapter typography agreement."""
    majors = report.get("major_sections", [])
    offset = report.get("alignment_offset")
    if (
        len(majors) < 3
        or offset is None
        or report.get("strategy") != "toc-and-relative-geometry"
    ):
        return outline
    raw = markdown_outline(text, skip_fences=True)["nodes"]
    scores = heading_scores(raw, pages, source_map["mappings"])
    by_start = {n["heading"]["start"]: n for n in raw}
    observed = [
        scores[n["id"]]
        for m in majors
        if (n := by_start.get(m["byte_offset"])) and n["id"] in scores
    ]
    if len(observed) < 3:
        return outline
    reference = statistics.median(observed)
    entries = _entries([p for p in pages if p["index"] in report.get("toc_pages", [])])
    mappings = source_map["mappings"]
    starts = [m["document"]["start"] for m in mappings]
    occupied = {m["source_page"] for m in majors}
    additions = []
    for node in raw:
        score = scores.get(node["id"], 0)
        if not reference * 0.78 <= score <= reference * 1.35:
            continue
        index = bisect.bisect_right(starts, node["heading"]["start"]) - 1
        if index < 0:
            continue
        page = mappings[index]["source"]["selectors"][0]["start"]
        if page in occupied or page <= max(report.get("toc_pages", []), default=-1):
            continue
        matches = [
            e
            for e in entries.values()
            if str(int(page - offset)) in e["labels"]
            and _key(e["title"]) != _key(node["title"])
            and _near(_compact(e["title"]), _compact(node["title"]))
        ]
        if len(matches) != 1:
            continue
        peers = [
            n
            for n in raw
            if n["id"] in scores
            and reference * 0.78 <= scores[n["id"]] <= reference * 1.35
            and starts[index]
            <= n["heading"]["start"]
            < mappings[index]["document"]["end"]
            and _near(_compact(matches[0]["title"]), _compact(n["title"]))
        ]
        if len(peers) != 1:
            continue
        additions.append(
            {
                "title": matches[0]["title"],
                "byte_offset": node["heading"]["start"],
                "heading_end": node["heading"]["end"],
                "source_page": page,
                "evidence": "aligned-toc-and-chapter-typography",
            }
        )
        occupied.add(page)
    if not additions:
        return outline
    majors = sorted(majors + additions, key=lambda m: m["byte_offset"])
    report["major_sections"] = majors
    report["layout_recovered_majors"] = [m["title"] for m in additions]
    report["diagnostics"].append("layout_recovered_major_titles; review_required")
    encoded = text.encode()
    output = []
    if majors[0]["byte_offset"]:
        output.append(
            {
                "id": "front-matter",
                "title": "Front matter",
                "level": 2,
                "heading": {"start": 0, "end": len(encoded.split(b"\n", 1)[0]) or 1},
            }
        )
    for i, major in enumerate(majors):
        start = major["byte_offset"]
        end = majors[i + 1]["byte_offset"] if i + 1 < len(majors) else len(encoded)
        output.append(
            {
                "id": f"major-{i}",
                "title": major["title"],
                "level": 2,
                "heading": {"start": start, "end": major["heading_end"]},
            }
        )
        for node in raw:
            if start < node["heading"]["start"] < end:
                # Contiguous split opener text retains its authored anchor, but
                # does not become a duplicate topic boundary.
                if (
                    _compact(node["title"]) in _compact(major["title"])
                    and not encoded[
                        major["heading_end"] : node["heading"]["start"]
                    ].strip()
                ):
                    continue
                output.append({**node, "level": min(6, max(3, node["level"] + 1))})
    rebuild_sections(output, len(encoded))
    outline["nodes"] = output
    return outline
