"""Bound oversized front matter using explicit, independently retained evidence."""

from __future__ import annotations

import bisect
import re
import statistics
from itertools import pairwise

from ..markdown_outline import markdown_outline
from .book_structure import CONTENTS, _entries
from .hierarchy import _key, _title
from .layout_structure import rebuild_sections
from .topic_geometry import heading_scores


def _rebuild(text, outline, majors):
    raw = markdown_outline(text, skip_fences=True)["nodes"]
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
    for index, major in enumerate(majors):
        start = major["byte_offset"]
        end = (
            majors[index + 1]["byte_offset"]
            if index + 1 < len(majors)
            else len(encoded)
        )
        output.append(
            {
                "id": f"major-{index}",
                "title": major["title"],
                "level": 2,
                "heading": {"start": start, "end": major["heading_end"]},
            }
        )
        compact_major = _key(major["title"]).replace(" ", "")
        opener_starts = set()
        leading = [node for node in raw if start < node["heading"]["start"] < end][:3]
        combined = ""
        cursor = None
        for node in leading:
            if (
                cursor is not None
                and encoded[cursor["heading"]["end"] : node["heading"]["start"]].strip()
            ):
                break
            combined = f"{combined} {node['title']}".strip()
            opener_starts.add(node["heading"]["start"])
            cursor = node
            if _key(combined).replace(" ", "") == compact_major:
                break
        if _key(combined).replace(" ", "") != compact_major:
            opener_starts.clear()
        last_index_letter = None
        for node in raw:
            if start < node["heading"]["start"] < end:
                if major["evidence"] == "observed-contents-cluster":
                    continue
                if node["heading"]["start"] < major["heading_end"]:
                    continue
                if node["heading"]["start"] in opener_starts:
                    continue
                if major["evidence"] == "alphabetical-backmatter-sequence":
                    letter = node["title"].strip()
                    if re.fullmatch(r"[A-Z]", letter):
                        if letter == last_index_letter:
                            continue
                        last_index_letter = letter
                output.append({**node, "level": min(6, max(3, node["level"] + 1))})
    rebuild_sections(output, len(encoded))
    outline["nodes"] = output
    return outline


def recover_frontmatter_boundaries(text, pages, source_map, outline, report):
    """Split front matter only at contents and corroborated page landmarks."""
    majors = report.get("major_sections", [])
    toc_pages = report.get("toc_pages", [])
    if not majors or not toc_pages or majors[0]["byte_offset"] <= 50_000:
        return outline
    mappings = source_map["mappings"]
    by_page = {m["source"]["selectors"][0]["start"]: m for m in mappings}
    raw = markdown_outline(text, skip_fences=True)["nodes"]
    starts = [mapping["document"]["start"] for mapping in mappings]

    def page_of(node):
        index = bisect.bisect_right(starts, node["heading"]["start"]) - 1
        return (
            mappings[index]["source"]["selectors"][0]["start"] if index >= 0 else None
        )

    first_major_page = majors[0]["source_page"]
    entries = _entries([page for page in pages if page["index"] in toc_pages])
    scores = heading_scores(raw, pages, mappings)
    major_scores = []
    for major in majors:
        candidates = [n for n in raw if n["heading"]["start"] == major["byte_offset"]]
        if candidates and candidates[0]["id"] in scores:
            major_scores.append(scores[candidates[0]["id"]])
    reference = statistics.median(major_scores) if len(major_scores) >= 3 else None
    additions = []

    toc_start = by_page.get(toc_pages[0])
    toc_node = next(
        (n for n in raw if page_of(n) == toc_pages[0] and _key(n["title"]) in CONTENTS),
        None,
    )
    if toc_start and toc_node:
        additions.append(
            {
                "title": toc_node["title"],
                "source_page": toc_pages[0],
                "byte_offset": toc_start["document"]["start"],
                "heading_end": toc_node["heading"]["end"],
                "evidence": "observed-contents-cluster",
            }
        )

    after_toc = toc_pages[-1] + 1
    first_after = next((n for n in raw if page_of(n) == after_toc), None)
    if first_after:
        additions.append(
            {
                "title": first_after["title"],
                "source_page": after_toc,
                "byte_offset": by_page[after_toc]["document"]["start"],
                "heading_end": first_after["heading"]["end"],
                "evidence": "authored-heading-after-contents",
            }
        )

    unlabeled = set()
    for page in pages:
        if page["index"] not in toc_pages:
            continue
        for block in page.get("blocks", []):
            content = _title(block.get("content", ""))
            if (
                block.get("type") in {"text", "title"}
                and "\n" in block.get("content", "")
                and not re.search(r"\d+\s*$", content)
                and content.isupper()
            ):
                unlabeled.add(_key(content))
    for key in unlabeled:
        candidates = [
            n
            for n in raw
            if _key(n["title"]) == key and toc_pages[-1] < page_of(n) < first_major_page
        ]
        pair = None
        if not candidates:
            pairs = [
                (left, right)
                for left, right in pairwise(raw)
                if toc_pages[-1] < page_of(left) < first_major_page
                and page_of(left) == page_of(right)
                and _key(f"{left['title']} {right['title']}") == key
                and not text.encode()[
                    left["heading"]["end"] : right["heading"]["start"]
                ].strip()
            ]
            if len(pairs) == 1:
                pair = pairs[0]
                candidates = [pair[0]]
        if len(candidates) != 1:
            continue
        node = candidates[0]
        score = scores.get(node["id"])
        if (
            reference is not None
            and score is not None
            and not reference * 0.65 <= score <= reference * 1.5
        ):
            continue
        additions.append(
            {
                "title": f"{pair[0]['title']} {pair[1]['title']}"
                if pair
                else node["title"],
                "source_page": page_of(node),
                "byte_offset": node["heading"]["start"],
                "heading_end": pair[1]["heading"]["end"]
                if pair
                else node["heading"]["end"],
                "evidence": "unlabeled-contents-title-and-unique-body-heading",
            }
        )

    for page in pages:
        if not toc_pages[-1] < page["index"] < first_major_page:
            continue
        headers = [
            _title(block.get("content", ""))
            for block in page.get("blocks", [])
            if block.get("type") == "header"
        ]
        for title in headers:
            if _key(title) not in entries:
                continue
            candidates = [n for n in raw if page_of(n) == page["index"]]
            if not candidates:
                continue
            node = candidates[0]
            additions.append(
                {
                    "title": title,
                    "source_page": page["index"],
                    "byte_offset": by_page[page["index"]]["document"]["start"],
                    "heading_end": node["heading"]["end"],
                    "evidence": "contents-title-and-native-page-header",
                }
            )

    existing = {major["byte_offset"] for major in majors}
    additions = sorted(
        {
            item["byte_offset"]: item
            for item in additions
            if item["byte_offset"] not in existing
        }.values(),
        key=lambda item: item["byte_offset"],
    )
    if not additions:
        return outline
    report["major_sections"] = sorted(
        majors + additions, key=lambda item: item["byte_offset"]
    )
    report["frontmatter_recovered_boundaries"] = additions
    report["diagnostics"].append(
        "frontmatter_landmarks_recovered_from_retained_evidence; review_required"
    )
    return _rebuild(text, outline, report["major_sections"])


def recover_document_landmarks(text, pages, source_map, outline, report):
    """Recover bounded front matter and structurally explicit alphabetical backmatter."""
    outline = recover_frontmatter_boundaries(text, pages, source_map, outline, report)
    majors = report.get("major_sections", [])
    if not majors:
        return outline
    raw = markdown_outline(text, skip_fences=True)["nodes"]
    mappings = source_map["mappings"]
    starts = [mapping["document"]["start"] for mapping in mappings]

    def page_of(node):
        index = bisect.bisect_right(starts, node["heading"]["start"]) - 1
        return (
            mappings[index]["source"]["selectors"][0]["start"] if index >= 0 else None
        )

    tail = [
        node for node in raw if node["heading"]["start"] > majors[-1]["byte_offset"]
    ]
    letters = [node for node in tail if re.fullmatch(r"[A-Z]", node["title"].strip())]
    collapsed = []
    for node in letters:
        if not collapsed or collapsed[-1]["title"] != node["title"]:
            collapsed.append(node)
    if len(collapsed) < 5 or [node["title"] for node in collapsed] != sorted(
        node["title"] for node in collapsed
    ):
        return outline
    first = collapsed[0]
    preceding = [
        node
        for node in tail
        if node["heading"]["start"] < first["heading"]["start"]
        and page_of(node) == page_of(first)
        and len(node["title"].strip()) > 3
    ]
    if not preceding:
        return outline
    candidate = preceding[-1]
    if first["heading"]["start"] - candidate["heading"]["end"] > 2_000:
        return outline
    boundary = {
        "title": candidate["title"],
        "source_page": page_of(candidate),
        "byte_offset": candidate["heading"]["start"],
        "heading_end": candidate["heading"]["end"],
        "evidence": "alphabetical-backmatter-sequence",
    }
    if boundary["byte_offset"] in {major["byte_offset"] for major in majors}:
        return outline
    report["major_sections"] = sorted(
        majors + [boundary], key=lambda item: item["byte_offset"]
    )
    report["backmatter_recovered_boundaries"] = [boundary]
    report["diagnostics"].append(
        "alphabetical_backmatter_recovered_from_authored_sequence; review_required"
    )
    return _rebuild(text, outline, report["major_sections"])
