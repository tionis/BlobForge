"""Recover topic tiers from explicitly styled contents tables, not book names."""
from __future__ import annotations

import bisect
import re
from collections import Counter

from .hierarchy import _key, _title


def _contents_rows(pages, toc_pages):
    """Read paired title/page columns down each column, never across rows."""
    stream = []
    for page in pages:
        if page["index"] not in toc_pages:
            continue
        lines = page["markdown"].splitlines()
        index = 0
        while index < len(lines):
            line = lines[index].strip()
            if line.startswith("|"):
                table = []
                while index < len(lines) and lines[index].strip().startswith("|"):
                    table.append([c.strip() for c in lines[index].strip().strip("|").split("|")])
                    index += 1
                width = max(map(len, table))
                if width % 2 == 0:
                    for column in range(0, width, 2):
                        for row in table:
                            if len(row) == width and _title(row[column + 1]).strip().isdecimal():
                                stream.append((row[column], int(_title(row[column + 1]).strip())))
                continue
            index += 1
            title = re.sub(r"^#{1,6}\s+", "", line)
            following = next((v.strip() for v in lines[index:] if v.strip()), "")
            if _key(title) in {"table of contents", "contents", "inhalt", "sommaire"}:
                continue
            match = re.fullmatch(r"(.+?)\s+(\d+)\**", title)
            if line.startswith("#") and _title(following).strip().isdecimal():
                stream.append((title, int(_title(following).strip())))
            elif match:
                stream.append((match[1], int(match[2])))
            elif title and not _title(title).strip().isdecimal():
                # OCR sometimes separates a heading from its page-number line.
                if _title(following).strip().isdecimal():
                    stream.append((title, int(_title(following).strip())))
                elif line.startswith("#"):
                    stream.append((title, None))
    return stream


def recover_topics(outline, pages, source_map, report):
    """Use chapter-scoped, page-disambiguated TOC evidence; retain uncertain tiers.

    Bold rows introduce topics. Plain rows are their children; italic rows are
    children of the latest plain row, or of the topic when no plain row exists.
    Only tables with repeated bold and subordinate evidence qualify. Missing
    topic anchors reject that chapter rather than absorbing it into a neighbour.
    """
    majors = report.get("major_sections", [])
    groups = {_key(m["title"]): [] for m in majors}
    current = None
    for title, label in _contents_rows(pages, report.get("toc_pages", [])):
        numbered_key = _key(f"{title} {label}") if label is not None else ""
        if numbered_key in groups:
            current = numbered_key
            continue
        if _key(title) in groups:
            current = _key(title)
            continue
        if current is None or label is None:
            continue
        style = "bold" if re.fullmatch(r"\*\*.+\*\*", title) else (
            "italic" if re.fullmatch(r"\*[^*]+\*", title) else "plain")
        groups[current].append((_key(title), label, style))

    nodes = outline["nodes"]
    if not nodes:
        report["diagnostics"].append("topic_tiers_unverified; empty_outline")
        return outline
    mappings = source_map["mappings"]
    starts = [m["document"]["start"] for m in mappings]
    def page_of(node):
        index = bisect.bisect_right(starts, node["heading"]["start"]) - 1
        return mappings[index]["source"]["selectors"][0]["start"] if index >= 0 else None

    recovered = []
    for major_index, major in enumerate(majors):
        rows = groups[_key(major["title"])]
        styles = Counter(row[2] for row in rows)
        if styles["bold"] < 3 or styles["plain"] + styles["italic"] < 3:
            continue
        chapter = next(n for n in nodes if n["id"] == f"major-{major_index}")
        children = [n for n in nodes if chapter["section"]["start"] < n["heading"]["start"] < chapter["section"]["end"]]
        anchors = {}
        unmatched = []
        topic = plain = None
        last_start = -1
        for key, label, style in rows:
            candidates = [n for n in children if _key(n["title"]) == key]
            if len(candidates) > 1 and report.get("alignment_offset") is not None:
                candidates = [n for n in candidates if page_of(n) == label + report["alignment_offset"]]
            if len(candidates) > 1:
                # A table caption and prose heading can repeat consecutively on
                # one page. They form one contiguous topic, not competing routes.
                first, last = children.index(candidates[0]), children.index(candidates[-1])
                if len({page_of(n) for n in candidates}) == 1 and all(
                    _key(n["title"]) == key for n in children[first:last + 1]
                ):
                    candidates = candidates[:1]
            if len(candidates) != 1 or candidates[0]["heading"]["start"] <= last_start:
                unmatched.append((key, style))
                continue
            node = candidates[0]
            if style == "bold":
                topic, plain, level = node["id"], None, 3
            elif topic is None:
                unmatched.append((key, style))
                continue
            elif style == "plain":
                plain, level = node["id"], 4
            else:
                level = 5 if plain else 4
            anchors[node["id"]] = level
            last_start = node["heading"]["start"]
        if any(style == "bold" for _, style in unmatched) or len(anchors) < len(rows) * .85:
            report["diagnostics"].append(f"styled_toc_alignment_incomplete: {major['title']}: {unmatched}")
            continue
        active = 3
        for node in children:
            if node["id"] in anchors:
                active = anchors[node["id"]]
                node["level"] = active
            else:
                node["level"] = min(6, active + 1)
        recovered.append({"chapter": major["title"], "matched_entries": len(anchors),
                          "entries": len(rows), "unmatched": [key for key, _ in unmatched]})
    report["topic_hierarchy"] = {"method": "styled-toc-v1", "chapters": recovered}
    if not recovered:
        report["diagnostics"].append("topic_tiers_unverified; retained_ocr_subheadings")
    else:
        report["diagnostics"].append("unlisted_topic_headings_inherit_context; review_required")
    stack = []
    end = max(n["section"]["end"] for n in nodes)
    for node in nodes:
        while stack and stack[-1]["level"] >= node["level"]:
            stack.pop()["section"]["end"] = node["heading"]["start"]
        node["parent"] = stack[-1]["id"] if stack else None
        node["section"] = {"start": node["heading"]["start"], "end": end}
        stack.append(node)
    return outline
