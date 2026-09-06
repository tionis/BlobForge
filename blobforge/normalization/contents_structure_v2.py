"""Bounded contents recovery around independently located evidence gaps.

Keep styled-toc-v1 frozen. These passes propose only chapter-local, aligned
anchors and retain the previous outline when the evidence is incomplete.
"""

from __future__ import annotations

import bisect
import re
import statistics
import unicodedata
from collections import Counter
from itertools import pairwise

from .hierarchy import _key, _title
from .layout_structure import rebuild_sections
from .topics import _contents_rows, recover_topics


def _native_widths(pages, toc_pages):
    """Widths of single contents entries whose number column is corroborated.

    Page-relative distances make this independent of rendering DPI. Do not use
    widths from arbitrary prose or from blocks containing multiple entries.
    """
    result = {}
    for page in pages:
        width = page.get("dimensions", {}).get("width", 0)
        if width <= 0 or page["index"] not in toc_pages:
            continue
        entries = []
        for block in page.get("blocks", []):
            content = _title(block.get("content", ""))
            match = re.fullmatch(r"(.+?)\s+(\d+)", content)
            left, right = block.get("top_left_x", 0), block.get("bottom_right_x", 0)
            if match and right > left and block.get("type") in {"text", "title"}:
                entries.append(
                    (_key(match[1]), int(match[2]), left / width, right / width)
                )
        for key, label, left, right in entries:
            peers = [e[3] for e in entries if abs(e[3] - right) <= 0.006]
            if len(peers) >= 8:
                result.setdefault((key, label), []).append(
                    statistics.median(peers) - left
                )
    return {
        key: statistics.mean(values)
        for key, values in result.items()
        if max(values) - min(values) <= 0.004
    }


def _indent_levels(rows, widths, minimum=3, allow_missing=False):
    values = [widths.get((row["key"], row["label"])) for row in rows]
    if sum(v is not None for v in values) < len(rows) * 0.9:
        return None
    clusters = []
    for value in sorted(v for v in values if v is not None):
        if not clusters or value - clusters[-1][0] > 0.004:
            clusters.append([value])
        else:
            clusters[-1].append(value)
    tiers = sorted(
        (statistics.mean(c) for c in clusters if len(c) >= minimum), reverse=True
    )
    if not 2 <= len(tiers) <= 3 or any(a - b < 0.006 for a, b in pairwise(tiers)):
        return None
    levels = []
    for value in values:
        if value is None:
            if not allow_missing:
                return None
            levels.append(None)
            continue
        nearest = min(range(len(tiers)), key=lambda i: abs(tiers[i] - value))
        if abs(tiers[nearest] - value) > 0.004:
            return None
        levels.append(3 + nearest)
    return levels


def _rows(pages, toc_pages):
    headings = {}
    for page in pages:
        if page["index"] not in toc_pages:
            continue
        for line in page["markdown"].splitlines():
            match = re.match(r"^(#{1,6})\s+(.+)", line.strip())
            if match:
                suffix = re.fullmatch(r"(.+?)\s+(\d+)\**", match[2])
                title, label = (
                    (suffix[1], int(suffix[2])) if suffix else (match[2], None)
                )
                headings.setdefault((_key(title), label), set()).add(len(match[1]))
    for title, label in _contents_rows(pages, toc_pages):
        clean = _title(title)
        if clean.isdecimal():
            continue
        heading = headings.get((_key(title), label), set())
        style = (
            "bold"
            if re.fullmatch(r"\*\*.+\*\*", title)
            else "italic"
            if re.fullmatch(r"\*[^*]+\*", title)
            else "plain"
        )
        yield {
            "key": _key(title),
            "alias": _alias_key(title),
            "label": label,
            "style": style,
            "heading": next(iter(heading)) if len(heading) == 1 else None,
            "caps": clean.isupper() and sum(c.isalpha() for c in clean) >= 4,
        }


def _step_prefix(rows):
    """A complete, consecutive numbered procedure belongs to its preceding row."""
    numbers = {
        word: i
        for i, word in enumerate(
            [
                "one",
                "two",
                "three",
                "four",
                "five",
                "six",
                "seven",
                "eight",
                "nine",
                "ten",
            ],
            1,
        )
    }
    if len(rows) < 4:
        return None
    count = 0
    tail = False
    for index, row in enumerate(rows[1:], 1):
        match = re.match(r"^(?:step|schritt) (\w+)\b", row["key"])
        if not match:
            tail = True
            continue
        number = int(match[1]) if match[1].isdecimal() else numbers.get(match[1])
        if tail or number != index:
            return None
        count += 1
    if count < 3:
        return None
    return [3] + [4] * (len(rows) - 1)


def _alias_key(value):
    value = unicodedata.normalize("NFKD", value.casefold().replace("&", " and "))
    return _key("".join(c for c in value if not unicodedata.combining(c)))


def _wrapped_rows(pages, toc_pages):
    repaired = []
    boundaries = set()
    for page in pages:
        if page["index"] not in toc_pages:
            continue
        lines = page["markdown"].splitlines()
        for index, line in enumerate(lines):
            if not re.fullmatch(r"#{1,6}\s+[^|]+:\s*", line.strip()):
                continue
            following = next(
                (i for i in range(index + 1, len(lines)) if lines[i].strip()), None
            )
            if following is None:
                continue
            title = re.sub(r"^#{1,6}\s+", "", lines[following].strip())
            match = re.fullmatch(r"(.+?)\s+(\d+)\**", title)
            if not match or "|" in title:
                continue
            lines[index] = line.rstrip() + " " + title
            lines[following] = ""
            boundaries.add(
                (_key(re.sub(r"^#{1,6}\s+", "", line) + " " + match[1]), int(match[2]))
            )
        repaired.append({**page, "markdown": "\n".join(lines)})
    for row in _rows(repaired, toc_pages):
        row["boundary"] = (row["key"], row["label"]) in boundaries
        yield row


def recover_contents_topics_v2(outline, pages, source_map, report):
    """Extend recovery for explicit heading and plain/italic contents tiers.

    Typography in body text alone never creates a topic. Caps count only when
    mixed-case subordinate contents rows establish a repeated contrast. Missing
    top-level anchors fence uncertain regions; unlocatable gaps reject a chapter.
    """
    outline = recover_topics(outline, pages, source_map, report)
    recovered = report.setdefault("topic_hierarchy", {"chapters": []})["chapters"]
    done = {_key(item["chapter"]) for item in recovered}
    majors = report.get("major_sections", [])
    groups = {_key(m["title"]): [] for m in majors}
    current = None
    offset = report.get("alignment_offset")
    major_pages = [m.get("source_page", -1) for m in majors]
    for row in _wrapped_rows(pages, report.get("toc_pages", [])):
        numbered = _key(f"{row['key']} {row['label']}")
        if offset is not None and row["label"] is not None:
            index = bisect.bisect_right(major_pages, row["label"] + offset) - 1
            current = _key(majors[index]["title"]) if index >= 0 else None
        if current is not None and (row["key"] == current or numbered == current):
            continue
        if row["label"] is None and row["key"] in groups:
            current = row["key"]
        elif current is not None:
            groups[current].append(row)
    nodes = outline["nodes"]
    mappings = source_map["mappings"]
    starts = [m["document"]["start"] for m in mappings]
    widths = _native_widths(pages, report.get("toc_pages", []))

    def page_of(node):
        i = bisect.bisect_right(starts, node["heading"]["start"]) - 1
        return mappings[i]["source"]["selectors"][0]["start"] if i >= 0 else None

    for index, major in enumerate(majors):
        key = _key(major["title"])
        if key in done:
            continue
        rows = groups[key]
        # Multi-column OCR can interleave distant printed pages or repeat a
        # contents row. Page order is evidence; repeated rows are not votes.
        unique = {}
        for row in rows:
            identity = (row["key"], row["label"])
            if identity not in unique or row["style"] == "bold":
                unique[identity] = row
        rows = list(unique.values())
        if rows and all(row["label"] is not None for row in rows):
            rows.sort(key=lambda row: row["label"])
        styles = Counter(row["style"] for row in rows)
        heads = Counter(row["heading"] for row in rows if row["heading"] is not None)
        caps = sum(row["caps"] for row in rows)
        chapter = next(n for n in nodes if n["id"] == f"major-{index}")
        children = [
            n
            for n in nodes
            if chapter["section"]["start"]
            < n["heading"]["start"]
            < chapter["section"]["end"]
        ]

        if styles["bold"] >= 3 and styles["plain"] + styles["italic"] >= 3:
            method = "mixed-styled-contents"
            first = next(i for i, row in enumerate(rows) if row["style"] == "bold")
            prefix = _indent_levels(rows[:first], widths, minimum=2) if first else []
            if prefix is None:
                prefix = _step_prefix(rows[:first])
            if prefix is None:
                prefix_rows = rows[:first]
                prefix_heads = Counter(
                    r["heading"] for r in prefix_rows if r["heading"] is not None
                )
                if (
                    prefix_heads
                    and prefix_heads[min(prefix_heads)] >= 2
                    and len(prefix_rows) - sum(prefix_heads.values()) >= 3
                ):
                    prefix = [
                        3 + min(2, r["heading"] - min(prefix_heads))
                        if r["heading"] is not None
                        else 4
                        for r in prefix_rows
                    ]
                elif sum(r["style"] == "plain" for r in prefix_rows) >= 2 and any(
                    r["style"] == "italic" for r in prefix_rows
                ):
                    prefix = [4 if r["style"] == "italic" else 3 for r in prefix_rows]
            if prefix is None:
                continue
            levels = prefix + [
                3 if row["style"] == "bold" else 5 if row["style"] == "italic" else 4
                for row in rows[first:]
            ]
            # Styling can disappear on a later contents page. A long unstyled
            # tail is not evidence that every remaining topic belongs to the
            # last bold row. Recover native tiers or reject this mixed pass.
            last_styled = max(i for i, r in enumerate(rows) if r["style"] != "plain")
            tail = rows[last_styled + 1 :]
            if len(tail) >= 20:
                tail_levels = _indent_levels(tail, widths, allow_missing=True)
                if tail_levels is None:
                    report["diagnostics"].append(
                        f"contents_style_transition_unresolved: {major['title']}"
                    )
                    continue
                for position, level in enumerate(tail_levels, last_styled + 1):
                    if level is not None:
                        levels[position] = level
        elif heads and heads[min(heads)] >= 3:
            method = "explicit-contents-headings"
            levels = [
                3 + min(2, row["heading"] - min(heads))
                if row["heading"] is not None
                else 4
                for row in rows
            ]
        elif not styles["bold"] and styles["plain"] >= 3 and styles["italic"] >= 3:
            method = "plain-italic-contents"
            levels = [4 if row["style"] == "italic" else 3 for row in rows]
            last_styled = max(i for i, r in enumerate(rows) if r["style"] == "italic")
            tail = rows[last_styled + 1 :]
            if len(tail) >= 3:
                tail_levels = _indent_levels(tail, widths)
                if tail_levels is None:
                    report["diagnostics"].append(
                        f"contents_style_transition_unresolved: {major['title']}"
                    )
                    continue
                levels[last_styled + 1 :] = tail_levels
        elif (
            not styles["bold"]
            and not styles["italic"]
            and caps >= 3
            and len(rows) - caps >= 3
        ):
            method = "case-contrasted-contents"
            levels = [3 if row["caps"] else 4 for row in rows]
        elif (
            not styles["bold"]
            and not styles["italic"]
            and (indents := _indent_levels(rows, widths))
        ):
            method = "column-aligned-contents-indentation"
            levels = indents
        else:
            continue
        levels = [
            3 if row.get("boundary") else level for row, level in zip(rows, levels)
        ]
        original_levels = {n["id"]: n["level"] for n in children}
        anchors, unmatched, missing_pages = {}, [], []
        last = -1
        for row, level in zip(rows, levels):
            candidates = [n for n in children if _key(n["title"]) == row["key"]]
            if (
                not candidates
                and row["label"] is not None
                and report.get("alignment_offset") is not None
            ):
                candidates = [
                    n
                    for n in children
                    if _alias_key(re.sub(r"\s*\([^()]+\)\s*$", "", n["title"]))
                    == row["alias"]
                    and page_of(n) == row["label"] + report["alignment_offset"]
                ]
            if (
                len(candidates) > 1
                and row["label"] is not None
                and report.get("alignment_offset") is not None
            ):
                candidates = [
                    n
                    for n in candidates
                    if page_of(n) == row["label"] + report["alignment_offset"]
                ]
            if len(candidates) > 1:
                first, last_index = (
                    children.index(candidates[0]),
                    children.index(candidates[-1]),
                )
                if len({page_of(n) for n in candidates}) == 1 and all(
                    _key(n["title"]) == row["key"]
                    for n in children[first : last_index + 1]
                ):
                    candidates = candidates[:1]
            if len(candidates) != 1 or candidates[0]["heading"]["start"] <= last:
                unmatched.append((row["key"], level))
                if level == 3:
                    missing_pages.append(
                        None
                        if row["label"] is None or offset is None
                        else row["label"] + offset
                    )
                continue
            node = candidates[0]
            anchors[node["id"]] = level
            last = node["heading"]["start"]
        guards = []
        top_starts = [
            n["heading"]["start"] for n in children if anchors.get(n["id"]) == 3
        ]
        for page in missing_pages:
            page_mappings = (
                [m for m in mappings if m["source"]["selectors"][0]["start"] == page]
                if page is not None
                else []
            )
            if not page_mappings:
                guards = None
                break
            page_start = min(m["document"]["start"] for m in page_mappings)
            page_end = max(m["document"]["end"] for m in page_mappings)
            # Retain the preceding topic as well: a coarse page location cannot
            # tell which of its paragraphs belong to the missing heading.
            start = max(
                (s for s in top_starts if s < page_start),
                default=chapter["section"]["start"],
            )
            # A preserved region must begin at an original chapter child, not
            # a deeper heading that could inherit a newly promoted neighbour.
            start = max(
                (
                    n["heading"]["start"]
                    for n in children
                    if n["heading"]["start"] <= start and original_levels[n["id"]] == 3
                ),
                default=chapter["section"]["start"],
            )
            end = min(
                (s for s in top_starts if s >= page_end),
                default=chapter["section"]["end"],
            )
            guards.append({"start": start, "end": end, "source_page": page})
        if guards is None or len(anchors) < len(rows) * 0.85:
            report["diagnostics"].append(
                f"contents_tiers_alignment_incomplete: {major['title']}: {unmatched}"
            )
            continue
        if (
            sum(
                anchors.get(n["id"]) == 3
                and not any(
                    g["start"] <= n["heading"]["start"] < g["end"] for g in guards
                )
                for n in children
            )
            < 3
        ):
            continue
        active = 3
        for node in children:
            if any(g["start"] <= node["heading"]["start"] < g["end"] for g in guards):
                node["level"] = original_levels[node["id"]]
                active = node["level"]
                continue
            if node["id"] in anchors:
                active = anchors[node["id"]]
                node["level"] = active
            else:
                node["level"] = min(6, active + 1)
        recovered.append(
            {
                "chapter": major["title"],
                "method": method,
                "matched_entries": len(anchors),
                "entries": len(rows),
                "unmatched": [key for key, _ in unmatched],
                "unverified_regions": guards,
            }
        )
        if guards:
            report["diagnostics"].append(
                f"contents_unverified_regions_retained: {major['title']}: {len(guards)}"
            )
    report["topic_hierarchy"]["method"] = "bounded-contents-v2"
    if recovered:
        report["diagnostics"] = [
            d
            for d in report["diagnostics"]
            if d != "topic_tiers_unverified; retained_ocr_subheadings"
        ]
        diagnostic = "unlisted_topic_headings_inherit_context; review_required"
        if diagnostic not in report["diagnostics"]:
            report["diagnostics"].append(diagnostic)
    if nodes:
        rebuild_sections(nodes, max(n["section"]["end"] for n in nodes))
    return outline
