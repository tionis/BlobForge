"""TOC-led book structure recovery, independent of publication names.

This module is versioned separately from the original geometry-led heuristic.
It never changes primary Markdown or invents printed labels for citations.
"""

from __future__ import annotations

import bisect
import re
import statistics
from collections import Counter

from ..markdown_outline import markdown_outline
from .hierarchy import _key, _title, _toc_entries, page_labels

SERIES = re.compile(
    r"^(?:chapter|kapitel|chapitre|capítulo|capitolo|appendix|anhang|annexe)\s+(?:\d+|[ivxlcdm]+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\b",
    re.IGNORECASE,
)
FRONT = {"introduction", "foreword", "preface", "einleitung", "vorwort"}
CONTENTS = {
    "contents",
    "table of contents",
    "contents credits",
    "inhalt",
    "inhaltsverzeichnis",
    "sommaire",
    "table des matières",
    "índice",
}


def _entry(value):
    clean = _title(value).strip(" |")
    match = re.fullmatch(r"(.+?)[\s.→]+([0-9]+)", clean, re.DOTALL)
    return (
        (" ".join(match[1].split()), str(int(match[2])))
        if match
        else (" ".join(clean.split()), None)
    )


def contents_pages(pages):
    """Locate the first early contents cluster using labels OR entry density."""
    candidates = []
    for page in pages[: max(12, min(40, len(pages) // 5))]:
        entries = _toc_entries([page])
        labels = {label for values in entries.values() for label in values}
        lines = [line for line in page["markdown"].splitlines() if line.strip()]
        named = any(
            _key(b.get("content", "")) in CONTENTS
            for b in page.get("blocks", [])
            if b.get("type") in {"title", "header", "footer"}
        )
        dense = (
            len(entries) >= 12
            and len(labels) >= 6
            and len(entries) / max(1, len(lines)) >= 0.3
        )
        if (named and len(entries) >= 3) or dense:
            candidates.append(page["index"])
    if not candidates:
        return []
    cluster = [candidates[0]]
    for index in candidates[1:]:
        if index - cluster[-1] > 2:
            break
        cluster.append(index)
    return list(range(cluster[0], cluster[-1] + 1))


def _entries(pages):
    scoped = []
    for page in pages:
        lines = page["markdown"].splitlines()
        for i, line in enumerate(lines):
            match = re.match(r"^(#{1,6})\s+(.*)", line)
            if match and _key(match[2]) in CONTENTS:
                stop = next(
                    (
                        j
                        for j in range(i + 1, len(lines))
                        if re.match(r"^#{1," + str(len(match[1])) + r"}\s", lines[j])
                    ),
                    len(lines),
                )
                lines = lines[i:stop]
                break
        scoped.append({**page, "markdown": "\n".join(lines)})
    pages = scoped
    entries = {
        key: {"title": key, "labels": labels, "heading": None}
        for key, labels in _toc_entries(pages).items()
        if key and "img " not in key
    }
    # Preserve cell-local multiline titles; joining complete rows mixes columns.
    for page in pages:
        pending_columns = {}
        previous_line = ""
        for line in page["markdown"].splitlines():
            cells = line.strip().strip("|").split("|") if "|" in line else [line]
            if len(cells) > 1:
                for column in range(0, len(cells) - 1, 2):
                    title = _title(cells[column]).strip()
                    label = _title(cells[column + 1]).strip()
                    previous = pending_columns.pop(column, None)
                    if previous and title and not label:
                        combined = previous[0] + " " + title
                        key = _key(combined)
                        entries[key] = {
                            "title": combined,
                            "labels": {previous[1]},
                            "heading": None,
                        }
                        entries.pop(_key(previous[0]), None)
                    if title.endswith(":") and label.isdecimal():
                        pending_columns[column] = (title, str(int(label)))
            else:
                title, label = _entry(line)
                combined = _title(previous_line).strip() + " " + title
                if label and _key(combined) in entries:
                    entries[_key(combined)]["title"] = combined
                if line.strip():
                    previous_line = line
            for cell in cells:
                title, label = _entry(cell)
                key = _key(title)
                if key in entries:
                    entries[key]["title"] = title
        for block in page.get("blocks", []):
            if block.get("type") != "title":
                continue
            value = block.get("content", "")
            title, label = _entry(value)
            key = _key(title)
            if not key or key in CONTENTS or key.isnumeric():
                continue
            entry = entries.setdefault(
                key, {"title": title, "labels": set(), "heading": None}
            )
            entry["title"] = title
            if label:
                if entry["heading"] is None:
                    entry["labels"] = {label}
                else:
                    entry["labels"].add(label)
            match = re.match(r"^(#{1,6})\s", value)
            if match:
                entry["heading"] = len(match[1])
    return entries


def _body_candidates(nodes, pages, mappings, text):
    encoded = text.encode()
    starts = [m["document"]["start"] for m in mappings]
    by_page = {}
    for node in nodes:
        pos = bisect.bisect_right(starts, node["heading"]["start"]) - 1
        if pos < 0:
            continue
        page = int(mappings[pos]["source"]["selectors"][0]["start"])
        by_page.setdefault(page, []).append(node)
    candidates = {}
    for page in pages:
        for node in by_page.get(page["index"], []):
            key = _key(node["title"])
            value = {
                "node": node,
                "page": page["index"],
                "size": 0.0,
                "width": 0.0,
                "title": node["title"],
            }
            candidates.setdefault(key, []).append(value)
            peers = by_page[page["index"]]
            pos = peers.index(node)
            combined = node["title"]
            for following in peers[pos + 1 : pos + 4]:
                previous = peers[peers.index(following) - 1]
                if (
                    following["level"] != node["level"]
                    or encoded[
                        previous["heading"]["end"] : following["heading"]["start"]
                    ].strip()
                ):
                    break
                combined += " " + following["title"]
                candidates.setdefault(_key(combined), []).append(
                    {**value, "title": combined}
                )
            for block in page.get("blocks", []):
                if block.get("type") != "title":
                    continue
                block_key = _key(block.get("content", ""))
                if block_key != key and not block_key.startswith(key + " "):
                    continue
                if block_key != key and "\n" not in block.get("content", ""):
                    continue
                height = page.get("dimensions", {}).get("height", 0)
                if not height:
                    continue
                size = (
                    (block.get("bottom_right_y", 0) - block.get("top_left_y", 0))
                    / height
                    / max(1, len(block["content"].splitlines()))
                )
                if size > value["size"]:
                    value["size"] = size
                    value["width"] = (
                        (block.get("bottom_right_x", 0) - block.get("top_left_x", 0))
                        / max(1, page["dimensions"].get("width", 0))
                        / max(1, len(_title(block["content"])))
                    )
                if block_key != key:
                    alias = {
                        **value,
                        "title": " ".join(_title(block["content"]).split()),
                    }
                    candidates.setdefault(block_key, []).append(alias)
    return candidates


def recover_book_structure(text, pages, source_map, *, geometry_ratio=0.65):
    outline = markdown_outline(text, skip_fences=True)
    nodes = outline["nodes"]
    toc = contents_pages(pages)
    report = {
        "method": "dev.tionis.blobforge/toc-led-book-outline-v2",
        "toc_pages": toc,
        "major_sections": [],
        "diagnostics": [],
        "unmatched_entries": [],
    }
    if not toc or not nodes:
        report["diagnostics"].append("no_contents_evidence; retained_markdown_outline")
        return outline, report
    entries = _entries([page for page in pages if page["index"] in toc])
    mappings = source_map["mappings"]
    by_page = {int(m["source"]["selectors"][0]["start"]): m for m in mappings}
    candidates = _body_candidates(nodes, pages, mappings, text)
    candidates = {
        key: [c for c in values if c["page"] > toc[-1]]
        for key, values in candidates.items()
    }
    labels = page_labels(pages)
    label_pages = {label: page for page, label in labels.items()}
    # Offset is alignment evidence only, never a fabricated observed label.
    offsets = Counter()
    for key, entry in entries.items():
        values = candidates.get(key, [])
        if len(values) == 1 and len(entry["labels"]) == 1:
            offsets[values[0]["page"] - int(next(iter(entry["labels"])))] += 1
    offset = None
    if offsets:
        best, count = offsets.most_common(1)[0]
        if count >= 3 and count / offsets.total() >= 0.75:
            offset = best
    report["alignment_offset"] = offset
    report["alignment_votes"] = dict(sorted(offsets.items()))
    matched = {}
    for key, entry in entries.items():
        values = candidates.get(key, [])
        target_pages = {
            label_pages[label] for label in entry["labels"] if label in label_pages
        }
        if offset is not None:
            target_pages.update(
                int(label) + offset
                for label in entry["labels"]
                if label not in label_pages
            )
        aligned = [v for v in values if v["page"] in target_pages]
        if len(aligned) == 1:
            matched[key] = aligned[0]
        else:
            ranked = sorted(values, key=lambda v: v["size"], reverse=True)
            if (
                len(ranked) == 1
                or len(ranked) > 1
                and ranked[0]["size"] > ranked[1]["size"] * 1.5
            ):
                matched[key] = ranked[0]
    numbered = [key for key in entries if SERIES.match(key)]
    chapters = [
        key for key in numbered if not re.match(r"^(appendix|anhang|annexe)\b", key)
    ]
    explicit = [
        key for key, e in entries.items() if e["heading"] is not None and e["labels"]
    ]
    if explicit:
        level = min(entries[key]["heading"] for key in explicit)
        explicit = [key for key in explicit if entries[key]["heading"] == level]
    all_labels = [int(label) for e in entries.values() for label in e["labels"]]
    explicit_labels = [
        int(label) for key in explicit for label in entries[key]["labels"]
    ]
    coherent_explicit = (
        len(explicit) >= 3
        and explicit_labels
        and all_labels
        and min(explicit_labels) <= min(all_labels) + 10
        and max(explicit_labels) - min(explicit_labels)
        >= 0.5 * (max(all_labels) - min(all_labels))
    )
    labelled_keys = [key for key, entry in entries.items() if entry["labels"]]
    if len(labelled_keys) <= 10 and len(all_labels) >= 3:
        selected = labelled_keys
        report["strategy"] = "compact-flat-toc"
    elif len(chapters) >= 3:
        selected = numbered + [key for key in entries if key in FRONT]
        # A flat chapter-only TOC can also list index/maps/other back matter.
        if len(numbered) >= 0.6 * len(entries):
            selected = numbered + list(entries)
        report["strategy"] = "numbered-toc"
    elif coherent_explicit:
        selected = explicit
        report["strategy"] = "explicit-toc-hierarchy"
    else:
        # Typography is a document-relative fallback only when TOC tiers were
        # flattened by extraction. Compare the upper title-size cohort.
        sizes = sorted(
            v["size"]
            for v in matched.values()
            if v["node"]["level"] == 1 and v["size"] > 0
        )
        upper = sizes[-max(3, min(10, len(sizes) // 20)) :]
        threshold = statistics.median(upper) * geometry_ratio if upper else float("inf")
        major_widths = [v["width"] for v in matched.values() if v["size"] >= threshold]
        wide_titles = bool(major_widths) and statistics.median(major_widths) >= 0.03
        selected = [
            key
            for key, v in matched.items()
            if v["node"]["level"] == 1
            and (
                v["size"] >= threshold
                and v["width"] >= 0.012
                or wide_titles
                and v["size"] >= 0.045
                and v["width"] >= 0.03
            )
        ]
        first_major = min(
            (matched[key]["page"] for key in selected), default=len(pages)
        )
        selected += [
            key
            for key in entries
            if key in FRONT and key in matched and matched[key]["page"] < first_major
        ]
        selected += numbered
        report["strategy"] = "toc-and-relative-geometry"
        report["geometry_threshold"] = threshold if upper else None
        report["diagnostics"].append(
            "toc_tiers_unavailable; relative_geometry_requires_review"
        )
    chosen = {}
    for key in dict.fromkeys(selected):
        entry = entries[key]
        value = matched.get(key)
        target_pages = {
            label_pages[label] for label in entry["labels"] if label in label_pages
        }
        if offset is not None:
            target_pages.update(
                int(label) + offset
                for label in entry["labels"]
                if label not in label_pages
            )
        if value and target_pages and value["page"] not in target_pages:
            report["diagnostics"].append(
                f"toc_title_page_disagreement: {entry['title']}"
            )
            if report["strategy"] != "toc-and-relative-geometry":
                value = None
        if value:
            page = value["page"]
            start = value["node"]["heading"]["start"]
            end = value["node"]["heading"]["end"]
            evidence = "title-match"
        else:
            target_pages = {p for p in target_pages if p > toc[-1] and p in by_page}
            if len(target_pages) != 1:
                report["unmatched_entries"].append(entry["title"])
                continue
            page = next(iter(target_pages))
            start = by_page[page]["document"]["start"]
            end = start + len(text.encode()[start:].split(b"\n", 1)[0])
            evidence = "toc-page-alignment"
        # Chapter openers own their leading prose/images, not just their title.
        if entry["labels"] and (
            str(labels.get(page)) in entry["labels"]
            or offset is not None
            and str(page - offset) in entry["labels"]
        ):
            start = by_page[page]["document"]["start"]
            end = max(end, start + 1)
        title = value["title"] if value and entry["title"] == key else entry["title"]
        chosen.setdefault(
            start,
            {
                "title": title,
                "source_page": page,
                "byte_offset": start,
                "heading_end": end,
                "evidence": evidence,
            },
        )
    majors = sorted(chosen.values(), key=lambda m: m["byte_offset"])
    report["major_sections"] = majors
    if report["unmatched_entries"]:
        report["diagnostics"].append(
            "unmatched_top_level_entries; partial_hierarchy_requires_review"
        )
    if len(majors) < 2:
        report["diagnostics"].append(
            "insufficient_major_sections; retained_markdown_outline"
        )
        return outline, report
    encoded = text.encode()
    output = []
    if majors[0]["byte_offset"]:
        output.append(
            {
                "id": "front-matter",
                "level": 2,
                "title": "Front matter",
                "heading": {"start": 0, "end": len(encoded.split(b"\n", 1)[0]) or 1},
            }
        )
    for i, major in enumerate(majors):
        start = major["byte_offset"]
        end = majors[i + 1]["byte_offset"] if i + 1 < len(majors) else len(encoded)
        output.append(
            {
                "id": f"major-{i}",
                "level": 2,
                "title": major["title"],
                "heading": {"start": start, "end": min(major["heading_end"], end)},
            }
        )
        opener_end = by_page[major["source_page"]]["document"]["end"]
        for node in nodes:
            if start < node["heading"]["start"] < end:
                if node["heading"]["start"] < opener_end and _key(
                    node["title"]
                ) in _key(major["title"]):
                    continue
                output.append({**node, "level": min(6, max(3, node["level"] + 1))})
    stack = []
    for node in output:
        start = node["heading"]["start"]
        while stack and stack[-1]["level"] >= node["level"]:
            stack.pop()["section"]["end"] = start
        node["parent"] = stack[-1]["id"] if stack else None
        node["section"] = {"start": start, "end": len(encoded)}
        stack.append(node)
    outline["nodes"] = output
    return outline, report
