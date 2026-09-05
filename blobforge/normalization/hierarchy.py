"""Evidence-backed alternative book outlines; primary Markdown stays canonical.

This is an opt-in recipe, not a replacement for the frozen ATX projection.
Numbered chapter openers take precedence. Otherwise a major section must be
both a large, provider-typed title and present in the retained contents pages.
"""

from __future__ import annotations

import bisect
import re
import statistics
from collections import Counter
from typing import Any, Mapping, Sequence

from ..mdaf.builder import markdown_outline

CHAPTER = re.compile(r"^chapter\s+(?:[0-9]+|[ivxlcdm]+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\b", re.I)


def _key(value: str) -> str:
    return " ".join(re.findall(r"[^\W_]+", value.casefold()))


def _title(value: str) -> str:
    return re.sub(r"[*_`#]", "", value).strip()


def _toc_entries(pages: Sequence[Mapping[str, Any]]) -> dict[str, set[str]]:
    entries: dict[str, set[str]] = {}
    for page in pages:
        pending = ""
        for line in page["markdown"].splitlines():
            value = _title(line).strip(" |")
            if not value:
                continue
            cells = [_title(cell).strip() for cell in line.strip().strip("|").split("|")]
            if len(cells) > 1:
                for title, label in zip(cells, cells[1:]):
                    if title and re.fullmatch(r"[0-9]+", label):
                        entries.setdefault(_key(title), set()).add(str(int(label)))
                pending = ""
                continue
            match = re.fullmatch(r"(.*?)[\s.]*([0-9]+)", value)
            if match:
                title, label = match.groups()
                if title.strip(" ."):
                    entries.setdefault(_key(title), set()).add(str(int(label)))
                    if pending:
                        entries.setdefault(_key(pending + " " + title), set()).add(str(int(label)))
                elif pending:
                    entries.setdefault(_key(pending), set()).add(str(int(label)))
                pending = ""
            else:
                pending = value
    return entries


def page_labels(pages: Sequence[Mapping[str, Any]]) -> dict[int, str]:
    """Publish observed, unambiguous Arabic footer labels, never an offset guess."""
    observed = {}
    for page in pages:
        labels = set()
        for block in page.get("blocks", []):
            if block.get("type") != "footer":
                continue
            value = block.get("content", "").strip()
            if re.fullmatch(r"[0-9]+", value):
                labels.add(str(int(value)))
        if len(labels) == 1:
            observed[page["index"]] = labels.pop()
    counts = Counter(observed.values())
    return {page: label for page, label in observed.items() if counts[label] == 1}


def page_references(text: str, labels: Mapping[int, str], source_id: str) -> list[dict[str, Any]]:
    """Only standalone parenthetical single-page citations with observed labels.

    Ranges, comma-qualified citations to other books, and missing/duplicate
    labels remain unbound. Consumers must still check syntactic placement and
    whether a page belongs to exactly one imported note.
    """
    pages = {label: page for page, label in labels.items()}
    references = []
    pattern = re.compile(r"\((?:see\s+)?(?:p\.|page)\s*([0-9]+)\)", re.I)
    for match in pattern.finditer(text):
        page = pages.get(str(int(match[1])))
        if page is None:
            continue
        references.append({
            "document": {"start": len(text[:match.start()].encode()), "end": len(text[:match.end()].encode())},
            "target": {"source_id": source_id, "selectors": [{"type": "interval", "unit": "page", "start": page, "end": page + 1}]},
            "kind": "page-citation",
        })
    return references


def book_outline(
    text: str,
    pages: Sequence[Mapping[str, Any]],
    source_map: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build level-two major sections and nested topics from retained evidence.

    Ambiguous title matches never become boundaries. With insufficient evidence
    return the existing outline and an explicit diagnostic instead of guessing.
    """
    outline = markdown_outline(text, skip_fences=True)
    encoded = text.encode()
    nodes = outline["nodes"]
    mappings = list(source_map["mappings"])
    starts = [m["document"]["start"] for m in mappings]
    by_page = {int(m["source"]["selectors"][0]["start"]): m for m in mappings}
    toc_pages = []
    for page in pages:
        if any(
            block.get("type") in {"title", "header", "footer"}
            and _key(block.get("content", "")) in {"contents", "table of contents"}
            for block in page.get("blocks", [])
        ):
            toc_pages.append(page["index"])
    report: dict[str, Any] = {
        "method": "dev.tionis.blobforge/book-outline-v1",
        "toc_pages": toc_pages,
        "major_sections": [],
        "diagnostics": [],
    }
    if not nodes or not toc_pages:
        report["diagnostics"].append("no_contents_evidence; retained_markdown_outline")
        return outline, report
    # Contents may alternate recto/verso labels. Include the intervening pages,
    # but never join distant, unrelated contents sections into one giant TOC.
    first = min(toc_pages)
    last = first
    for page in sorted(toc_pages)[1:]:
        if page - last > 2:
            break
        last = page
    entries = _toc_entries(pages[first:last + 1])
    labels = page_labels(pages)
    heights = []
    titles: dict[tuple[int, str], list[Mapping[str, Any]]] = {}
    for page in pages:
        height = page.get("dimensions", {}).get("height", 0)
        for block in page.get("blocks", []):
            if block.get("type") != "title" or not height:
                continue
            content = block.get("content", "")
            size = (block.get("bottom_right_y", 0) - block.get("top_left_y", 0)) / height
            size /= max(1, len(content.splitlines()))
            heights.append(size)
            glyph_width = (block.get("bottom_right_x", 0) - block.get("top_left_x", 0)) / max(1, len(_title(content))) / max(1, page.get("dimensions", {}).get("width", 0))
            titles.setdefault((page["index"], _key(content)), []).append({"size": size, "glyph_width": glyph_width, "content": content})
    threshold = min(0.08, max(0.05, statistics.median(heights) * 3)) if heights else 1
    node_pages = {}
    numbered = []
    large = []
    body_counts = Counter()
    for index, node in enumerate(nodes):
        pos = bisect.bisect_right(starts, node["heading"]["start"]) - 1
        if pos < 0 or node["heading"]["start"] >= mappings[pos]["document"]["end"]:
            continue
        page = int(mappings[pos]["source"]["selectors"][0]["start"])
        node_pages[index] = page
        if page <= last:
            continue
        key = _key(node["title"])
        body_counts[key] += 1
        evidence = titles.get((page, key), [])
        if len(evidence) != 1:
            continue
        if CHAPTER.match(key) and evidence[0]["size"] >= 0.03:
            numbered.append(index)
        elif node["level"] == 1 and evidence[0]["size"] >= threshold and evidence[0]["glyph_width"] >= 0.012 and key in entries:
            large.append(index)
    numbered = [i for i in numbered if body_counts[_key(nodes[i]["title"])] == 1]
    use_numbered = len(numbered) >= 2
    large_counts = Counter(_key(nodes[i]["title"]) for i in large)
    matched = [i for i in large if labels.get(node_pages[i]) in entries[_key(nodes[i]["title"])]]
    matched_counts = Counter(_key(nodes[i]["title"]) for i in matched)
    major = numbered if use_numbered else [
        i for i in large
        if (i in matched and matched_counts[_key(nodes[i]["title"]) ] == 1)
        or large_counts[_key(nodes[i]["title"])] == 1
    ]
    if len(major) < 2:
        report["diagnostics"].append("insufficient_unique_major_sections; retained_markdown_outline")
        return outline, report
    if use_numbered:
        # An introduction before numbered chapters is independently recognizable.
        intros = [i for i, node in enumerate(nodes) if i < major[0]
                  and node_pages.get(i, 0) > last and _key(node["title"]) == "introduction"
                  and node["level"] == 1]
        if len(intros) == 1 and any(_key(b.get("content", "")) == "introduction"
                                   for p in pages[first:last + 1] for b in p.get("blocks", [])
                                   if b.get("type") == "title"):
            major = intros + major
    chapter_titles = {}
    absorbed = set()
    for i in major:
        title = nodes[i]["title"]
        if use_numbered and i in numbered:
            # A chapter number and its title often occupy separate ATX blocks
            # on the same opener page. Keep their bytes but name one section.
            pieces = [title]
            for j in range(i + 1, min(i + 4, len(nodes))):
                if node_pages.get(j) != node_pages.get(i) or nodes[j]["level"] != 1:
                    break
                if encoded[nodes[j - 1]["heading"]["end"]:nodes[j]["heading"]["start"]].strip():
                    break
                pieces.append(nodes[j]["title"])
                absorbed.add(j)
            title = ": ".join([pieces[0], " ".join(pieces[1:])]) if len(pieces) > 1 else title
        chapter_titles[i] = title
    output = []
    if nodes[major[0]]["heading"]["start"] > 0:
        output.append({
            "id": "front-matter", "parent": None, "level": 2,
            "title": "Front matter", "heading": {"start": 0, "end": len(text.splitlines(keepends=True)[0].encode())},
            "section": {"start": 0, "end": nodes[major[0]]["heading"]["start"]},
        })
    current = None
    for i, node in enumerate(nodes):
        if i < major[0] or i in absorbed:
            continue
        value = dict(node)
        if i in chapter_titles:
            current = i
            value["title"] = chapter_titles[i]
            value["level"] = 2
            report["major_sections"].append({"title": value["title"], "source_page": node_pages[i], "byte_offset": node["heading"]["start"]})
        else:
            value["level"] = min(6, 3 + max(0, node["level"] - nodes[current]["level"] - 1))
        mapping = by_page.get(node_pages.get(i))
        if mapping:
            value["source"] = mapping["source"]
        output.append(value)
    # Recompute the forest and exact section ends in one stack pass.
    stack = []
    for value in output:
        start = value["heading"]["start"]
        while stack and stack[-1]["level"] >= value["level"]:
            stack.pop()["section"]["end"] = start
        value["parent"] = stack[-1]["id"] if stack else None
        value["section"] = {"start": start, "end": len(encoded)}
        stack.append(value)
    outline["nodes"] = output
    report["strategy"] = "numbered-chapters" if use_numbered else "contents-and-title-geometry"
    return outline, report
