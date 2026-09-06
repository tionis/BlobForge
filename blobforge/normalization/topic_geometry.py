"""Document-relative heading typography; no publication vocabulary."""

from __future__ import annotations

import math
from collections import defaultdict

from .hierarchy import _key, _title


def heading_scores(nodes, pages, mappings):
    """Area per character tolerates visually wrapped headings better than height.

    Only uniquely matching provider-typed title blocks on the mapped page count.
    This is layout evidence, not a font-size assertion or semantic guarantee.
    """
    by_page = defaultdict(list)
    for page in pages:
        height = page.get("dimensions", {}).get("height", 0)
        width = page.get("dimensions", {}).get("width", 0)
        if not height or not width:
            continue
        for block in page.get("blocks", []):
            if block.get("type") != "title":
                continue
            title = _title(block.get("content", ""))
            length = len("".join(title.split()))
            h = block.get("bottom_right_y", 0) - block.get("top_left_y", 0)
            w = block.get("bottom_right_x", 0) - block.get("top_left_x", 0)
            if length >= 3 and h > 0 and w > 0:
                by_page[page["index"]].append(
                    (_key(title), math.sqrt(h * w / (height * width * length)))
                )
    scores = {}
    cursor = 0
    for node in nodes:
        start = node["heading"]["start"]
        while (
            cursor + 1 < len(mappings)
            and mappings[cursor + 1]["document"]["start"] <= start
        ):
            cursor += 1
        if (
            not mappings
            or not mappings[cursor]["document"]["start"]
            <= start
            < mappings[cursor]["document"]["end"]
        ):
            continue
        page = mappings[cursor]["source"]["selectors"][0]["start"]
        candidates = [
            score for key, score in by_page[page] if key == _key(node["title"])
        ]
        if len(candidates) == 1:
            scores[node["id"]] = candidates[0]
    return scores
