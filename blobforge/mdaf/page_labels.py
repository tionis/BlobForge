"""A PDF's own page labels as MDAF v1 ``page_labels`` ranges.

MDAF's page label ranges are the model of PDF page labels (Vulcan
``docs/specs/mdaf/v1/SPEC.md``, normalized source map): each range starts at a
zero-based page and applies until the next, with a numbering style, an optional
prefix, and an optional first number. A PDF's ``/PageLabels`` number tree maps
onto them one to one, so labels read here are the source's own evidence and may
be published as they are.

Nothing is inferred: a PDF without ``/PageLabels`` (or with an unreadable one)
has no labels here. A presumed offset must not be published as labels.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pypdf import PdfReader
from pypdf.generic import ArrayObject, DictionaryObject

# PDF numbering styles (ISO 32000-1, 12.4.2) and their MDAF names.
_STYLES = {
    "/D": "decimal",
    "/R": "roman-upper",
    "/r": "roman-lower",
    "/A": "alpha-upper",
    "/a": "alpha-lower",
}


def _number_tree(
    node: DictionaryObject, out: list[tuple[int, DictionaryObject]], depth: int = 0
) -> None:
    """Collects a number tree's (key, value) pairs, leaves and intermediate nodes alike."""
    if depth > 32:
        raise ValueError("page label tree too deep")
    nums = node.get("/Nums")
    if isinstance(nums, ArrayObject):
        for index in range(0, len(nums) - 1, 2):
            key = nums[index]
            value = nums[index + 1].get_object()
            if isinstance(value, DictionaryObject):
                out.append((int(key), value))
    kids = node.get("/Kids")
    if isinstance(kids, ArrayObject):
        for kid in kids:
            child = kid.get_object()
            if isinstance(child, DictionaryObject):
                _number_tree(child, out, depth + 1)


def page_label_ranges(path: str | Path) -> list[dict[str, Any]] | None:
    """The PDF's page label ranges as MDAF ``page_labels`` entries, or ``None``.

    ``None`` when the PDF has no page labels or they cannot be read. Ranges are
    ordered by start; a range's ``first`` is omitted when it is 1, and a range
    without a numbering style has ``style`` ``none`` (only its prefix).
    """
    try:
        reader = PdfReader(str(path))
        root = reader.trailer["/Root"].get_object()
        tree = root.get("/PageLabels")
        if tree is None:
            return None
        pairs: list[tuple[int, DictionaryObject]] = []
        _number_tree(tree.get_object(), pairs)
        pages = len(reader.pages)
    except Exception:  # noqa: BLE001 - an unreadable label tree is no evidence
        return None
    ranges: list[dict[str, Any]] = []
    for start, label in sorted(pairs, key=lambda pair: pair[0]):
        if start < 0 or start >= pages or (ranges and ranges[-1]["start"] == start):
            continue
        entry: dict[str, Any] = {
            "start": start,
            "style": _STYLES.get(str(label.get("/S")), "none"),
        }
        prefix = label.get("/P")
        if prefix is not None and str(prefix):
            entry["prefix"] = str(prefix)
        first = label.get("/St")
        if first is not None and int(first) >= 1 and int(first) != 1:
            entry["first"] = int(first)
        ranges.append(entry)
    return ranges or None
