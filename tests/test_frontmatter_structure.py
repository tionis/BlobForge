"""Front-matter boundaries are evidence-scoped and size-bounded."""

import copy

from blobforge.markdown_outline import markdown_outline
from blobforge.normalization.frontmatter_structure import (
    recover_document_landmarks,
    recover_frontmatter_boundaries,
)


def fixture(padding=55_000):
    text = (
        "# Cover\n"
        + "x" * padding
        + "\n# TABLE OF CONTENTS\n\nOPENING STORY\n\nINTRODUCTION 10\n"
        + "\n# Credits\nNames\n"
        + "\n# INTRO TOPIC\nIntro\n"
        + "\n# OPENING\n# STORY\nFiction\n"
        + "\n# Chapter One\nRules\n"
    )
    outline = markdown_outline(text)
    starts = [
        0,
        text.index("# TABLE"),
        text.index("# Credits"),
        text.index("# INTRO"),
        text.index("# OPENING"),
        text.index("# Chapter"),
    ]
    mappings = []
    for page, start in enumerate(starts):
        end = starts[page + 1] if page + 1 < len(starts) else len(text.encode())
        mappings.append(
            {
                "document": {"start": start, "end": end},
                "source": {"selectors": [{"start": page}]},
            }
        )
    pages = [
        {
            "index": 0,
            "markdown": "# Cover",
            "dimensions": {"width": 1000, "height": 1000},
            "blocks": [],
        },
        {
            "index": 1,
            "markdown": "# TABLE OF CONTENTS\n\nOPENING STORY\n\nINTRODUCTION 10",
            "dimensions": {"width": 1000, "height": 1000},
            "blocks": [
                {"type": "title", "content": "# TABLE OF CONTENTS"},
                {"type": "text", "content": "OPENING\nSTORY"},
            ],
        },
        {
            "index": 2,
            "markdown": "# Credits",
            "dimensions": {"width": 1000, "height": 1000},
            "blocks": [
                {
                    "type": "title",
                    "content": "# Credits",
                    "top_left_x": 10,
                    "top_left_y": 10,
                    "bottom_right_x": 400,
                    "bottom_right_y": 80,
                }
            ],
        },
        {
            "index": 3,
            "markdown": "# INTRO TOPIC",
            "dimensions": {"width": 1000, "height": 1000},
            "blocks": [
                {"type": "header", "content": "INTRODUCTION"},
                {
                    "type": "title",
                    "content": "# INTRO TOPIC",
                    "top_left_x": 10,
                    "top_left_y": 10,
                    "bottom_right_x": 400,
                    "bottom_right_y": 80,
                },
            ],
        },
        {
            "index": 4,
            "markdown": "# OPENING\n# STORY",
            "dimensions": {"width": 1000, "height": 1000},
            "blocks": [
                {
                    "type": "title",
                    "content": "# OPENING\n# STORY",
                    "top_left_x": 10,
                    "top_left_y": 10,
                    "bottom_right_x": 400,
                    "bottom_right_y": 100,
                }
            ],
        },
        {
            "index": 5,
            "markdown": "# Chapter One",
            "dimensions": {"width": 1000, "height": 1000},
            "blocks": [
                {
                    "type": "title",
                    "content": "# Chapter One",
                    "top_left_x": 10,
                    "top_left_y": 10,
                    "bottom_right_x": 400,
                    "bottom_right_y": 80,
                }
            ],
        },
    ]
    report = {
        "toc_pages": [1],
        "major_sections": [
            {
                "title": "Chapter One",
                "source_page": 5,
                "byte_offset": starts[5],
                "heading_end": starts[5] + len("# Chapter One"),
                "evidence": "title-match",
            }
        ],
        "diagnostics": [],
    }
    return text, pages, {"mappings": mappings}, outline, report


def test_large_frontmatter_splits_at_corroborated_landmarks_and_hides_toc_rows():
    args = fixture()
    result = recover_frontmatter_boundaries(*args)
    majors = args[4]["frontmatter_recovered_boundaries"]
    assert [major["title"] for major in majors] == [
        "TABLE OF CONTENTS",
        "Credits",
        "INTRODUCTION",
        "OPENING STORY",
    ]
    nodes = result["nodes"]
    contents = next(node for node in nodes if node["title"] == "TABLE OF CONTENTS")
    assert not [node for node in nodes if node["parent"] == contents["id"]]
    assert (
        "frontmatter_landmarks_recovered_from_retained_evidence; review_required"
        in args[4]["diagnostics"]
    )


def test_small_frontmatter_is_unchanged():
    args = fixture(padding=100)
    before = copy.deepcopy(args[3])
    assert recover_frontmatter_boundaries(*args) == before
    assert "frontmatter_recovered_boundaries" not in args[4]


def test_alphabetical_backmatter_becomes_a_major_and_collapses_duplicate_letters():
    parts = [
        "# CONTENTS\nChapter One 2",
        "# Chapter One\nRules",
        "# Reference Finder\n# A\nAlpha\n# B\nBeta\n# C\nGamma",
        "# D\nDelta\n# D\ncontinued\n# F\nFoxtrot",
    ]
    text = "\n\n".join(parts)
    starts = []
    cursor = 0
    for part in parts:
        starts.append(cursor)
        cursor += len(part.encode()) + 2
    mappings = [
        {
            "document": {
                "start": start,
                "end": starts[index + 1] - 2
                if index + 1 < len(starts)
                else len(text.encode()),
            },
            "source": {"selectors": [{"start": index}]},
        }
        for index, start in enumerate(starts)
    ]
    pages = [
        {
            "index": index,
            "markdown": part,
            "dimensions": {"width": 1000, "height": 1000},
            "blocks": [],
        }
        for index, part in enumerate(parts)
    ]
    report = {
        "toc_pages": [0],
        "major_sections": [
            {
                "title": "Chapter One",
                "source_page": 1,
                "byte_offset": starts[1],
                "heading_end": starts[1] + len("# Chapter One"),
                "evidence": "title-match",
            }
        ],
        "diagnostics": [],
    }
    outline = recover_document_landmarks(
        text, pages, {"mappings": mappings}, markdown_outline(text), report
    )
    recovered = report["backmatter_recovered_boundaries"]
    assert [item["title"] for item in recovered] == ["Reference Finder"]
    reference = next(
        node for node in outline["nodes"] if node["title"] == "Reference Finder"
    )
    children = [
        node["title"] for node in outline["nodes"] if node["parent"] == reference["id"]
    ]
    assert children == ["A", "B", "C", "D", "F"]
