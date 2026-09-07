"""Wrapped title recovery requires body page and native layout agreement."""

import copy

from blobforge.markdown_outline import markdown_outline
from blobforge.normalization.contents_structure_v3 import (
    _corroborated_joined_pages,
    _join_split_body_titles,
    _joined_block_candidates,
    _replace_lines,
)


def fixture():
    text = (
        "# Manual\n\n# STEP TWO:\n# CHOOSE MATERIAL\n\nBody.\n\n"
        "# STEP THREE: ASSEMBLE PARTS\n\nBody.\n"
    )
    outline = markdown_outline(text)
    for node in outline["nodes"]:
        node["level"] = 2 if node["title"] == "Manual" else 3
    pages = [
        {
            "index": 0,
            "markdown": "STEP THREE: ASSEMBLE\n\nPARTS 3\n\nSTEP TWO: CHOOSE MATERIAL 2",
            "dimensions": {"width": 1000, "height": 1000},
            "blocks": [
                {
                    "type": "text",
                    "content": "STEP THREE: ASSEMBLE",
                    "top_left_x": 100,
                    "top_left_y": 100,
                    "bottom_right_x": 400,
                    "bottom_right_y": 110,
                },
                {
                    "type": "text",
                    "content": "PARTS 3",
                    "top_left_x": 106,
                    "top_left_y": 111,
                    "bottom_right_x": 400,
                    "bottom_right_y": 121,
                },
            ],
        }
    ]
    starts = [node["heading"]["start"] for node in outline["nodes"]]
    source_map = {
        "mappings": [
            {
                "document": {
                    "start": starts[0],
                    "end": starts[1],
                },
                "source": {"selectors": [{"start": 1}]},
            },
            {
                "document": {"start": starts[1], "end": starts[3]},
                "source": {"selectors": [{"start": 2}]},
            },
            {
                "document": {"start": starts[3], "end": len(text.encode())},
                "source": {"selectors": [{"start": 3}]},
            },
        ]
    }
    report = {
        "toc_pages": [0],
        "alignment_offset": 0,
        "major_sections": [{"title": "Manual", "source_page": 1}],
    }
    return text, outline, pages, source_map, report


def test_adjacent_native_rows_join_only_with_aligned_body_confirmation():
    _, outline, pages, source_map, report = fixture()
    repaired, titles = _corroborated_joined_pages(outline, pages, source_map, report)
    assert titles == ["STEP THREE: ASSEMBLE PARTS"]
    assert "STEP THREE: ASSEMBLE PARTS 3" in repaired[0]["markdown"]
    wrong = copy.deepcopy(source_map)
    wrong["mappings"][2]["source"]["selectors"][0]["start"] = 30
    repaired, titles = _corroborated_joined_pages(outline, pages, wrong, report)
    assert not titles
    assert repaired[0]["markdown"] == pages[0]["markdown"]


def test_same_native_block_is_a_wrapped_candidate():
    page = {
        "dimensions": {"width": 1000, "height": 1000},
        "blocks": [{"type": "text", "content": "CREATE A\nCHARACTER 12"}],
    }
    assert list(_joined_block_candidates(page)) == [["CREATE A", "CHARACTER 12"]]


def test_split_body_headings_join_only_on_corroborated_page():
    text, outline, pages, source_map, report = fixture()
    repaired = _join_split_body_titles(text, outline, pages, source_map, report)
    assert repaired == ["STEP TWO: CHOOSE MATERIAL"]
    assert outline["nodes"][1]["title"] == "STEP TWO: CHOOSE MATERIAL"
    _, outline, pages, source_map, report = fixture()
    report["alignment_offset"] = 10
    assert not _join_split_body_titles(text, outline, pages, source_map, report)
    assert outline["nodes"][1]["title"] == "STEP TWO:"


def test_loose_or_misaligned_blocks_are_not_joined():
    page = {
        "dimensions": {"width": 1000, "height": 1000},
        "blocks": [
            {
                "type": "text",
                "content": "SECTION",
                "top_left_x": 100,
                "top_left_y": 100,
                "bottom_right_x": 300,
                "bottom_right_y": 110,
            },
            {
                "type": "text",
                "content": "FIRST TOPIC 4",
                "top_left_x": 200,
                "top_left_y": 130,
                "bottom_right_x": 500,
                "bottom_right_y": 140,
            },
        ],
    }
    assert not list(_joined_block_candidates(page))


def test_duplicate_markdown_rows_are_not_arbitrarily_replaced():
    markdown = "CREATE A\n\nCHARACTER 12\n\nCREATE A\n\nCHARACTER 12"
    assert _replace_lines(markdown, ["CREATE A", "CHARACTER 12"]) == markdown
