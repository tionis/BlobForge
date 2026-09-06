"""Publication-independent manuals exercise corroboration and safe fallback."""

import copy

import pytest

from blobforge.markdown_outline import markdown_outline
from blobforge.normalization.contents_structure import (
    _indent_levels,
    _native_widths,
    _rows,
    _step_prefix,
    recover_contents_topics,
)
from blobforge.normalization.layout_structure import repair_major_boundaries
from blobforge.normalization.topic_geometry import heading_scores


def manual(style="italic"):
    titles = [
        "Tools",
        "Hammer",
        "Saw",
        "Process",
        "Prepare",
        "Finish",
        "Results",
        "Scores",
        "Review",
    ]
    text = "# Workshop\n\n" + "\n\n".join("# " + title for title in titles) + "\n"
    outline = markdown_outline(text)
    outline["nodes"][0]["id"] = "major-0"
    for index, node in enumerate(outline["nodes"]):
        node["level"] = 2 if index == 0 else 3
    outline["nodes"][0]["section"]["end"] = len(text.encode())
    toc = "# Workshop\n"
    for i, title in enumerate(titles):
        if style == "italic":
            title = title if i % 3 == 0 else f"*{title}*"
        elif style == "headings":
            title = "### " + title if i % 3 == 0 else title
        elif style == "caps":
            title = title.upper() if i % 3 == 0 else title
        toc += title + " 1\n\n"
    pages = [{"index": 0, "markdown": toc}]
    source_map = {
        "mappings": [
            {
                "document": {"start": 0, "end": len(text.encode())},
                "source": {"selectors": [{"start": 1}]},
            }
        ]
    }
    report = {
        "major_sections": [{"title": "Workshop", "source_page": 1}],
        "toc_pages": [0],
        "alignment_offset": 0,
        "diagnostics": [],
    }
    return outline, pages, source_map, report


@pytest.mark.parametrize("style", ["italic", "headings", "caps"])
def test_manual_contents_styles_preserve_topic_ownership(style):
    args = manual(style)
    result = recover_contents_topics(*args)
    nodes = {n["title"]: n for n in result["nodes"]}
    for parent, child in [
        ("Tools", "Saw"),
        ("Process", "Prepare"),
        ("Results", "Review"),
    ]:
        assert nodes[child]["parent"] == nodes[parent]["id"]
        assert nodes[parent]["level"] == 3
        assert nodes[child]["level"] == 4
    assert args[3]["topic_hierarchy"]["chapters"][0]["matched_entries"] == 9


@pytest.mark.parametrize("style", ["italic", "headings", "caps"])
def test_missing_top_anchor_does_not_absorb_neighbouring_topics(style):
    args = manual(style)
    args[1][0]["markdown"] = (
        args[1][0]["markdown"].replace("Process", "Absent").replace("PROCESS", "ABSENT")
    )
    before = copy.deepcopy(args[0])
    result = recover_contents_topics(*args)
    assert [(n["title"], n["level"]) for n in result["nodes"]] == [
        (n["title"], n["level"]) for n in before["nodes"]
    ]
    assert not args[3]["topic_hierarchy"]["chapters"]


def test_duplicate_toc_entries_do_not_count_as_missing_anchors():
    args = manual()
    args[1][0]["markdown"] += "Tools 1\n"
    recover_contents_topics(*args)
    assert args[3]["topic_hierarchy"]["chapters"][0]["matched_entries"] == 9


def test_parenthetical_body_parameters_need_corroborated_page():
    args = manual()
    next(n for n in args[0]["nodes"] if n["title"] == "Hammer")["title"] = (
        "Hammer (small)"
    )
    recover_contents_topics(*args)
    assert args[3]["topic_hierarchy"]["chapters"][0]["matched_entries"] == 9
    args = manual()
    next(n for n in args[0]["nodes"] if n["title"] == "Tools")["title"] = (
        "Tools (small)"
    )
    args[3]["alignment_offset"] = 10
    recover_contents_topics(*args)
    assert not args[3]["topic_hierarchy"]["chapters"]


def test_same_title_with_different_page_does_not_borrow_heading_style():
    rows = list(_rows([{"index": 0, "markdown": "# Tools 2\n\nTools 5"}], [0]))
    assert [r["heading"] for r in rows] == [1, None]


def test_column_relative_indentation_is_dpi_independent():
    rows = [{"key": f"item {i}", "label": i + 1} for i in range(12)]

    def page(scale):
        return {
            "index": 0,
            "dimensions": {"width": 1000 * scale},
            "blocks": [
                {
                    "type": "text",
                    "content": f"Item {i} {i + 1}",
                    "top_left_x": (100 if i % 3 == 0 else 120) * scale,
                    "bottom_right_x": 300 * scale,
                }
                for i in range(12)
            ],
        }

    for scale in [1, 2, 0.5]:
        levels = _indent_levels(rows, _native_widths([page(scale)], [0]))
        assert levels == [3 if i % 3 == 0 else 4 for i in range(12)]
    assert _indent_levels(rows, {}) is None


def chapter_fixture(
    body_title="Trouble-shooting", label=4, strategy="toc-and-relative-geometry"
):
    titles = ["Setup", "Operation", "Maintenance", body_title]
    text = "\n\n".join("# " + title for title in titles)
    outline = markdown_outline(text)
    pages = [
        {
            "index": 0,
            "markdown": f"# Contents\n\nSetup 1\n\nOperation 2\n\nMaintenance 3\n\nTroubleshooting {label}",
        }
    ]
    mappings, majors = [], []
    for index, node in enumerate(outline["nodes"]):
        start = node["heading"]["start"]
        end = (
            outline["nodes"][index + 1]["heading"]["start"]
            if index < 3
            else len(text.encode())
        )
        mappings.append(
            {
                "document": {"start": start, "end": end},
                "source": {"selectors": [{"start": index + 1}]},
            }
        )
        pages.append(
            {
                "index": index + 1,
                "markdown": "# " + node["title"],
                "dimensions": {"width": 1000, "height": 1000},
                "blocks": [
                    {
                        "type": "title",
                        "content": "# " + node["title"],
                        "top_left_x": 0,
                        "top_left_y": 0,
                        "bottom_right_x": len(node["title"].replace("-", "")) * 20,
                        "bottom_right_y": 40,
                    }
                ],
            }
        )
        if index < 3:
            majors.append(
                {
                    "title": node["title"],
                    "source_page": index + 1,
                    "byte_offset": start,
                    "heading_end": node["heading"]["end"],
                }
            )
    report = {
        "major_sections": majors,
        "toc_pages": [0],
        "alignment_offset": 0,
        "strategy": strategy,
        "diagnostics": [],
    }
    return text, pages, {"mappings": mappings}, outline, report


def test_chapter_alias_needs_title_page_and_established_geometry():
    args = chapter_fixture()
    repair_major_boundaries(*args)
    assert args[-1]["layout_recovered_majors"] == ["Troubleshooting"]
    nodes = args[-2]["nodes"]
    assert all(n["parent"] is None for n in nodes)
    assert nodes[-1]["section"]["end"] == len(args[0].encode())


@pytest.mark.parametrize(
    "kwargs",
    [
        {"body_title": "Troubleshooting"},
        {"body_title": "Unrelated discussion"},
        {"label": 10},
        {"strategy": "numbered-toc"},
    ],
)
def test_no_geometry_only_or_uncorroborated_chapter_promotion(kwargs):
    args = chapter_fixture(**kwargs)
    before = copy.deepcopy(args[-2])
    assert repair_major_boundaries(*args) == before
    assert "layout_recovered_majors" not in args[-1]


def test_small_caption_and_ambiguous_layout_do_not_become_chapters():
    args = chapter_fixture()
    args[1][-1]["blocks"][0]["bottom_right_y"] = 4
    assert repair_major_boundaries(*args) == args[-2]
    assert "layout_recovered_majors" not in args[-1]
    args = chapter_fixture()
    args[1][-1]["blocks"] *= 2
    scores = heading_scores(args[-2]["nodes"], args[1], args[2]["mappings"])
    assert args[-2]["nodes"][-1]["id"] not in scores


def test_empty_outline_has_explicit_fallback():
    args = manual()
    args[0]["nodes"] = []
    args[3]["major_sections"] = []
    recover_contents_topics(*args)
    assert args[3]["topic_hierarchy"]["chapters"] == []


def test_only_consecutive_numbered_steps_establish_plain_prefix_ownership():
    rows = [
        {"key": title.lower()}
        for title in [
            "Assembly",
            "Step One Attach",
            "Step Two Tighten",
            "Step Three Inspect",
        ]
    ]
    assert _step_prefix(rows) == [3, 4, 4, 4]
    assert _step_prefix(rows + [{"key": "safety notes"}]) == [3, 4, 4, 4, 4]
    rows[2]["key"] = "step four tighten"
    assert _step_prefix(rows) is None
    assert (
        _step_prefix(
            [
                {"key": title}
                for title in ["Assembly", "Tools", "Precautions", "Results"]
            ]
        )
        is None
    )


def test_unstyled_tail_without_native_tiers_does_not_extend_last_bold_topic():
    args = manual()
    args[1][0]["markdown"] = (
        args[1][0]["markdown"]
        .replace("Tools 1", "**Tools** 1")
        .replace("Process 1", "**Process** 1")
        .replace("Results 1", "**Results** 1")
    )
    args[1][0]["markdown"] += "\n".join(f"Unstyled entry {i} 1" for i in range(25))
    recover_contents_topics(*args)
    assert not args[3]["topic_hierarchy"]["chapters"]
    assert any("style_transition_unresolved" in d for d in args[3]["diagnostics"])


@pytest.mark.parametrize(
    "prefix",
    [
        [
            ("Assembly", "### "),
            ("Parts", ""),
            ("Fit", ""),
            ("Inspection", "### "),
            ("Finish", ""),
            ("Safety", ""),
        ],
        [("Assembly", ""), ("Parts", "*"), ("Inspection", "")],
    ],
)
def test_explicit_prefix_styles_can_precede_bold_topic_tables(prefix):
    args = manual()
    titles = [title for title, _ in prefix] + [n["title"] for n in args[0]["nodes"][1:]]
    text = "# Workshop\n\n" + "\n\n".join("# " + title for title in titles)
    args[0]["nodes"] = markdown_outline(text)["nodes"]
    for index, node in enumerate(args[0]["nodes"]):
        node["level"] = 2 if index == 0 else 3
    args[0]["nodes"][0]["id"] = "major-0"
    args[0]["nodes"][0]["section"]["end"] = len(text.encode())
    args[2]["mappings"][0]["document"]["end"] = len(text.encode())
    toc = args[1][0]["markdown"]
    for title in ["Tools", "Process", "Results"]:
        toc = toc.replace(title + " 1", "**" + title + "** 1")
    prefix_text = "\n".join(
        marker + title + ("*" if marker == "*" else "") + " 1"
        for title, marker in prefix
    )
    args[1][0]["markdown"] = toc.replace(
        "# Workshop\n", "# Workshop\n" + prefix_text + "\n"
    )
    recover_contents_topics(*args)
    nodes = {n["title"]: n for n in args[0]["nodes"]}
    assert nodes["Assembly"]["level"] == 3
    assert nodes["Parts"]["parent"] == nodes["Assembly"]["id"]


def test_tail_tiers_allow_sparse_missing_rows_but_not_missing_majority():
    rows = [{"key": f"entry {i}", "label": i} for i in range(30)]
    widths = {
        (r["key"], r["label"]): [0.24, 0.22, 0.20][i % 3] for i, r in enumerate(rows)
    }
    del widths["entry 1", 1]
    levels = _indent_levels(rows, widths, allow_missing=True)
    assert levels == [None if i == 1 else 3 + i % 3 for i in range(30)]
    assert _indent_levels(rows, widths) is None
    assert (
        _indent_levels(rows, dict(list(widths.items())[:10]), allow_missing=True)
        is None
    )
