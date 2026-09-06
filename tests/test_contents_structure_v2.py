"""Bounded recovery in synthetic workshop manuals, never book-name rules."""

import copy

import pytest

from blobforge.markdown_outline import markdown_outline
from blobforge.normalization.contents_structure_v2 import (
    _wrapped_rows,
    recover_contents_topics_v2,
)


def manual():
    topics = ["Tools", "Preparation", "Assembly", "Finishing", "Inspection"]
    chunks = ["# Workshop\n\n"]
    toc = "# Workshop\n\n"
    mappings = []
    for page, topic in enumerate(topics, 1):
        start = len("".join(chunks).encode())
        for title in [topic, f"{topic} basics", f"{topic} examples"]:
            chunks.append(f"# {title}\n\nExample prose.\n\n")
            toc += f"{title if title == topic else '*' + title + '*'} {page}\n\n"
        mappings.append(
            {
                "document": {
                    "start": 0 if page == 1 else start,
                    "end": len("".join(chunks).encode()),
                },
                "source": {"selectors": [{"start": page}]},
            }
        )
    text = "".join(chunks)
    outline = markdown_outline(text)
    outline["nodes"][0]["id"] = "major-0"
    for i, node in enumerate(outline["nodes"]):
        node["level"] = 2 if i == 0 else 3
    outline["nodes"][0]["section"]["end"] = len(text.encode())
    return (
        outline,
        [{"index": 0, "markdown": toc}],
        {"mappings": mappings},
        {
            "major_sections": [{"title": "Workshop", "source_page": 1}],
            "toc_pages": [0],
            "alignment_offset": 0,
            "diagnostics": [],
        },
    )


def test_missing_topic_retains_preceding_and_uncertain_page_only():
    args = manual()
    args[1][0]["markdown"] = args[1][0]["markdown"].replace(
        "Preparation 2", "Unknown 2"
    )
    result = recover_contents_topics_v2(*args)
    nodes = {n["title"]: n for n in result["nodes"]}
    for topic in ["Tools", "Preparation"]:
        assert nodes[topic + " basics"]["level"] == 3
        assert nodes[topic + " basics"]["parent"] == "major-0"
    for topic in ["Assembly", "Finishing", "Inspection"]:
        assert nodes[topic + " basics"]["parent"] == nodes[topic]["id"]
        assert nodes[topic + " basics"]["level"] == 4
    chapter = args[3]["topic_hierarchy"]["chapters"][0]
    assert chapter["unverified_regions"] == [
        {
            "start": nodes["Tools"]["heading"]["start"],
            "end": nodes["Assembly"]["heading"]["start"],
            "source_page": 2,
        }
    ]
    assert any(
        d.startswith("contents_unverified_regions_retained:")
        for d in args[3]["diagnostics"]
    )


@pytest.mark.parametrize("missing_page", [None, 90])
def test_unlocatable_missing_topic_rejects_recovery(missing_page):
    args = manual()
    args[1][0]["markdown"] = args[1][0]["markdown"].replace(
        "Preparation 2", "Unknown 2"
    )
    if missing_page is None:
        args[3]["alignment_offset"] = None
    else:
        args[2]["mappings"][1]["source"]["selectors"][0]["start"] = missing_page
    levels = [n["level"] for n in args[0]["nodes"]]
    result = recover_contents_topics_v2(*args)
    assert [n["level"] for n in result["nodes"]] == levels
    assert not args[3]["topic_hierarchy"]["chapters"]


@pytest.mark.parametrize("offset,matched", [(0, 15), (10, 14)])
@pytest.mark.parametrize(
    "toc_title,body_title",
    [
        ("Éléments and parts", "Elements & Parts"),
        ("Éléments & parts", "Elements and Parts"),
    ],
)
def test_accent_and_conjunction_alias_requires_aligned_page(
    offset, matched, toc_title, body_title
):
    args = manual()
    args[1][0]["markdown"] = args[1][0]["markdown"].replace(
        "*Assembly basics*", f"*{toc_title}*"
    )
    node = next(n for n in args[0]["nodes"] if n["title"] == "Assembly basics")
    node["title"] = body_title
    args[3]["alignment_offset"] = offset
    # Keep grouping within the single chapter while changing page corroboration.
    args[3]["major_sections"][0]["source_page"] = 0
    recover_contents_topics_v2(*args)
    assert args[3]["topic_hierarchy"]["chapters"][0]["matched_entries"] == matched


def test_alias_collision_never_selects_first_heading():
    args = manual()
    args[1][0]["markdown"] = args[1][0]["markdown"].replace(
        "*Assembly basics*", "*Éléments and parts*"
    )
    for title, replacement in [
        ("Assembly basics", "Elements & Parts"),
        ("Assembly examples", "Élements and Parts"),
    ]:
        next(n for n in args[0]["nodes"] if n["title"] == title)["title"] = replacement
    recover_contents_topics_v2(*args)
    chapter = args[3]["topic_hierarchy"]["chapters"][0]
    assert chapter["matched_entries"] == 13
    assert "éléments and parts" in chapter["unmatched"]


def test_wrapped_contents_boundaries_are_joined_without_mutating_native():
    pages = [
        {
            "index": 0,
            "markdown": "## SECTION:\n\n## Assembly 3\n\n*Details* 3\n\n## INTERLUDE:\nAn example 4",
        }
    ]
    original = copy.deepcopy(pages)
    rows = list(_wrapped_rows(pages, [0]))
    assert [(r["key"], r["label"], r["boundary"]) for r in rows] == [
        ("section assembly", 3, True),
        ("details", 3, False),
        ("interlude an example", 4, True),
    ]
    assert pages == original


def test_lost_italic_tail_does_not_create_false_topics():
    args = manual()
    markdown = args[1][0]["markdown"]
    cut = markdown.index("Finishing 4")
    args[1][0]["markdown"] = markdown[:cut] + markdown[cut:].replace("*", "")
    result = recover_contents_topics_v2(*args)
    assert all(n["level"] == 3 for n in result["nodes"][1:])
    assert not args[3]["topic_hierarchy"]["chapters"]
    assert any(
        d.startswith("contents_style_transition_unresolved:")
        for d in args[3]["diagnostics"]
    )


def test_no_recovery_claim_when_gaps_cover_entire_chapter():
    args = manual()
    args[1][0]["markdown"] = args[1][0]["markdown"].replace("Assembly 3", "Unknown 3")
    args[2]["mappings"] = [
        {
            "document": {"start": 0, "end": args[0]["nodes"][0]["section"]["end"]},
            "source": {"selectors": [{"start": 3}]},
        }
    ]
    recover_contents_topics_v2(*args)
    assert not args[3]["topic_hierarchy"]["chapters"]
