import json
import zipfile
from pathlib import Path

import pytest

from blobforge.normalization.mistral import render_mistral_response
from blobforge.reprocessing import reprocess_mdaf
from tests.test_book_hierarchy import _block, _page
from tests.test_reprocessing import _parent


def book(label="CONTENTS", prefix="Chapter", missing=False):
    titles = [f"{prefix} {i}: Realm {i}" for i in range(1, 4)]
    toc = [_block("title", f"# {label}")]
    toc += [_block("title", f"## {title} {i}") for i, title in enumerate(titles, 1)]
    pages = [_page(0, toc)]
    for i, title in enumerate(titles, 1):
        blocks = [_block("text", "Opening prose.")]
        if not missing or i != 2:
            blocks += [_block("title", f"# {title}")]
        blocks += [
            _block("title", "## Topic"),
            _block("text", "Café (p. 1)."),
            _block("footer", str(i)),
        ]
        pages.append(_page(i, blocks))
    return {
        "pages": pages,
        "model": "fixture",
        "usage_info": {"pages_processed": len(pages)},
    }


def check_forest(result):
    encoded = result.text.encode()
    stack = []
    for n in result.outline["nodes"]:
        start, end = n["section"].values()
        assert 0 <= start < end <= len(encoded)
        encoded[:start].decode()
        encoded[: n["heading"]["end"]].decode()
        assert start == n["heading"]["start"] < n["heading"]["end"] <= end
        while stack and stack[-1]["level"] >= n["level"]:
            assert stack.pop()["section"]["end"] == start
        assert n["parent"] == (stack[-1]["id"] if stack else None)
        stack.append(n)


def test_v6_unique_body_title_wins_over_wrong_contents_page():
    native = book()
    native['pages'][0]['blocks'][-1]['content'] = '## Chapter 3: Realm 3 2'
    native['pages'][0]['markdown'] = native['pages'][0]['markdown'].replace('Realm 3 3', 'Realm 3 2')
    old = render_mistral_response(native, normalization_profile='wiki-v4')
    new = render_mistral_response(native, normalization_profile='wiki-v5')
    assert old.text == new.text
    third = next(m for m in new.hierarchy_report['major_sections'] if 'Realm 3' in m['title'])
    assert third['source_page'] == 3
    assert third['evidence'] == 'title-match'
    assert any('conflict_resolved_by_unique_body_title' in d for d in new.hierarchy_report['diagnostics'])
    check_forest(new)


def test_v6_does_not_guess_between_repeated_body_titles():
    native = book()
    native['pages'][0]['blocks'][-1]['content'] = '## Chapter 3: Realm 3 2'
    native['pages'][0]['markdown'] = native['pages'][0]['markdown'].replace('Realm 3 3', 'Realm 3 2')
    native['pages'].append(_page(4, [_block('title', '# Chapter 3: Realm 3'), _block('text', 'Repeated heading.')]))
    native['usage_info']['pages_processed'] = 5
    new = render_mistral_response(native, normalization_profile='wiki-v5')
    assert 'Chapter 3: Realm 3' in new.hierarchy_report['unmatched_entries']
    assert not any('Realm 3' in m['title'] for m in new.hierarchy_report['major_sections'])
    check_forest(new)


def test_v6_retains_observed_unoccupied_opener_before_title_page():
    native = book()
    native['pages'][2]['blocks'] = [_block('title', '# Opening vignette'), _block('text', 'Fiction.'), _block('footer', '2')]
    native['pages'][2]['markdown'] = '# Opening vignette\n\nFiction.\n\n2'
    # The chapter title is on the immediately following physical page, while
    # the next chapter moves later; neither opener belongs to another chapter.
    native['pages'].insert(3, _page(3, [_block('title', '# Chapter 2: Realm 2'), _block('text', 'Rules.')]))
    native['pages'][4]['index'] = 4
    native['usage_info']['pages_processed'] = 5
    new = render_mistral_response(native, normalization_profile='wiki-v5')
    middle = next(m for m in new.hierarchy_report['major_sections'] if 'Realm 2' in m['title'])
    assert middle['source_page'] == 2
    assert any('corroborated_adjacent_opener_retained' in d for d in new.hierarchy_report['diagnostics'])
    check_forest(new)


def test_v6_recipe_keeps_extraction_identity_and_allows_v5_replay(tmp_path):
    from blobforge.recipe_lifecycle import assert_reprocessable
    root = Path(__file__).parents[1] / 'blobforge/recipes'
    old = json.loads((root/'mistral-ocr-4.1-wiki-v5.json').read_text())
    new = json.loads((root/'mistral-ocr-4.1-wiki-v6.json').read_text())
    assert_reprocessable(old, new)
    assert old['lifecycle']['extraction'] == new['lifecycle']['extraction']
    parent, _, native, _ = _parent(tmp_path)
    result = reprocess_mdaf(parent, root/'mistral-ocr-4.1-wiki-v6.json', tmp_path/'new.mdaf')
    with zipfile.ZipFile(result.path) as archive:
        assert archive.read('renditions/ai.mistral/ocr-response.json') == native
        assert json.loads(archive.read('extensions/dev.tionis.blobforge/hierarchy.json'))['method'].endswith('v3')


@pytest.mark.parametrize(
    "label,prefix",
    [
        ("CONTENTS & CREDITS", "Chapter"),
        ("Inhalt", "Kapitel"),
        ("Sommaire", "Chapitre"),
    ],
)
def test_localized_contents_and_chapters_preserve_bytes(label, prefix):
    native = book(label, prefix)
    old = render_mistral_response(native, normalization_profile="wiki-v2")
    new = render_mistral_response(native, normalization_profile="wiki-v4")
    assert old.text == new.text
    assert len(new.hierarchy_report["major_sections"]) == 3
    check_forest(new)


def test_missing_heading_uses_observed_page_and_keeps_leading_prose():
    r = render_mistral_response(book(missing=True), normalization_profile="wiki-v4")
    middle = r.hierarchy_report["major_sections"][1]
    assert middle["source_page"] == 2
    assert middle["evidence"] == "toc-page-alignment"
    assert r.text.encode()[middle["byte_offset"] :].startswith(b"Opening prose.")
    check_forest(r)


def test_first_authored_subheading_is_a_level_three_topic():
    r = render_mistral_response(book(), normalization_profile="wiki-v4")
    topics = [n for n in r.outline["nodes"] if n["title"] == "Topic"]
    assert len(topics) == 3
    assert all(n["level"] == 3 for n in topics)


def test_explicit_toc_tiers_do_not_promote_large_subsections():
    native = book(prefix="Division")
    for page in native["pages"][1:]:
        page["blocks"][2] = _block("title", "# Oversized subsection", True)
    native["pages"][0]["markdown"] += "\nOversized subsection 2\n"
    r = render_mistral_response(native, normalization_profile="wiki-v4")
    assert len(r.hierarchy_report["major_sections"]) == 3
    assert not any(
        m["title"] == "Oversized subsection"
        for m in r.hierarchy_report["major_sections"]
    )


def test_unmatched_top_level_entry_is_not_silent():
    native = book()
    native["pages"][0]["blocks"].append(_block("title", "## Chapter 4: Unknown 999"))
    native["pages"][0]["markdown"] += "\n## Chapter 4: Unknown 999\n"
    r = render_mistral_response(native, normalization_profile="wiki-v4")
    assert "Chapter 4: Unknown" in r.hierarchy_report["unmatched_entries"]
    assert any("partial_hierarchy" in d for d in r.hierarchy_report["diagnostics"])


def test_unlabelled_dense_contents_can_be_detected_structurally():
    native = book()
    native["pages"][0]["blocks"] = []
    native["pages"][0]["markdown"] = "\n".join(
        f"Chapter {i}: Realm {i} {i}" for i in range(1, 13)
    )
    r = render_mistral_response(native, normalization_profile="wiki-v4")
    assert r.hierarchy_report["toc_pages"] == [0]
    assert len(r.hierarchy_report["major_sections"]) == 3
    assert r.hierarchy_report["unmatched_entries"]


def test_no_toc_preserves_outline_and_reports_fallback():
    native = book()
    native["pages"][0] = _page(0, [_block("text", "A regular introductory paragraph.")])
    r = render_mistral_response(native, normalization_profile="wiki-v4")
    assert (
        "no_contents_evidence; retained_markdown_outline"
        in r.hierarchy_report["diagnostics"]
    )


def test_inferred_offsets_do_not_create_observed_citation_labels():
    native = book()
    for page in native["pages"]:
        page["blocks"] = [b for b in page["blocks"] if b["type"] != "footer"]
    r = render_mistral_response(native, normalization_profile="wiki-v4")
    assert r.hierarchy_report["alignment_offset"] == 0
    assert r.source_map["references"] == []
    assert all(
        m["source"]["selectors"][0].get("label_start") is None
        for m in r.source_map["mappings"]
    )


def test_replay_new_generation_is_deterministic_and_retains_native(tmp_path):
    parent, _, native, _ = _parent(tmp_path)
    recipe = (
        Path(__file__).parents[1] / "blobforge/recipes/mistral-ocr-4.1-wiki-v5.json"
    )
    a = reprocess_mdaf(parent, recipe, tmp_path / "a.mdaf")
    b = reprocess_mdaf(parent, recipe, tmp_path / "b.mdaf")
    assert a.path.read_bytes() == b.path.read_bytes()
    with zipfile.ZipFile(a.path) as z:
        assert z.read("renditions/ai.mistral/ocr-response.json") == native
        assert json.loads(z.read("extensions/dev.tionis.blobforge/hierarchy.json"))[
            "method"
        ].endswith("v2")


def test_v5_runtime_and_evaluator_select_profile_without_changing_provider_key(
    monkeypatch, tmp_path
):
    from types import SimpleNamespace

    from blobforge import cli
    from blobforge.recipe_runtime import mistral_wiki_v3_recipe, mistral_wiki_v5_recipe

    kwargs = {
        "max_pages": 20,
        "max_cost_usd": 1,
        "response_cache": tmp_path,
        "api_rights_confirmed": True,
        "cache_only": True,
    }
    old, new = mistral_wiki_v3_recipe(**kwargs), mistral_wiki_v5_recipe(**kwargs)
    assert (
        new.parameters["provider_request_digest"]
        == old.parameters["provider_request_digest"]
    )
    assert new.parameters["normalization_profile"] == "wiki-v4"
    assert new.recipe_digest != old.recipe_digest
    assert not new.claim_unassigned
    captured = {}

    def converter(*args, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            artifact_path="fixture",
            identity="fixture",
            elapsed_seconds=0,
            diagnostics=[],
        )

    monkeypatch.setattr(cli, "run_converter", converter)
    args = SimpleNamespace(
        engine="mistral-wiki-v5",
        path="fixture.pdf",
        output=None,
        no_ocr=False,
        no_tables=False,
        no_images=False,
        max_pages=3,
        max_cost_usd=1,
        model=None,
        confirm_api_rights=True,
        response_cache=str(tmp_path),
        plan=False,
        timeout=60,
    )
    assert cli.cmd_evaluate_converter(args) == 0
    assert captured["parameters"]["normalization_profile"] == "wiki-v4"
    assert captured["parameters"]["recipe_digest"] == new.recipe_digest


def test_split_chapter_title_matches_without_offset_or_footer_evidence():
    native = book()
    for p in native["pages"][1:]:
        i = p["index"]
        p["blocks"] = [
            _block("title", f"# Chapter {i}"),
            _block("title", f"# Realm {i}"),
            _block("text", "Body"),
        ]
    r = render_mistral_response(native, normalization_profile="wiki-v4")
    assert len(r.hierarchy_report["major_sections"]) == 3
    assert all(
        m["evidence"] == "title-match" for m in r.hierarchy_report["major_sections"]
    )
    check_forest(r)


def test_appendices_do_not_suppress_unnumbered_body_chapters():
    native = book(prefix="Division")
    for i in range(1, 4):
        native["pages"][0]["markdown"] += f"\nAppendix {i}: Supplement {i} {i + 3}\n"
    r = render_mistral_response(native, normalization_profile="wiki-v4")
    assert len(r.hierarchy_report["major_sections"]) == 3
    assert all(
        m["title"].startswith("Division") for m in r.hierarchy_report["major_sections"]
    )


def test_longer_single_line_title_does_not_supply_short_title_geometry():
    from blobforge.mdaf.builder import markdown_outline
    from blobforge.normalization.book_structure import _body_candidates

    text = "# Rules\n\n# Rules for complex situations\n"
    page = _page(
        0,
        [
            _block("title", "# Rules"),
            _block("title", "# Rules for complex situations", True),
        ],
    )
    mappings = [
        {
            "document": {"start": 0, "end": len(text)},
            "source": {"selectors": [{"start": 0}]},
        }
    ]
    candidates = _body_candidates(
        markdown_outline(text)["nodes"], [page], mappings, text
    )
    assert (
        candidates["rules"][0]["size"]
        < candidates["rules for complex situations"][0]["size"]
    )


def test_wrapped_column_title_is_joined_within_its_own_column():
    from blobforge.normalization.book_structure import _entries

    page = _page(0, [])
    page["markdown"] = (
        "| Main Topic: | 8 | Other | 20 |\n| Continuation | | Unrelated | 21 |"
    )
    entries = _entries([page])
    assert entries["main topic continuation"]["labels"] == {"8"}
    assert entries["main topic continuation"]["title"] == "Main Topic: Continuation"
    assert "main topic" not in entries
    assert entries["unrelated"]["labels"] == {"21"}
