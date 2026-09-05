import json
import zipfile
from pathlib import Path
from types import SimpleNamespace

from blobforge.normalization.hierarchy import page_labels, page_references
from blobforge.mdaf.builder import markdown_outline
from blobforge.normalization.mistral import render_mistral_response
from blobforge.reprocessing import reprocess_mdaf
from tests.test_reprocessing import _parent
from blobforge.recipe_runtime import mistral_wiki_v3_recipe, mistral_wiki_v4_recipe


def test_evaluate_v4_uses_new_profile_and_unchanged_extraction_key(monkeypatch, tmp_path):
    from blobforge import cli
    captured = {}
    def converter(*args, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(artifact_path="fixture.mdaf", identity="fixture", elapsed_seconds=0, diagnostics=[])
    monkeypatch.setattr(cli, "run_converter", converter)
    args = SimpleNamespace(engine="mistral-wiki-v4", path="fixture.pdf", output=None,
                           no_ocr=False, no_tables=False, no_images=False, max_pages=3,
                           max_cost_usd=1, model=None, confirm_api_rights=True,
                           response_cache=str(tmp_path), plan=False, timeout=60)
    assert cli.cmd_evaluate_converter(args) == 0
    assert captured["parameters"]["normalization_profile"] == "wiki-v3"
    assert captured["recipe"]["lifecycle"]["postprocessing"]["profile"] == "wiki-v3"


def test_second_upgrade_retains_earlier_lineage(tmp_path):
    parent, _, _, _ = _parent(tmp_path)
    recipes = Path(__file__).parents[1] / "blobforge/recipes"
    first = reprocess_mdaf(parent, recipes / "mistral-ocr-4.1-wiki-v3.json", tmp_path / "v3.mdaf")
    second = reprocess_mdaf(first.path, recipes / "mistral-ocr-4.1-wiki-v4.json", tmp_path / "v4.mdaf")
    from blobforge.recipe_lifecycle import PARENT_INFO_PATH, PARENT_PROVENANCE_PATH, PREVIOUS_RECIPE_PATH
    with zipfile.ZipFile(first.path) as older, zipfile.ZipFile(second.path) as newer:
        for path in (PARENT_INFO_PATH, PARENT_PROVENANCE_PATH, PREVIOUS_RECIPE_PATH):
            archived = f"extensions/dev.tionis.blobforge/ancestors/{first.identity.split(':', 1)[1]}/{Path(path).name}"
            assert newer.read(archived) == older.read(path)


def _block(kind, content, large=False, narrow=False):
    return {"type": kind, "content": content, "top_left_x": 50, "top_left_y": 50,
            "bottom_right_x": 150 if narrow else 650,
            "bottom_right_y": 150 if large else 70}


def test_new_outline_ignores_fenced_headings_without_changing_frozen_projection():
    text = "# Café\n```md\n# Example\n```\n## Real\n~~~\n# Unclosed\n"
    assert len(markdown_outline(text)["nodes"]) == 4
    nodes = markdown_outline(text, skip_fences=True)["nodes"]
    assert [n["title"] for n in nodes] == ["Café", "Real"]
    assert text.encode()[nodes[1]["heading"]["start"]:].startswith(b"## Real")


def _page(index, blocks):
    return {"index": index, "blocks": blocks, "markdown": "\n\n".join(b["content"] for b in blocks),
            "dimensions": {"width": 800, "height": 1000}, "images": []}


def _native(numbered=False):
    return {"model": "fixture", "usage_info": {"pages_processed": 3}, "pages": [
        _page(0, [_block("title", "# CONTENTS"), _block("text", "First Realm...1\nSecond Realm...2")]),
        _page(1, [*([_block("title", "# CHAPTER ONE", True)] if numbered else []),
                  _block("title", "# First Realm", True), _block("text", "Café (p. 2)."),
                  _block("title", "## Small topic"), _block("text", "Details."), _block("footer", "1")]),
        _page(2, [*([_block("title", "# CHAPTER TWO", True)] if numbered else []),
                  _block("title", "# Second Realm", True), _block("text", "Target."), _block("footer", "2")]),
    ]}


def test_contents_and_geometry_produce_chapters_without_changing_markdown():
    native = _native()
    old = render_mistral_response(native, normalization_profile="wiki-v2")
    new = render_mistral_response(native, normalization_profile="wiki-v3")
    assert old.text == new.text
    assert [n["title"] for n in new.outline["nodes"] if n["level"] == 2] == [
        "Front matter", "First Realm", "Second Realm"]
    topic = next(n for n in new.outline["nodes"] if n["title"] == "Small topic")
    first = next(n for n in new.outline["nodes"] if n["title"] == "First Realm")
    assert topic["parent"] == first["id"]
    assert topic["level"] == 3
    ref = new.source_map["references"][0]
    assert new.text.encode()[ref["document"]["start"]:ref["document"]["end"]] == b"(p. 2)"
    assert ref["target"]["selectors"][0]["start"] == 2


def test_numbered_chapter_titles_combine_only_adjacent_opener_headings():
    native = _native(True)
    native["pages"][1]["blocks"].insert(3, _block("title", "# Not part of the title", True))
    result = render_mistral_response(native, normalization_profile="wiki-v3")
    majors = result.hierarchy_report["major_sections"]
    assert [n["title"] for n in majors] == ["CHAPTER ONE: First Realm", "CHAPTER TWO: Second Realm"]
    assert any(n["title"] == "Not part of the title" and n["level"] == 3 for n in result.outline["nodes"])


def test_advertisements_and_wrapped_small_titles_do_not_become_chapters():
    native = _native()
    native["pages"][0]["blocks"].append(_block("text", "A long wrapped subsection title...2\nAdvertisement"))
    native["pages"][0]["markdown"] += "\nA long wrapped subsection title...2\nAdvertisement"
    native["pages"][2]["blocks"] += [
        _block("title", "# A long wrapped subsection title", True, True),
        _block("title", "# Advertisement", True),
    ]
    result = render_mistral_response(native, normalization_profile="wiki-v3")
    assert len(result.hierarchy_report["major_sections"]) == 2


def test_no_contents_preserves_outline_and_reports_fallback():
    native = _native()
    native["pages"][0]["blocks"][0]["content"] = "# Preface"
    result = render_mistral_response(native, normalization_profile="wiki-v3")
    assert result.hierarchy_report["diagnostics"] == ["no_contents_evidence; retained_markdown_outline"]


def test_duplicate_page_labels_and_external_or_range_citations_stay_unbound():
    pages = [_page(0, [_block("footer", "1")]), _page(1, [_block("footer", "1")]),
             _page(2, [_block("footer", "2"), _block("footer", "Copyright 2026")])]
    assert page_labels(pages) == {2: "2"}
    text = "(p. 1) (pp. 2-3) (p. 2, Other Book) (page 2) (p. 3)"
    references = page_references(text, page_labels(pages), "original")
    assert len(references) == 1
    assert references[0]["target"]["source_id"] == "original"
    span = references[0]["document"]
    assert text[span["start"]:span["end"]] == "(page 2)"


def test_new_recipe_replays_with_explicit_recovered_name_and_native_evidence(tmp_path):
    parent, identity, native, _ = _parent(tmp_path)
    recipe = Path(__file__).parents[1] / "blobforge/recipes/mistral-ocr-4.1-wiki-v4.json"
    first = reprocess_mdaf(parent, recipe, tmp_path / "first.mdaf", source_name="/private/Fixture Book.pdf")
    second = reprocess_mdaf(parent, recipe, tmp_path / "second.mdaf", source_name="Fixture Book.pdf")
    assert first.path.read_bytes() == second.path.read_bytes()
    with zipfile.ZipFile(first.path) as archive:
        info = json.loads(archive.read("info.json"))
        assert info["title"] == "Fixture Book"
        assert info["derived_from"] == [identity]
        assert archive.read("renditions/ai.mistral/ocr-response.json") == native
        assert "private" not in archive.read("provenance.json").decode()
        assert archive.read("extensions/dev.tionis.blobforge/hierarchy.json")


def test_v4_runtime_keeps_provider_cache_identity_and_requires_explicit_recipe(tmp_path):
    kwargs = dict(max_pages=20, max_cost_usd=1, response_cache=tmp_path,
                  api_rights_confirmed=True, cache_only=True)
    old, new = mistral_wiki_v3_recipe(**kwargs), mistral_wiki_v4_recipe(**kwargs)
    assert old.recipe_digest != new.recipe_digest
    assert new.parameters["provider_request_digest"] == old.parameters["provider_request_digest"]
    assert new.command == old.command
    assert new.claim_unassigned is False
    assert new.parameters["normalization_profile"] == "wiki-v3"
