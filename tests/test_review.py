import json
import stat
from pathlib import Path

import pytest

from blobforge.mdaf import MdafSource, blake3_file, build_mdaf
from blobforge.mdaf.builder import activity
from blobforge.review import _page_text, build_review_bundle, parse_page_selection


def _artifact(tmp_path, source, name, text):
    split = text.index("PAGE TWO")
    mappings = [
        {
            "document": {"start": 0, "end": len(text[:split].encode("utf-8"))},
            "source": {
                "source_id": "document",
                "selectors": [
                    {"type": "interval", "unit": "page", "start": 0, "end": 1}
                ],
            },
        },
        {
            "document": {
                "start": len(text[:split].encode("utf-8")),
                "end": len(text.encode("utf-8")),
            },
            "source": {
                "source_id": "document",
                "selectors": [
                    {"type": "interval", "unit": "page", "start": 1, "end": 2}
                ],
            },
        },
    ]
    return build_mdaf(
        tmp_path / f"{name}.mdaf",
        text=text,
        sources=[MdafSource("document", "application/pdf", blake3_file(source))],
        activities=[
            activity(
                activity_id="activity:convert",
                kind="document-extraction",
                tools=[{"name": name, "version": "1"}],
                inputs=["source:document"],
                outputs=["text.md", "source-map.json", "provenance.json"],
                parameters={},
            )
        ],
        producer={"name": name, "version": "1"},
        source_map={"mappings": mappings, "references": []},
    )


def test_page_selection_is_one_based_and_bounded():
    assert parse_page_selection(None, {2, 0, 1}) == (0, 1, 2)
    assert parse_page_selection("1,3-4", {0, 1, 2, 3}) == (0, 2, 3)
    with pytest.raises(ValueError, match="not mapped"):
        parse_page_selection("5", {0, 1})
    with pytest.raises(ValueError, match="invalid"):
        parse_page_selection("3-2", {0, 1, 2})


def test_page_text_rejects_ambiguous_multi_page_mapping():
    source_map = {
        "mappings": [
            {
                "document": {"start": 0, "end": 4},
                "source": {
                    "selectors": [
                        {"type": "interval", "unit": "page", "start": 0, "end": 2}
                    ]
                },
            }
        ]
    }
    with pytest.raises(ValueError, match="page-exact"):
        _page_text(b"text", source_map)


def test_review_bundle_is_blinded_source_backed_and_deterministic(tmp_path):
    source = tmp_path / "book.pdf"
    source.write_bytes(b"%PDF synthetic source")
    first = _artifact(
        tmp_path,
        source,
        "engine-one",
        "# Caf\N{LATIN SMALL LETTER E WITH ACUTE}\n<script>alert(1)</script>\nPAGE TWO alpha",
    )
    second = _artifact(
        tmp_path,
        source,
        "engine-two",
        "# Cafe\nordinary\nPAGE TWO beta",
    )
    output = tmp_path / "review"
    key_path = tmp_path / "private-key.json"
    result = build_review_bundle(
        source,
        [second.path, first.path],
        output,
        pages="1-2",
        seed="fixed-test-seed",
        key_output=key_path,
    )
    assert result.pages == 2
    assert result.artifacts == 2
    assert (output / "source.pdf").read_bytes() == source.read_bytes()
    public = json.loads((output / "review.json").read_text(encoding="utf-8"))
    key = json.loads(key_path.read_text(encoding="utf-8"))
    assert public["campaign_digest"] == key["campaign_digest"] == result.campaign_digest
    assert [item["label"] for item in public["candidates"]] == ["A", "B"]
    assert "identity" not in json.dumps(public)
    assert "producer" not in json.dumps(public)
    assert {item["producer"]["name"] for item in key["candidates"]} == {
        "engine-one",
        "engine-two",
    }
    assert stat.S_IMODE(key_path.stat().st_mode) == 0o600
    html = (output / "index.html").read_text(encoding="utf-8")
    assert "engine-one" not in html and "engine-two" not in html
    assert "<script>alert(1)</script>" not in html
    assert "\\u003cscript>alert(1)\\u003c/script>" in html
    assert "autosave unavailable; use Export scores" in html
    assert "JSON.stringify(output,null,2)+'\\n'" in html

    # Input order cannot change the blinded campaign or label assignment.
    repeated = build_review_bundle(
        source,
        [first.path, second.path],
        tmp_path / "review-again",
        seed="fixed-test-seed",
    )
    repeated_key = json.loads(repeated.key_path.read_text(encoding="utf-8"))
    assert repeated.campaign_digest == result.campaign_digest
    assert [item["identity"] for item in repeated_key["candidates"]] == [
        item["identity"] for item in key["candidates"]
    ]


def test_review_rejects_mismatched_source_and_existing_destination(tmp_path):
    source = tmp_path / "book.pdf"
    source.write_bytes(b"%PDF one")
    first = _artifact(tmp_path, source, "one", "PAGE ONE\nPAGE TWO one")
    second = _artifact(tmp_path, source, "two", "PAGE ONE\nPAGE TWO two")
    wrong = tmp_path / "wrong.pdf"
    wrong.write_bytes(b"%PDF different")
    with pytest.raises(ValueError, match="do not all match"):
        build_review_bundle(wrong, [first.path, second.path], tmp_path / "wrong-review")
    existing = tmp_path / "exists"
    existing.mkdir()
    with pytest.raises(ValueError, match="already exists"):
        build_review_bundle(source, [first.path, second.path], existing)
