import base64
import importlib.util
import json
import sys
from pathlib import Path

import pytest

from blobforge.mdaf import blake3_bytes
from blobforge.mdaf.digest import canonical_json_bytes


def _load_adapter():
    path = Path(__file__).parents[1] / "evaluators" / "mistral" / "adapter.py"
    spec = importlib.util.spec_from_file_location("blobforge_test_mistral_adapter", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_frozen_recipe_identity():
    path = (
        Path(__file__).parents[1]
        / "blobforge"
        / "recipes"
        / "mistral-ocr-4.1-v1.json"
    )
    recipe = json.loads(path.read_text(encoding="utf-8"))
    assert blake3_bytes(canonical_json_bytes(recipe)) == (
        "blake3:982a97ca1d45f5a0ac30dd8c7507efb594688d1b949f406ef4620f3352e723c7"
    )

    wiki_path = path.with_name("mistral-ocr-4.1-wiki-v1.json")
    wiki_recipe = json.loads(wiki_path.read_text(encoding="utf-8"))
    assert blake3_bytes(canonical_json_bytes(wiki_recipe)) == (
        "blake3:52d29542b2171c154f877d59e4e16019b85296ac4d12a6de97d2080a81a18dba"
    )
    assert wiki_recipe["base_recipe"]["digest"] == blake3_bytes(
        canonical_json_bytes(recipe)
    )


def _request(tmp_path, output, **parameters):
    source = tmp_path / "source.pdf"
    source.write_bytes(b"%PDF synthetic")
    request = tmp_path / f"request-{output.name}.json"
    request.write_text(
        json.dumps(
            {
                "contract": "dev.tionis.blobforge.converter-bundle/v1",
                "source_path": str(source),
                "output_dir": str(output),
                "parameters": {
                    "max_pages": 2,
                    "max_cost_usd": 1,
                    "recipe_digest": "blake3:" + "a" * 64,
                    "api_rights_confirmed": True,
                    **parameters,
                },
            }
        ),
        encoding="utf-8",
    )
    return request


def _response():
    image = base64.b64encode(b"\xff\xd8\xffjpeg bytes").decode("ascii")
    return {
        "model": "mistral-ocr-4-1",
        "usage_info": {"pages_processed": 2, "doc_size_bytes": 14},
        "pages": [
            {
                "index": 0,
                "markdown": "# Café\n\n![figure](same.png)",
                "images": [
                    {
                        "id": "../same.png",
                        "image_base64": f"data:image/jpeg;base64,{image}",
                    }
                ],
                "confidence_scores": {"average_page_confidence_score": 0.91},
                "blocks": [
                    {
                        "type": "title",
                        "content": "# Café",
                        "top_left_x": 1,
                        "top_left_y": 2,
                        "bottom_right_x": 30,
                        "bottom_right_y": 20,
                    }
                ],
            },
            {
                "index": 1,
                "markdown": "Second page",
                "images": [],
                "confidence_scores": {"average_page_confidence_score": 0.8},
                "blocks": [],
            },
        ],
    }


def test_success_is_captured_then_replayed_without_api_key(tmp_path, monkeypatch):
    adapter = _load_adapter()
    cache = tmp_path / "responses"
    monkeypatch.setenv("BLOBFORGE_MISTRAL_RESPONSE_CACHE", str(cache))
    monkeypatch.setenv("MISTRAL_API_KEY", "not-written-to-output")
    monkeypatch.setattr(adapter, "_page_count", lambda _source: 2)
    monkeypatch.setattr(adapter, "version", lambda _name: "2.9.4")
    calls = []
    monkeypatch.setattr(
        adapter,
        "_perform_request",
        lambda source, model, api_key: calls.append((source, model, api_key)) or _response(),
    )

    first = tmp_path / "first"
    monkeypatch.setattr(sys, "argv", ["adapter", str(_request(tmp_path, first))])
    assert adapter.main() == 0
    assert len(calls) == 1
    cache_entry = next(cache.glob("*/*.json"))
    assert cache_entry.stat().st_mode & 0o777 == 0o600
    assert "not-written-to-output" not in cache_entry.read_text(encoding="utf-8")

    monkeypatch.delenv("MISTRAL_API_KEY")
    second = tmp_path / "second"
    monkeypatch.setattr(sys, "argv", ["adapter", str(_request(tmp_path, second))])
    assert adapter.main() == 0
    assert len(calls) == 1
    assert (first / "data/text.md").read_bytes() == (second / "data/text.md").read_bytes()
    assert (first / "data/native/response.json").read_bytes() == (
        second / "data/native/response.json"
    ).read_bytes()

    text = (second / "data/text.md").read_text(encoding="utf-8")
    assert "assets/page-0000-000-same.jpg" in text
    source_map = json.loads((second / "data/source-map.json").read_text(encoding="utf-8"))
    assert [item["confidence"] for item in source_map["mappings"]] == [0.91, 0.8]
    assert source_map["mappings"][1]["document"]["start"] == len(
        text.split("\n\nSecond page", 1)[0].encode("utf-8")
    ) + 2
    bundle = json.loads((second / "bundle.json").read_text(encoding="utf-8"))
    asset = next(item for item in bundle["members"] if item["role"] == "asset")
    assert asset["media_type"] == "image/jpeg"
    assert bundle["parameters"] == {
        "model": "mistral-ocr-4-1",
        "include_blocks": True,
        "confidence_scores_granularity": "block",
        "include_image_base64": True,
        "recipe_digest": "blake3:" + "a" * 64,
    }
    assert any("cache hit" in item["message"] for item in bundle["diagnostics"])


def test_wiki_profile_reuses_raw_cache_and_declares_normalization(tmp_path, monkeypatch):
    adapter = _load_adapter()
    cache = tmp_path / "responses"
    monkeypatch.setenv("BLOBFORGE_MISTRAL_RESPONSE_CACHE", str(cache))
    monkeypatch.setenv("MISTRAL_API_KEY", "one-use-test-key")
    monkeypatch.setattr(adapter, "_page_count", lambda _source: 2)
    monkeypatch.setattr(adapter, "version", lambda _name: "2.9.4")
    response = _response()
    response["pages"][0]["dimensions"] = {"width": 788, "height": 1023}
    response["pages"][0]["blocks"] = [
        {"type": "header", "content": "REPEATED HEADER"},
        {"type": "title", "content": "# Café"},
        {
            "type": "table",
            "content": "| Name | | Value |\n| --- | --- | --- |\n| Ada | | 5 |",
        },
        {
            "type": "image",
            "content": "![figure](same.png)",
            "top_left_y": 200,
            "bottom_right_y": 800,
        },
        {"type": "footer", "content": "2 REPEATED HEADER"},
    ]
    response["pages"][1]["dimensions"] = {"width": 788, "height": 1023}
    response["pages"][1]["blocks"] = [
        {"type": "text", "content": "Second page"}
    ]
    calls = []
    monkeypatch.setattr(
        adapter,
        "_perform_request",
        lambda *_args: calls.append(True) or response,
    )

    raw = tmp_path / "raw"
    monkeypatch.setattr(sys, "argv", ["adapter", str(_request(tmp_path, raw))])
    assert adapter.main() == 0
    assert calls == [True]

    monkeypatch.delenv("MISTRAL_API_KEY")
    wiki = tmp_path / "wiki"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "adapter",
            str(
                _request(
                    tmp_path,
                    wiki,
                    recipe_digest="blake3:" + "b" * 64,
                    provider_request_digest="blake3:" + "a" * 64,
                    normalization_profile="wiki-v1",
                )
            ),
        ],
    )
    assert adapter.main() == 0
    assert calls == [True]
    text = (wiki / "data/text.md").read_text(encoding="utf-8")
    assert "REPEATED HEADER" not in text
    assert "<table>" in text and 'colspan="2"' in text
    bundle = json.loads((wiki / "bundle.json").read_text(encoding="utf-8"))
    assert bundle["markdown_features"] == ["raw-html", "semantic-html-table-v1"]
    assert bundle["additional_tools"][0]["name"] == "blobforge-wiki-normalizer"
    assert bundle["parameters"]["provider_request_digest"] == "blake3:" + "a" * 64


def test_ceiling_rejects_before_cache_or_api(tmp_path, monkeypatch):
    adapter = _load_adapter()
    monkeypatch.setattr(adapter, "_page_count", lambda _source: 2)
    monkeypatch.setattr(
        adapter,
        "_perform_request",
        lambda *_args: pytest.fail("provider must not be called"),
    )
    output = tmp_path / "output"
    monkeypatch.setattr(
        sys,
        "argv",
        ["adapter", str(_request(tmp_path, output, max_pages=1))],
    )
    with pytest.raises(ValueError, match="page ceiling"):
        adapter.main()

    output = tmp_path / "nan-output"
    monkeypatch.setattr(
        sys,
        "argv",
        ["adapter", str(_request(tmp_path, output, max_cost_usd=float("nan")))],
    )
    with pytest.raises(ValueError, match="spend ceiling"):
        adapter.main()


@pytest.mark.parametrize("flag", ["do_ocr", "do_table_structure", "extract_images"])
def test_frozen_recipe_rejects_disabled_features(tmp_path, monkeypatch, flag):
    adapter = _load_adapter()
    monkeypatch.setattr(adapter, "_page_count", lambda _source: 2)
    output = tmp_path / flag
    monkeypatch.setattr(
        sys,
        "argv",
        ["adapter", str(_request(tmp_path, output, **{flag: False}))],
    )
    with pytest.raises(ValueError, match="frozen Mistral recipe requires"):
        adapter.main()


def test_corrupt_cache_fails_closed_without_repurchase(tmp_path, monkeypatch):
    adapter = _load_adapter()
    cache = tmp_path / "responses"
    monkeypatch.setenv("BLOBFORGE_MISTRAL_RESPONSE_CACHE", str(cache))
    monkeypatch.setenv("MISTRAL_API_KEY", "available-but-unused")
    monkeypatch.setattr(adapter, "_page_count", lambda _source: 2)
    monkeypatch.setattr(
        adapter,
        "_perform_request",
        lambda *_args: pytest.fail("corrupt cache must not trigger provider"),
    )
    request = _request(tmp_path, tmp_path / "output")
    source_sha = adapter._sha256_file(tmp_path / "source.pdf")
    request_id, _ = adapter._request_identity(
        source_sha, "blake3:" + "a" * 64, "mistral-ocr-4-1"
    )
    path = adapter._cache_path(cache, request_id)
    path.parent.mkdir(parents=True)
    path.write_text("not json", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["adapter", str(request)])
    with pytest.raises(ValueError, match="invalid Mistral response cache"):
        adapter.main()


def test_cache_miss_requires_explicit_api_rights(tmp_path, monkeypatch):
    adapter = _load_adapter()
    monkeypatch.setenv("BLOBFORGE_MISTRAL_RESPONSE_CACHE", str(tmp_path / "cache"))
    monkeypatch.setenv("MISTRAL_API_KEY", "must-not-be-used")
    monkeypatch.setattr(adapter, "_page_count", lambda _source: 2)
    monkeypatch.setattr(
        adapter,
        "_perform_request",
        lambda *_args: pytest.fail("rights rejection must precede provider call"),
    )
    request = _request(
        tmp_path,
        tmp_path / "output",
        api_rights_confirmed=False,
    )
    monkeypatch.setattr(sys, "argv", ["adapter", str(request)])
    with pytest.raises(ValueError, match="api_rights_confirmed"):
        adapter.main()


def test_response_requires_complete_page_and_usage_coverage():
    adapter = _load_adapter()
    response = _response()
    response["pages"][1]["index"] = 4
    with pytest.raises(ValueError, match="exactly cover"):
        adapter._validate_response(response, 2)
    response = _response()
    response["usage_info"]["pages_processed"] = 1
    with pytest.raises(ValueError, match="pages_processed"):
        adapter._validate_response(response, 2)
    response = _response()
    del response["model"]
    with pytest.raises(ValueError, match="returned model identity"):
        adapter._validate_response(response, 2)


def test_image_payload_must_match_a_supported_raster_type():
    adapter = _load_adapter()
    with pytest.raises(ValueError, match="supported raster"):
        adapter._image_media_type(b"<svg onload=alert(1)>", "image/svg+xml")
    with pytest.raises(ValueError, match="media type mismatch"):
        adapter._image_media_type(b"\x89PNG\r\n\x1a\nrest", "image/jpeg")
