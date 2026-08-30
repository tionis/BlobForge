import base64
import importlib.util
import json
import sys
from pathlib import Path

import pytest

from blobforge.mdaf import blake3_bytes
from blobforge.mdaf.digest import canonical_json_bytes


def _load_adapter():
    path = Path(__file__).parents[1] / "evaluators" / "datalab" / "adapter.py"
    spec = importlib.util.spec_from_file_location("blobforge_test_datalab_adapter", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_frozen_recipe_identity():
    path = (
        Path(__file__).parents[1]
        / "blobforge"
        / "recipes"
        / "datalab-convert-accurate-v1.json"
    )
    recipe = json.loads(path.read_text(encoding="utf-8"))
    assert blake3_bytes(canonical_json_bytes(recipe)) == (
        "blake3:c1dc8c06bf29a7a5f1639a4a0bdfc8be1250745d5f6e13438c68b1e38df9bc6f"
    )

    wiki_path = path.with_name("datalab-convert-accurate-wiki-v1.json")
    wiki_recipe = json.loads(wiki_path.read_text(encoding="utf-8"))
    assert blake3_bytes(canonical_json_bytes(wiki_recipe)) == (
        "blake3:fcc851f8e84d0c22e44200208ccd50d76319c5aec6d3bc1de6bc9b026d3ac502"
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
                    "max_cost_usd": 0.10,
                    "mode": "accurate",
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
    image = base64.b64encode(b"\x89PNG\r\n\x1a\nimage").decode("ascii")
    separator = lambda page: f"{{{page}}}" + "-" * 48 + "\n\n"
    return {
        "status": "complete",
        "success": True,
        "markdown": (
            separator(0)
            + "# Café\n\n![figure](../same.png)\n\n"
            + separator(1)
            + "Second page"
        ),
        "images": {"../same.png": image},
        "metadata": {"language": "en"},
        "page_count": 2,
        "parse_quality_score": 4.5,
        "cost_breakdown": {"list_cost_cents": 2.0, "final_cost_cents": 0.5},
    }


def test_success_is_captured_then_replayed_without_api_key(tmp_path, monkeypatch):
    adapter = _load_adapter()
    cache = tmp_path / "responses"
    monkeypatch.setenv("BLOBFORGE_DATALAB_RESPONSE_CACHE", str(cache))
    monkeypatch.setenv("DATALAB_API_KEY", "not-written-to-output")
    monkeypatch.setattr(adapter, "_page_count", lambda _source: 2)
    submissions = []
    polls = []
    monkeypatch.setattr(
        adapter,
        "_submit",
        lambda source, api_key, pages, mode: submissions.append(
            (source, api_key, pages, mode)
        )
        or {"request_check_url": "https://www.datalab.to/api/v1/convert/request-1"},
    )
    monkeypatch.setattr(
        adapter,
        "_poll",
        lambda url, api_key: polls.append((url, api_key)) or _response(),
    )

    first = tmp_path / "first"
    monkeypatch.setattr(sys, "argv", ["adapter", str(_request(tmp_path, first))])
    assert adapter.main() == 0
    assert len(submissions) == len(polls) == 1
    cache_entry = next(cache.glob("*/*.json"))
    assert cache_entry.stat().st_mode & 0o777 == 0o600
    assert "not-written-to-output" not in cache_entry.read_text(encoding="utf-8")

    monkeypatch.delenv("DATALAB_API_KEY")
    second = tmp_path / "second"
    monkeypatch.setattr(sys, "argv", ["adapter", str(_request(tmp_path, second))])
    assert adapter.main() == 0
    assert len(submissions) == len(polls) == 1
    assert (first / "data/text.md").read_bytes() == (second / "data/text.md").read_bytes()
    assert (first / "data/native/response.json").read_bytes() == (
        second / "data/native/response.json"
    ).read_bytes()

    text = (second / "data/text.md").read_text(encoding="utf-8")
    assert "assets/image-0000-same.png" in text
    assert "{0}" not in text and "-" * 48 not in text
    source_map = json.loads((second / "data/source-map.json").read_text(encoding="utf-8"))
    assert [item["source"]["selectors"][0]["start"] for item in source_map["mappings"]] == [0, 1]
    bundle = json.loads((second / "bundle.json").read_text(encoding="utf-8"))
    assert bundle["models"][0]["resolution"] == "mutable-alias"
    assert any("billed=$0.0050" in item["message"] for item in bundle["diagnostics"])


def test_provider_probe_reserves_ceiling_and_reports_returned_billing(tmp_path, monkeypatch):
    adapter = _load_adapter()
    cache = tmp_path / "responses"
    monkeypatch.setenv("BLOBFORGE_DATALAB_RESPONSE_CACHE", str(cache))
    monkeypatch.setenv("DATALAB_API_KEY", "test-key")
    monkeypatch.setattr(adapter, "_page_count", lambda _source: 2)
    submissions = []
    monkeypatch.setattr(
        adapter,
        "_submit",
        lambda *_args: submissions.append(True)
        or {"request_check_url": "https://www.datalab.to/api/v1/convert/quota"},
    )
    monkeypatch.setattr(adapter, "_poll", lambda *_args: _response())

    probe_output = tmp_path / "probe"
    probe_request = _request(
        tmp_path,
        probe_output,
        provider_account="datalab:test",
        quota_managed=True,
    )
    probe_value = json.loads(probe_request.read_text(encoding="utf-8"))
    probe_value["operation"] = "probe"
    probe_request.write_text(json.dumps(probe_value), encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["adapter", str(probe_request)])
    assert adapter.main() == 0
    probe = json.loads((probe_output / "probe.json").read_text(encoding="utf-8"))
    assert probe["cache_hit"] is False
    assert probe["requests"] == 1
    assert probe["pages"] == 2
    assert probe["estimated_micro_usd"] == 100_000
    assert probe["estimate_basis"] == "configured-per-job-ceiling"
    assert probe["currency"] == "USD"
    assert submissions == []

    output = tmp_path / "converted"
    request_path = _request(
        tmp_path,
        output,
        provider_account="datalab:test",
        quota_managed=True,
    )
    value = json.loads(request_path.read_text(encoding="utf-8"))
    report_path = tmp_path / "attempt.json"
    value.update(
        {
            "reservation_id": "qres_datalab",
            "attempt_report_path": str(report_path),
        }
    )
    request_path.write_text(json.dumps(value), encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["adapter", str(request_path)])
    assert adapter.main() == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["state"] == "committed"
    assert report["estimated_micro_usd"] == 100_000
    assert report["list_micro_usd"] == 20_000
    assert report["billed_micro_usd"] == 5_000
    assert report["credits_micro_usd"] == 15_000
    assert report["currency"] == "USD"
    envelope = json.loads(next(cache.glob("*/*.json")).read_text(encoding="utf-8"))
    assert envelope["reservation_id"] == "qres_datalab"


def test_wiki_profile_reuses_raw_cache_and_normalizes_tables(tmp_path, monkeypatch):
    adapter = _load_adapter()
    cache = tmp_path / "responses"
    monkeypatch.setenv("BLOBFORGE_DATALAB_RESPONSE_CACHE", str(cache))
    monkeypatch.setenv("DATALAB_API_KEY", "one-use-test-key")
    monkeypatch.setattr(adapter, "_page_count", lambda _source: 2)
    monkeypatch.setattr(adapter, "version", lambda _name: "6.14.2")
    response = _response()
    separator = lambda page: f"{{{page}}}" + "-" * 48 + "\n\n"
    response["markdown"] = (
        separator(0)
        + "![A hero](../same.png)\n\nA hero\n\n"
        + "| Name | | Value |\n| --- | --- | --- |\n| Ada | | 5 |\n\n"
        + separator(1)
        + "Second page"
    )
    submissions = []
    monkeypatch.setattr(
        adapter,
        "_submit",
        lambda *_args: submissions.append(True)
        or {"request_check_url": "https://www.datalab.to/api/v1/convert/request-2"},
    )
    monkeypatch.setattr(adapter, "_poll", lambda *_args: response)

    raw = tmp_path / "raw"
    monkeypatch.setattr(sys, "argv", ["adapter", str(_request(tmp_path, raw))])
    assert adapter.main() == 0
    assert submissions == [True]

    monkeypatch.delenv("DATALAB_API_KEY")
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
    assert submissions == [True]
    text = (wiki / "data/text.md").read_text(encoding="utf-8")
    assert text.count("A hero") == 1
    assert "<table>" in text and 'colspan="2"' in text
    bundle = json.loads((wiki / "bundle.json").read_text(encoding="utf-8"))
    assert bundle["markdown_features"] == ["raw-html", "semantic-html-table-v1"]
    assert bundle["additional_tools"] == [
        {"name": "pypdf", "version": "6.14.2"},
        {"name": "blobforge-wiki-normalizer", "version": "1.0.0"},
    ]
    assert bundle["parameters"]["provider_request_digest"] == "blake3:" + "a" * 64


def test_page_and_cost_guards_fail_closed(tmp_path, monkeypatch):
    adapter = _load_adapter()
    monkeypatch.setattr(adapter, "_page_count", lambda _source: 2)
    output = tmp_path / "page-output"
    monkeypatch.setattr(
        sys, "argv", ["adapter", str(_request(tmp_path, output, max_pages=1))]
    )
    with pytest.raises(ValueError, match="page ceiling"):
        adapter.main()

    with pytest.raises(ValueError, match="above ceiling"):
        adapter._validate_response(_response(), 2, 0.001)

    response_without_list_price = _response()
    del response_without_list_price["cost_breakdown"]["list_cost_cents"]
    _markdown, list_cents, final_cents = adapter._validate_response(
        response_without_list_price, 2, 0.10
    )
    assert list_cents is None
    assert final_cents == 0.5

    monkeypatch.setattr(adapter, "MAX_FILE_BYTES", 1)
    output = tmp_path / "size-output"
    monkeypatch.setattr(sys, "argv", ["adapter", str(_request(tmp_path, output))])
    with pytest.raises(ValueError, match="no larger"):
        adapter.main()


def test_delimiters_and_poll_url_must_be_exact():
    adapter = _load_adapter()
    with pytest.raises(ValueError, match="exactly cover"):
        adapter._split_pages("{1}" + "-" * 48 + "\n\nwrong", 2)
    with pytest.raises(ValueError, match="unsafe"):
        adapter._safe_check_url("https://attacker.invalid/api/v1/convert/id")


def test_cache_miss_requires_rights_before_provider(tmp_path, monkeypatch):
    adapter = _load_adapter()
    monkeypatch.setenv("BLOBFORGE_DATALAB_RESPONSE_CACHE", str(tmp_path / "cache"))
    monkeypatch.setenv("DATALAB_API_KEY", "must-not-be-used")
    monkeypatch.setattr(adapter, "_page_count", lambda _source: 2)
    monkeypatch.setattr(
        adapter, "_submit", lambda *_args: pytest.fail("provider must not be called")
    )
    output = tmp_path / "output"
    monkeypatch.setattr(
        sys,
        "argv",
        ["adapter", str(_request(tmp_path, output, api_rights_confirmed=False))],
    )
    with pytest.raises(ValueError, match="api_rights_confirmed"):
        adapter.main()
