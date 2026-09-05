import json
import os
import sys
import threading
import time
import zipfile
from pathlib import Path

import pytest

from blobforge.converters import (
    AdapterCancelled,
    ConverterExecutionError,
    run_converter,
)
from blobforge.mdaf import blake3_bytes, canonical_json_bytes
from blobforge.recipe_lifecycle import RECIPE_MEMBER_PATH


@pytest.mark.skipif(os.name != "posix", reason="process-group cancellation is POSIX")
def test_converter_cancellation_terminates_isolated_adapter(tmp_path):
    source = tmp_path / "source.pdf"
    source.write_bytes(b"%PDF synthetic")
    adapter = tmp_path / "slow-adapter.py"
    pid_path = tmp_path / "adapter.pid"
    adapter.write_text(
        "import os,pathlib,time\n"
        f"pathlib.Path({str(pid_path)!r}).write_text(str(os.getpid()))\n"
        "time.sleep(30)\n",
        encoding="utf-8",
    )
    cancel = threading.Event()

    def request_stop():
        deadline = time.monotonic() + 5
        while not pid_path.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        cancel.set()

    thread = threading.Thread(target=request_stop)
    thread.start()
    started = time.monotonic()
    with pytest.raises(AdapterCancelled):
        run_converter(
            [sys.executable, str(adapter)],
            source,
            tmp_path / "cancelled.mdaf",
            cancel_event=cancel,
        )
    thread.join(timeout=1)
    assert time.monotonic() - started < 5
    pid = int(pid_path.read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


def test_failed_converter_retains_provider_attempt_before_temp_cleanup(tmp_path):
    source = tmp_path / "source.pdf"
    source.write_bytes(b"%PDF synthetic")
    adapter = tmp_path / "failed-adapter.py"
    adapter.write_text(
        """
import json, pathlib, sys
request = json.loads(pathlib.Path(sys.argv[1]).read_text())
pathlib.Path(request["attempt_report_path"]).write_text(json.dumps({
  "contract": "dev.tionis.blobforge.provider-attempt/v1",
  "reservation_id": request["reservation_id"],
  "provider": "test-provider",
  "account_key": "test:primary",
  "currency": "USD",
  "checkpoint_key": "checkpoint:test",
  "state": "committed",
  "cache_hit": False,
  "requests": 1,
  "pages": 8,
  "estimated_micro_usd": 32000,
  "list_micro_usd": 32000,
  "billed_micro_usd": None,
  "credits_micro_usd": None
}), encoding="utf-8")
raise RuntimeError("post-provider packaging failure")
""",
        encoding="utf-8",
    )
    report = tmp_path / "provider-attempt.json"
    with pytest.raises(ConverterExecutionError) as raised:
        run_converter(
            [sys.executable, str(adapter)],
            source,
            tmp_path / "failed.mdaf",
            attempt_report_path=report,
            reservation_id="qres_test",
        )
    assert raised.value.provider_attempt is not None
    assert raised.value.provider_attempt["state"] == "committed"
    assert raised.value.provider_attempt["reservation_id"] == "qres_test"


@pytest.mark.parametrize("original_name,expected_name", [
    (None, "source.pdf"),
    ("Storypath Ultra.pdf", "Storypath Ultra.pdf"),
    ("/private/books/London Falling.pdf", "London Falling.pdf"),
    (r"C:\books\London Falling.pdf", "London Falling.pdf"),
    ("Cafe\u0301\n.pdf", "Café.pdf"),
    ("../", "source.pdf"),
])
def test_converter_bundle_is_packaged_by_shared_builder(tmp_path, original_name, expected_name):
    source = tmp_path / "source.pdf"
    source.write_bytes(b"%PDF synthetic")
    adapter = tmp_path / "adapter.py"
    adapter.write_text(
        """
import json, pathlib, sys
request = json.loads(pathlib.Path(sys.argv[1]).read_text())
root = pathlib.Path(request["output_dir"])
(root / "data").mkdir(parents=True)
(root / "data/text.md").write_text("# Adapter output\\n", encoding="utf-8")
(root / "data/native.json").write_text("{}\\n", encoding="utf-8")
(root / "bundle.json").write_text(json.dumps({
  "contract": "dev.tionis.blobforge.converter-bundle/v1",
  "text_path": "data/text.md",
  "members": [{
    "path": "renditions/example.test/native.json",
    "file": "data/native.json",
    "role": "rendition",
    "media_type": "application/json",
    "namespace": "example.test"
  }],
  "tool": {"name": "fake", "version": "1.0.0"},
  "additional_tools": [{"name": "normalizer", "version": "2.0.0"}],
  "markdown_features": ["raw-html", "semantic-html-table-v1"],
  "models": [],
  "parameters": {"quality": "test"},
  "diagnostics": []
}), encoding="utf-8")
""",
        encoding="utf-8",
    )
    output = tmp_path / "fake.mdaf"
    result = run_converter(
        [sys.executable, str(adapter)], source, output, original_name=original_name
    )
    assert result.artifact_path == output
    with zipfile.ZipFile(output) as archive:
        manifest = json.loads(archive.read("info.json"))
        assert manifest["title"] == Path(expected_name).stem
        assert manifest["sources"][0]["name"] == expected_name
        assert manifest["sources"][0]["digest"].startswith("blake3:")
        assert "native-renditions" in manifest["capabilities"]
        assert "outline" in manifest["capabilities"]
        assert manifest["markdown"]["features"] == [
            "raw-html",
            "semantic-html-table-v1",
        ]
        provenance = json.loads(archive.read("provenance.json"))
        assert provenance["activities"][0]["tools"] == [
            {"name": "fake", "version": "1.0.0"},
            {"name": "normalizer", "version": "2.0.0"},
        ]
        outline = json.loads(archive.read("outline.json"))
        assert outline["nodes"][0]["title"] == "Adapter output"


def test_lifecycle_recipe_is_embedded_and_splits_provenance_stages(tmp_path):
    recipe_path = (
        Path(__file__).parents[1]
        / "blobforge"
        / "recipes"
        / "mistral-ocr-4.1-wiki-v3.json"
    )
    recipe = json.loads(recipe_path.read_text(encoding="utf-8"))
    digest = blake3_bytes(canonical_json_bytes(recipe))
    source = tmp_path / "source.pdf"
    source.write_bytes(b"%PDF synthetic")
    adapter = tmp_path / "adapter.py"
    adapter.write_text(
        f"""
import json, pathlib, sys
request = json.loads(pathlib.Path(sys.argv[1]).read_text())
root = pathlib.Path(request["output_dir"])
(root / "data").mkdir(parents=True)
(root / "data/text.md").write_text("# Reprocessed output\\n", encoding="utf-8")
(root / "data/response.json").write_text("{{}}\\n", encoding="utf-8")
(root / "bundle.json").write_text(json.dumps({{
  "contract": "dev.tionis.blobforge.converter-bundle/v1",
  "text_path": "data/text.md",
  "members": [{{
    "path": "renditions/ai.mistral/ocr-response.json",
    "file": "data/response.json",
    "role": "rendition",
    "media_type": "application/json",
    "namespace": "ai.mistral"
  }}],
  "tool": {{"name": "extractor", "version": "1.0.0"}},
  "additional_tools": [{{"name": "normalizer", "version": "2.0.0"}}],
  "models": [],
  "parameters": {{"recipe_digest": "{digest}", "normalization_profile": "wiki-v2"}},
  "diagnostics": []
}}), encoding="utf-8")
""",
        encoding="utf-8",
    )
    output = tmp_path / "lifecycle.mdaf"
    run_converter(
        [sys.executable, str(adapter)],
        source,
        output,
        recipe=recipe,
    )
    with zipfile.ZipFile(output) as archive:
        assert archive.read(RECIPE_MEMBER_PATH) == canonical_json_bytes(recipe)
        manifest = json.loads(archive.read("info.json"))
        assert "extensions" in manifest["capabilities"]
        members = {value["path"]: value for value in manifest["members"]}
        assert members["renditions/ai.mistral/ocr-response.json"]["created_by"] == (
            "activity:extract"
        )
        assert members["text.md"]["created_by"] == "activity:postprocess"
        provenance = json.loads(archive.read("provenance.json"))
        assert [value["id"] for value in provenance["activities"]] == [
            "activity:extract",
            "activity:postprocess",
        ]
        assert provenance["activities"][1]["depends_on"] == ["activity:extract"]
