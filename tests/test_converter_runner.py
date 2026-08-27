import json
import sys
import zipfile

from blobforge.converters import run_converter


def test_converter_bundle_is_packaged_by_shared_builder(tmp_path):
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
  "models": [],
  "parameters": {"quality": "test"},
  "diagnostics": []
}), encoding="utf-8")
""",
        encoding="utf-8",
    )
    output = tmp_path / "fake.mdaf"
    result = run_converter([sys.executable, str(adapter)], source, output)
    assert result.artifact_path == output
    with zipfile.ZipFile(output) as archive:
        manifest = json.loads(archive.read("info.json"))
        assert manifest["sources"][0]["digest"].startswith("blake3:")
        assert "native-renditions" in manifest["capabilities"]
        assert "outline" in manifest["capabilities"]
        outline = json.loads(archive.read("outline.json"))
        assert outline["nodes"][0]["title"] == "Adapter output"
