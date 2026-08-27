import json

from blobforge.corpus import build_manifest
from blobforge.mdaf.digest import blake3_bytes, canonical_json_bytes


def test_empty_corpus_manifest_has_stable_content_identity(tmp_path):
    root = tmp_path / "corpus"
    root.mkdir()
    result = build_manifest(root, tmp_path / "manifest.json")
    manifest = json.loads(result.path.read_text())
    body = {key: manifest[key] for key in ("format", "version", "documents")}
    assert result.digest == blake3_bytes(canonical_json_bytes(body))
    assert result.documents == result.pages == result.bytes == 0
