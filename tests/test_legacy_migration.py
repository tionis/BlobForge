import hashlib
import json
import zipfile

from blobforge.legacy_migration import convert_one, inventory, stage_v2, verify_outputs
from blobforge.local_import import import_legacy_sources, import_stage
from blobforge.mdaf import validate_mdaf
from blobforge.server.database import Database


def test_inventory_and_convert_legacy_archive(tmp_path):
    workspace = tmp_path / "migration"
    raw_root = workspace / "remote/pdf/store/raw"
    out_root = workspace / "remote/pdf/store/out"
    raw_root.mkdir(parents=True)
    out_root.mkdir(parents=True)

    source = b"%PDF-1.4\nsynthetic fixture\n%%EOF\n"
    sha256 = hashlib.sha256(source).hexdigest()
    (raw_root / f"{sha256}.pdf").write_bytes(source)
    markdown = (
        '<span id="page-0-0"></span>\n# Introduction\n\nCafé.\n'
        '# <img src="assets/divider.png">\n'
        '<span id="page-1-0"></span>\n## Rules\n\nText.\n'
    )
    info = {
        "original_filename": "example.pdf",
        "marker_meta": {
            "table_of_contents": [
                {
                    "title": "Introduction",
                    "page_id": 0,
                    "polygon": [[0, 0], [100, 0], [100, 20], [0, 20]],
                }
            ],
            "page_stats": [{"page_id": 0}, {"page_id": 1}],
        },
    }
    with zipfile.ZipFile(out_root / f"{sha256}.zip", "w") as archive:
        archive.writestr("content.md", markdown)
        archive.writestr("info.json", json.dumps(info))
        archive.writestr("assets/example.txt", "asset")

    summary = inventory(workspace)
    assert (summary.sources, summary.legacy_artifacts, summary.paired) == (1, 1, 1)

    output = convert_one(sha256, workspace)
    result = validate_mdaf(output)
    assert result.manifest["sources"][0]["alternate_digests"] == [f"sha256:{sha256}"]
    with zipfile.ZipFile(output) as archive:
        assert b"page-0-0" not in archive.read("text.md")
        source_map = json.loads(archive.read("source-map.json"))
        assert {mapping["source"]["selectors"][0]["start"] for mapping in source_map["mappings"]} == {0, 1}
        provenance = json.loads(archive.read("provenance.json"))
        assert provenance["activities"][0]["tools"][0]["version"] == "unavailable"

    verified = verify_outputs(workspace)
    assert (verified.checked, verified.valid, verified.errors) == (1, 1, ())

    staged = stage_v2(workspace)
    assert (staged.sources, staged.artifacts) == (1, 1)
    manifest = workspace / "staged-v2/store/v2/migrations/legacy-mdaf-v1/manifest.json"
    payload = json.loads(manifest.read_text())
    entry = payload["entries"][0]
    assert (workspace / "staged-v2" / entry["source_key"]).read_bytes() == source
    assert validate_mdaf(workspace / "staged-v2" / entry["artifact_key"])

    data_dir = tmp_path / "server-data"
    dry_run = import_stage(workspace / "staged-v2", data_dir)
    assert (dry_run.checked, dry_run.imported, dry_run.skipped) == (1, 0, 0)
    imported = import_stage(workspace / "staged-v2", data_dir, dry_run=False)
    assert (imported.checked, imported.imported, imported.skipped) == (1, 1, 0)
    repeated = import_stage(workspace / "staged-v2", data_dir, dry_run=False)
    assert (repeated.imported, repeated.skipped) == (0, 1)
    database = Database(data_dir / "blobforge.sqlite3", lease_seconds=900, max_retries=3)
    assert database.get_job(sha256)["digest_algorithm"] == "blake3"
    artifact_record = database.artifacts(sha256)[0]
    assert artifact_record["artifact_type"] == "mdaf/v1"
    assert artifact_record["legacy"] is True
    assert artifact_record["converter_backend"] == "marker"
    assert artifact_record["converter_version"] == "unavailable"
    assert artifact_record["provenance"]["mapping_strategy"] == "page-anchors-and-exact-toc-heading-alignment"


def test_import_unconverted_legacy_source(tmp_path):
    workspace = tmp_path / "migration"
    raw_root = workspace / "remote/pdf/store/raw"
    raw_root.mkdir(parents=True)
    source = b"%PDF-1.4\nunconverted fixture\n%%EOF\n"
    sha256 = hashlib.sha256(source).hexdigest()
    (raw_root / f"{sha256}.pdf").write_bytes(source)
    assert inventory(workspace).sources == 1

    data_dir = tmp_path / "server-data"
    dry_run = import_legacy_sources(workspace, data_dir)
    assert (dry_run.checked, dry_run.imported, dry_run.skipped) == (1, 0, 0)
    imported = import_legacy_sources(workspace, data_dir, dry_run=False)
    assert (imported.checked, imported.imported, imported.skipped) == (1, 1, 0)
    repeated = import_legacy_sources(workspace, data_dir, dry_run=False)
    assert (repeated.imported, repeated.skipped) == (0, 1)

    database = Database(data_dir / "blobforge.sqlite3", lease_seconds=900, max_retries=3)
    job = database.get_job(sha256)
    assert job["status"] == "todo"
    assert job["digest_algorithm"] == "blake3"
    assert "metadata-unavailable" in job["tags"]
