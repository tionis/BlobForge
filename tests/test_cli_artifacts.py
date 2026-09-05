"""CLI coverage for recipe-aware conversion artifact operations."""

import json
from argparse import ArgumentTypeError
from pathlib import Path
from types import SimpleNamespace

import pytest

from blobforge import cli

HASH = "a" * 64
RECIPE = "b" * 64


class FakeCoordinator:
    def __init__(self):
        self.downloads = []
        self.requests = []

    def get_job(self, _hash):
        return {"hash": HASH, "status": "todo", "recipe_digest": RECIPE}

    def list_artifacts(self, _hash):
        return [
            {
                "recipe_digest": RECIPE,
                "recipe": {"engine": "marker", "engine_generation": "1"},
                "provenance": {"packages": {"marker-pdf": "1.10.2"}},
                "worker_id": "worker-1",
                "output_size_bytes": 1234,
                "created_at": 1_700_000_000_000,
            }
        ]

    def download_output(self, file_hash, local_path, recipe_digest=None):
        self.downloads.append((file_hash, local_path, recipe_digest))
        Path(local_path).write_bytes(b"zip")

    def request_conversion(self, file_hash, recipe_digest):
        self.requests.append((file_hash, recipe_digest))
        return {"status": "selected"}


@pytest.fixture
def coordinator(monkeypatch):
    instance = FakeCoordinator()
    monkeypatch.setattr(cli, "_coordinator_client", lambda: instance)
    return instance


def args(**values):
    return SimpleNamespace(coordinator_url=None, token=None, **values)


def test_download_by_filename_directory_and_json(coordinator, tmp_path, capsys):
    coordinator.list_jobs = lambda **kwargs: {"jobs": [{"hash": HASH, "original_name": "Über Rules.pdf", "recipe_digest": RECIPE}], "total": 1}
    coordinator.list_artifacts = lambda key: [{"recipe_digest": RECIPE, "artifact_type": "mdaf/v1"}]
    options = args(hash="Über Rules.pdf", output=str(tmp_path), recipe_digest=None, mdaf=True, json=True)
    assert cli.cmd_download(options) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["downloads"][0]["output"] == str(tmp_path / "Über Rules.mdaf")
    assert (tmp_path / "Über Rules.mdaf").read_bytes() == b"zip"
    assert cli.cmd_download(options) == 1
    assert "already exists" in json.loads(capsys.readouterr().out)["error"]
    assert len(coordinator.downloads) == 1


def test_bulk_mdaf_preview_skips_legacy_and_does_not_convert(coordinator, tmp_path, capsys):
    coordinator.list_jobs = lambda **kwargs: {"jobs": [
        {"hash": HASH, "original_name": "Book.pdf", "recipe_digest": RECIPE},
        {"hash": "c" * 64, "original_name": "Legacy.pdf"}], "total": 2}
    coordinator.list_artifacts = lambda key: [{"recipe_digest": RECIPE, "artifact_type": "mdaf/v1" if key == HASH else "legacy-archive"}]
    assert cli.cmd_download(args(hash=None, search="book", output=str(tmp_path), recipe_digest=None,
                                 mdaf=True, dry_run=True, json=True)) == 0
    payload = json.loads(capsys.readouterr().out)
    assert len(payload["downloads"]) == len(payload["skipped"]) == 1
    assert payload["dry_run"] is True
    assert coordinator.downloads == coordinator.requests == []
    assert list(tmp_path.iterdir()) == []


def test_name_ambiguity_and_bulk_collisions_fail_before_writes(coordinator, tmp_path, capsys):
    coordinator.list_jobs = lambda **kwargs: {"jobs": [
        {"hash": HASH, "original_name": "Book.pdf"},
        {"hash": "c" * 64, "original_name": "Book.pdf"}], "total": 2}
    assert cli.cmd_download(args(hash="Book.pdf", output=str(tmp_path), recipe_digest=None)) == 1
    assert "Ambiguous source" in capsys.readouterr().out
    assert cli.cmd_download(args(hash=None, search="Book", output=str(tmp_path), recipe_digest=None)) == 1
    assert "Multiple sources" in capsys.readouterr().out
    assert coordinator.downloads == []


def test_partial_download_cleanup_force_and_race_protection(coordinator, tmp_path):
    from blobforge.downloads import download_one
    target = tmp_path / "book.mdaf"
    target.write_bytes(b"keep")
    plan = {"hash": HASH, "recipe_digest": RECIPE, "output": str(target)}
    with pytest.raises(FileExistsError):
        download_one(coordinator, plan)
    assert target.read_bytes() == b"keep"
    def fail(key, path, recipe):
        Path(path).write_bytes(b"partial")
        raise RuntimeError("interrupted")
    coordinator.download_output = fail
    with pytest.raises(RuntimeError):
        download_one(coordinator, plan, force=True)
    assert target.read_bytes() == b"keep"
    assert list(tmp_path.iterdir()) == [target]
    coordinator.download_output = lambda key, path, recipe: Path(path).write_bytes(b"complete")
    download_one(coordinator, plan, force=True)
    assert target.read_bytes() == b"complete"


def test_filename_lookup_paginates_and_prefers_exact_match(coordinator):
    from blobforge.downloads import resolve_sources
    calls = []
    def listing(**kwargs):
        calls.append(kwargs)
        return {"total": 2, "jobs": [{"hash": str(kwargs["offset"]),
                 "original_name": "Book.pdf" if kwargs["offset"] else "Book supplement.pdf"}]}
    coordinator.list_jobs = listing
    assert resolve_sources(coordinator, "Book.pdf")[0]["hash"] == "1"
    assert [call["offset"] for call in calls] == [0, 1]


def test_recipe_digest_argument_is_normalized_and_validated():
    assert cli._recipe_digest_arg("B" * 64) == RECIPE
    assert cli._recipe_digest_arg("BLAKE3:" + "B" * 64) == "blake3:" + RECIPE
    with pytest.raises(ArgumentTypeError, match="64 hexadecimal"):
        cli._recipe_digest_arg("not-a-digest")
    with pytest.raises(ArgumentTypeError, match="64 hexadecimal"):
        cli._recipe_digest_arg("sha256:" + "b" * 64)


def test_artifacts_json_includes_selected_recipe(coordinator, capsys):
    result = cli.cmd_artifacts(args(hash=HASH, json=True))

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["selected_recipe_digest"] == RECIPE
    assert payload["artifacts"][0]["recipe_digest"] == RECIPE


def test_human_artifact_listing_uses_explicit_legacy_catalog_fields(coordinator, capsys):
    coordinator.list_artifacts = lambda _hash: [{
        "recipe_digest": "0" * 64,
        "legacy": True,
        "converter_backend": "marker",
        "converter_version": "unavailable",
        "created_at": 1_700_000_000_000,
    }]
    assert cli.cmd_artifacts(args(hash=HASH, json=False)) == 0
    output = capsys.readouterr().out
    assert "legacy" in output
    assert "Engine: marker" in output
    assert "Converter version: unavailable" in output


def test_recipe_specific_download_works_while_another_recipe_is_queued(
    coordinator, tmp_path
):
    target = tmp_path / "artifact.zip"

    result = cli.cmd_download(
        args(hash=HASH, output=str(target), recipe_digest=RECIPE)
    )

    assert result == 0
    assert target.read_bytes() == b"zip"
    assert coordinator.downloads[0][0] == HASH
    assert coordinator.downloads[0][2] == RECIPE
    assert Path(coordinator.downloads[0][1]).parent == target.parent
    assert not Path(coordinator.downloads[0][1]).exists()


def test_request_conversion_dry_run_does_not_mutate(coordinator, capsys):
    result = cli.cmd_request_conversion(
        args(hash=HASH, recipe_digest=RECIPE, dry_run=True)
    )

    assert result == 0
    assert coordinator.requests == []
    assert "Would select retained artifact" in capsys.readouterr().out


def test_request_conversion_selects_or_queues_exact_recipe(coordinator, capsys):
    result = cli.cmd_request_conversion(
        args(hash=HASH, recipe_digest=RECIPE, dry_run=False)
    )

    assert result == 0
    assert coordinator.requests == [(HASH, RECIPE)]
    assert "selected" in capsys.readouterr().out
