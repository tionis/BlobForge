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
    assert coordinator.downloads == [(HASH, str(target), RECIPE)]


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
