"""CLI coverage for administrative source intake."""

import json
from types import SimpleNamespace

import pytest

from blobforge import cli


RECIPE = "blake3:" + "a" * 64


class FakeCoordinator:
    def __init__(self):
        self.timeout = None
        self.uploads = []

    def list_recipes(self, media_type=None):
        return [{
            "recipe_digest": RECIPE,
            "backend": "mistral-ocr-wiki",
            "display_name": "Mistral OCR wiki-v3",
            "enabled": True,
            "worker_count": 1,
            "input_kinds": ["source"],
            "media_types": [media_type],
        }]

    def upload_admin_source(self, local_path, **metadata):
        self.uploads.append((local_path, metadata))
        return {"hash": "b" * 64, "status": "todo"}


def arguments(tmp_path, **overrides):
    values = {
        "paths": [str(tmp_path)], "priority": "2_high",
        "tag": ["rulebook", "campaign,priority"], "media_type": None,
        "recipe": "mistral-ocr-wiki", "unassigned": False,
        "dry_run": False, "json": False, "timeout": 90.0,
        "coordinator_url": None, "token": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_upload_recurses_pdfs_resolves_backend_and_preserves_priority_tags(
    tmp_path, monkeypatch, capsys
):
    nested = tmp_path / "nested"
    nested.mkdir()
    first = tmp_path / "first.pdf"
    second = nested / "second.PDF"
    first.write_bytes(b"%PDF first")
    second.write_bytes(b"%PDF second")
    (tmp_path / "ignore.txt").write_text("not selected")
    coordinator = FakeCoordinator()
    monkeypatch.setattr(cli, "_coordinator_client", lambda: coordinator)

    assert cli.cmd_upload(arguments(tmp_path)) == 0

    assert coordinator.timeout == 90.0
    assert [item[0] for item in coordinator.uploads] == [str(first), str(second)]
    assert all(item[1]["recipe_digest"] == RECIPE for item in coordinator.uploads)
    assert all(item[1]["priority"] == "2_high" for item in coordinator.uploads)
    assert all(
        item[1]["tags"] == ["rulebook", "campaign", "priority"]
        for item in coordinator.uploads
    )
    assert "Uploaded 2/2 source(s)." in capsys.readouterr().out


def test_upload_dry_run_is_read_only_and_json_reports_exact_plan(
    tmp_path, monkeypatch, capsys
):
    (tmp_path / "book.pdf").write_bytes(b"%PDF")
    coordinator = FakeCoordinator()
    monkeypatch.setattr(cli, "_coordinator_client", lambda: coordinator)

    assert cli.cmd_upload(arguments(tmp_path, dry_run=True, json=True)) == 0

    assert coordinator.uploads == []
    payload = json.loads(capsys.readouterr().out)
    assert payload["failed"] == 0
    assert payload["files"][0]["recipe_digest"] == RECIPE
    assert payload["files"][0]["status"] == "planned"


def test_upload_can_explicitly_leave_job_unassigned(
    tmp_path, monkeypatch
):
    source = tmp_path / "book.pdf"
    source.write_bytes(b"%PDF")
    coordinator = FakeCoordinator()
    monkeypatch.setattr(cli, "_coordinator_client", lambda: coordinator)

    assert cli.cmd_upload(arguments(
        tmp_path, recipe=None, unassigned=True
    )) == 0

    assert coordinator.uploads[0][1]["recipe_digest"] is None


def test_upload_uses_self_hosted_priority_contract(monkeypatch):
    monkeypatch.setattr(
        "sys.argv", ["blobforge", "upload", "book.pdf", "--unassigned",
                     "--priority", "1_critical"]
    )
    with pytest.raises(SystemExit) as raised:
        cli.main()
    assert raised.value.code == 2
    assert cli.COORDINATOR_PRIORITIES == (
        "1_urgent", "2_high", "3_normal", "4_low"
    )
