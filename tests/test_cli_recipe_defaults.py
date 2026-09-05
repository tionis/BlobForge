"""The promoted worker release is default; frozen releases remain selectable."""

import sys

import pytest

from blobforge import cli


@pytest.mark.parametrize("explicit", [None, "v3", "v4", "v5", "v6"])
def test_mistral_worker_release_selection(monkeypatch, explicit):
    captured = []
    monkeypatch.setattr(cli, "cmd_recipe_worker", lambda args: captured.append(args))
    arguments = ["blobforge", "recipe-worker", "--max-pages", "100",
                 "--max-cost-usd", "1.0", "--confirm-api-rights"]
    if explicit is not None:
        arguments.extend(["--mistral-recipe", explicit])
    monkeypatch.setattr(sys, "argv", arguments)
    with pytest.raises(SystemExit) as stopped:
        cli.main()
    assert stopped.value.code == 0
    assert captured[0].mistral_recipe == (explicit or "v6")
