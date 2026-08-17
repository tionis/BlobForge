import os

import pytest


@pytest.fixture(autouse=True)
def _isolate_hash_index(tmp_path, monkeypatch):
    """Point the persistent hash/status index at a per-test temp database."""
    monkeypatch.setenv("BLOBFORGE_HASH_INDEX_PATH", str(tmp_path / "hash_index.sqlite3"))