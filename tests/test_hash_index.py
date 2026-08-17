import os
import time

from blobforge.hash_index import HashIndex, default_cache_dir, default_db_path


def test_default_paths_honor_cache_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("BLOBFORGE_CACHE_DIR", str(tmp_path / "custom"))
    assert default_db_path() == str(tmp_path / "custom" / "hash_index.sqlite3")
    assert os.path.isdir(os.path.dirname(default_db_path())) is False  # dir not created until use


def test_file_hash_roundtrip_and_invalidation(tmp_path):
    db = HashIndex(db_path=str(tmp_path / "index.sqlite3"))
    try:
        assert db.get_file_hash("/a.pdf", 100, 12345) is None
        db.set_file_hash("/a.pdf", 100, 12345, "hash1")
        assert db.get_file_hash("/a.pdf", 100, 12345) == "hash1"
        assert db.get_file_hash("/a.pdf", 101, 12345) is None  # size changed
        assert db.get_file_hash("/a.pdf", 100, 99999) is None  # mtime changed
        db.set_file_hash("/a.pdf", 100, 12345, "hash2")
        assert db.get_file_hash("/a.pdf", 100, 12345) == "hash2"  # overwrite
    finally:
        db.close()


def test_file_hashes_bulk_upsert(tmp_path):
    db = HashIndex(db_path=str(tmp_path / "index.sqlite3"))
    try:
        db.set_file_hashes([("/a.pdf", 10, 1, "ha"), ("/b.pdf", 20, 2, "hb")])
        assert db.get_file_hash("/a.pdf", 10, 1) == "ha"
        assert db.get_file_hash("/b.pdf", 20, 2) == "hb"
    finally:
        db.close()


def test_status_done_is_sticky(tmp_path):
    db = HashIndex(db_path=str(tmp_path / "index.sqlite3"))
    try:
        db.set_status("a" * 64, True)
        time.sleep(0.05)
        assert db.get_status("a" * 64, 0.01) is True  # done never expires
    finally:
        db.close()


def test_status_missing_expires_after_ttl(tmp_path):
    db = HashIndex(db_path=str(tmp_path / "index.sqlite3"))
    try:
        db.set_status("a" * 64, False)
        assert db.get_status("a" * 64, 3600) is False
        assert db.get_status("a" * 64, 0.0001) is None  # stale -> must re-query
    finally:
        db.close()


def test_status_unknown_returns_none(tmp_path):
    db = HashIndex(db_path=str(tmp_path / "index.sqlite3"))
    try:
        assert db.get_status("b" * 64, 3600) is None
    finally:
        db.close()


def test_set_statuses_batch_and_known_hashes(tmp_path):
    db = HashIndex(db_path=str(tmp_path / "index.sqlite3"))
    try:
        db.set_statuses({"a" * 64: True, "b" * 64: False})
        done, missing = db.known_hashes()
        assert done == {"a" * 64}
        assert missing == {"b" * 64}
    finally:
        db.close()