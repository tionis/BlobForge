import os

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


def test_done_set_defaults_empty(tmp_path):
    db = HashIndex(db_path=str(tmp_path / "index.sqlite3"))
    try:
        assert db.done_count() == 0
        assert db.is_done("a" * 64) is False
        assert db.get_watermark() == (0, "")
    finally:
        db.close()


def test_done_set_membership_and_bulk_insert(tmp_path):
    db = HashIndex(db_path=str(tmp_path / "index.sqlite3"))
    try:
        db.add_done_hashes(["a" * 64, "b" * 64, "a" * 64])  # dedup
        assert db.is_done("a" * 64) is True
        assert db.is_done("b" * 64) is True
        assert db.is_done("c" * 64) is False
        assert db.done_count() == 2
        assert set(db.done_hashes()) == {"a" * 64, "b" * 64}
    finally:
        db.close()


def test_watermark_roundtrip(tmp_path):
    db = HashIndex(db_path=str(tmp_path / "index.sqlite3"))
    try:
        db.set_watermark(1234567890, "a" * 64)
        assert db.get_watermark() == (1234567890, "a" * 64)
    finally:
        db.close()


def test_reset_done_set_clears_mirror_and_watermark(tmp_path):
    db = HashIndex(db_path=str(tmp_path / "index.sqlite3"))
    try:
        db.add_done_hashes(["a" * 64])
        db.set_watermark(5, "b" * 64)
        db.reset_done_set()
        assert db.done_count() == 0
        assert db.get_watermark() == (0, "")
    finally:
        db.close()


def test_drop_done_hash(tmp_path):
    db = HashIndex(db_path=str(tmp_path / "index.sqlite3"))
    try:
        db.add_done_hashes(["a" * 64, "b" * 64])
        db.drop_done_hash("a" * 64)
        assert db.is_done("a" * 64) is False
        assert db.is_done("b" * 64) is True
    finally:
        db.close()