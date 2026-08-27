import os
import sqlite3

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


def test_algorithm_specific_file_digests(tmp_path):
    db = HashIndex(db_path=str(tmp_path / "index.sqlite3"))
    try:
        db.set_file_hash("/a.pdf", 100, 12345, "sha", "sha256")
        db.set_file_hash("/a.pdf", 100, 12345, "b3", "blake3")
        assert db.get_file_hash("/a.pdf", 100, 12345, "sha256") == "sha"
        assert db.get_file_hash("/a.pdf", 100, 12345, "blake3") == "b3"
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


def test_done_sets_and_watermarks_are_scoped_by_coordinator(tmp_path):
    db = HashIndex(db_path=str(tmp_path / "index.sqlite3"))
    try:
        scope_a = "https://coordinator-a.example"
        scope_b = "https://coordinator-b.example"
        db.add_done_hashes(["a" * 64], scope_a)
        db.set_watermark(100, "a" * 64, scope_a)

        assert db.is_done("a" * 64, scope_a) is True
        assert db.is_done("a" * 64, scope_b) is False
        assert db.get_watermark(scope_a) == (100, "a" * 64)
        assert db.get_watermark(scope_b) == (0, "")

        db.add_done_hashes(["b" * 64], scope_b)
        db.reset_done_set(scope_a)
        assert db.done_count(scope_a) == 0
        assert db.done_hashes(scope_b) == ["b" * 64]
    finally:
        db.close()


def test_legacy_unscoped_done_mirror_is_discarded_safely(tmp_path):
    db_path = str(tmp_path / "index.sqlite3")
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE file_hashes (
            path TEXT PRIMARY KEY, size INTEGER NOT NULL,
            mtime_ns INTEGER NOT NULL, hash TEXT NOT NULL
        );
        CREATE TABLE done_hashes (hash TEXT PRIMARY KEY);
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        INSERT INTO file_hashes VALUES ('/book.pdf', 10, 1, 'file-hash');
        INSERT INTO done_hashes VALUES ('done-hash');
        INSERT INTO meta VALUES (
            'done_watermark',
            '{"version": 2, "since": 99, "cursor": "done-hash"}'
        );
        """
    )
    conn.commit()
    conn.close()

    db = HashIndex(db_path=db_path)
    try:
        columns = {
            row[1] for row in db._conn.execute("PRAGMA table_info(done_hashes)")
        }
        assert columns == {"scope", "hash"}
        assert db.done_count("https://coordinator.example") == 0
        assert db.get_watermark("https://coordinator.example") == (0, "")
        assert db.get_file_hash("/book.pdf", 10, 1) == "file-hash"
    finally:
        db.close()


def test_legacy_hash_status_table_is_dropped(tmp_path):
    db_path = str(tmp_path / "index.sqlite3")
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE hash_status (hash TEXT PRIMARY KEY, done INTEGER NOT NULL, checked_at REAL NOT NULL)")
    conn.execute("INSERT INTO hash_status VALUES ('a', 1, 0.0)")
    conn.commit()
    conn.close()

    db = HashIndex(db_path=db_path)
    try:
        tables = {
            row[0]
            for row in db._conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert "hash_status" not in tables
        assert "done_hashes" in tables
        assert "meta" in tables
    finally:
        db.close()
