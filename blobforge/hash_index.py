"""
Local persistent index for hydration caching.

Two caches live in one SQLite database (WAL mode):

1. File hashes keyed by (path, size, mtime_ns). This gives fast re-runs on any
   filesystem, including ones without extended-attribute support where the
   xattr-based hash cache silently misses and every file is re-read.

2. Done-status answers keyed by content hash, timestamped. This makes repeated
   hydration runs incremental: hashes that are known-done are never re-sent to
   the coordinator, and hashes that were previously missing are only re-queried
   after a TTL. New/changed files are the only real delta each run.
"""
import os
import sqlite3
import time
from typing import Dict, Optional, Tuple


def default_cache_dir() -> str:
    """Resolve the default cache directory, honoring XDG and BLOBFORGE_CACHE_DIR."""
    override = os.getenv("BLOBFORGE_CACHE_DIR")
    if override:
        return override
    xdg = os.getenv("XDG_CACHE_HOME")
    if xdg:
        return os.path.join(xdg, "blobforge")
    return os.path.join(os.path.expanduser("~"), ".cache", "blobforge")


def default_db_path() -> str:
    return os.path.join(default_cache_dir(), "hash_index.sqlite3")


class HashIndex:
    """SQLite-backed hash and status cache with nanosecond mtime precision."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or default_db_path()
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._conn = sqlite3.connect(self.db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS file_hashes (
                path      TEXT PRIMARY KEY,
                size      INTEGER NOT NULL,
                mtime_ns  INTEGER NOT NULL,
                hash      TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS hash_status (
                hash        TEXT PRIMARY KEY,
                done        INTEGER NOT NULL,
                checked_at  REAL NOT NULL
            );
            """
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    # ------------------------------------------------------------------
    # File hash cache
    # ------------------------------------------------------------------
    def get_file_hash(self, path: str, size: int, mtime_ns: int) -> Optional[str]:
        """Return the cached hash if (path, size, mtime_ns) match, else None."""
        row = self._conn.execute(
            "SELECT hash FROM file_hashes WHERE path=? AND size=? AND mtime_ns=?",
            (path, size, mtime_ns),
        ).fetchone()
        return row[0] if row else None

    def set_file_hash(self, path: str, size: int, mtime_ns: int, file_hash: str) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO file_hashes (path, size, mtime_ns, hash) VALUES (?,?,?,?)",
            (path, size, mtime_ns, file_hash),
        )
        self._conn.commit()

    def set_file_hashes(self, entries) -> None:
        """Bulk upsert of (path, size, mtime_ns, hash) tuples."""
        self._conn.executemany(
            "INSERT OR REPLACE INTO file_hashes (path, size, mtime_ns, hash) VALUES (?,?,?,?)",
            entries,
        )
        self._conn.commit()

    # ------------------------------------------------------------------
    # Done-status cache (incremental reconciliation)
    # ------------------------------------------------------------------
    def get_status(self, file_hash: str, missing_ttl_seconds: float) -> Optional[bool]:
        """
        Return the cached done-status for a hash.

        Returns:
            True  if the hash is known-done (content-addressed outputs are
                  immutable, so this never expires).
            False if the hash was previously missing and the answer is still
                  within missing_ttl_seconds.
            None  if unknown or stale (must be re-queried).
        """
        row = self._conn.execute(
            "SELECT done, checked_at FROM hash_status WHERE hash=?",
            (file_hash,),
        ).fetchone()
        if row is None:
            return None
        done, checked_at = row
        if done:
            return True
        if time.time() - checked_at < missing_ttl_seconds:
            return False
        return None

    def set_statuses(self, results: Dict[str, bool]) -> None:
        """Record done-status answers for a batch of hashes."""
        now = time.time()
        self._conn.executemany(
            "INSERT OR REPLACE INTO hash_status (hash, done, checked_at) VALUES (?,?,?)",
            [(file_hash, 1 if done else 0, now) for file_hash, done in results.items()],
        )
        self._conn.commit()

    def set_status(self, file_hash: str, done: bool) -> None:
        self.set_statuses({file_hash: done})

    def known_hashes(self) -> Tuple[set, set]:
        """Return (done_hashes, missing_hashes) currently cached, for tests."""
        done: set = set()
        missing: set = set()
        for row in self._conn.execute("SELECT hash, done FROM hash_status"):
            (done if row[1] else missing).add(row[0])
        return done, missing