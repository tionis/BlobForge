"""
Local persistent index for hydration caching.

Two caches live in one SQLite database (WAL mode):

1. File hashes keyed by (path, size, mtime_ns). This gives fast re-runs on any
   filesystem, including ones without extended-attribute support where the
   xattr-based hash cache silently misses and every file is re-read.

2. A mirror of the coordinator's done-set, reconciled incrementally with a
   watermark: each run pulls only hashes completed since the last sync and
   merges them locally. Membership of a local hash is then answered from the
   mirror without re-sending the candidate set to the coordinator. Content-
   addressed outputs are immutable, so done hashes never need re-querying.
"""
import json
import os
import sqlite3
from typing import Dict, List, Optional, Tuple


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
    """SQLite-backed file-hash cache and done-set mirror with a sync watermark."""

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
            CREATE TABLE IF NOT EXISTS done_hashes (
                hash TEXT PRIMARY KEY
            );
            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            DROP TABLE IF EXISTS hash_status;
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
    # Done-set mirror (incremental reconciliation via watermark)
    # ------------------------------------------------------------------
    def get_watermark(self) -> Tuple[int, str]:
        """Return the (since_ms, cursor) watermark from the last completed sync.

        The coordinator's done-sync protocol now pages over a strictly monotonic
        ``done_seq`` and resumes strictly after the previous ``since``. Watermarks
        written by older clients (inclusive timestamp + cursor tie-break) are not
        forward-compatible, so they are treated as absent and force a full resync.
        """
        row = self._conn.execute("SELECT value FROM meta WHERE key='done_watermark'").fetchone()
        if row is None:
            return 0, ""
        try:
            parsed = json.loads(row[0])
            if parsed.get("version") != 2:
                return 0, ""
            since = int(parsed.get("since", 0))
            cursor = str(parsed.get("cursor", ""))
            return since, cursor
        except (ValueError, TypeError, AttributeError):
            return 0, ""

    def set_watermark(self, since_ms: int, cursor: str) -> None:
        value = json.dumps({"version": 2, "since": int(since_ms), "cursor": cursor or ""})
        self._conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('done_watermark', ?)",
            (value,),
        )
        self._conn.commit()

    def reset_done_set(self) -> None:
        """Clear the done mirror and watermark, forcing a full re-sync next run."""
        self._conn.execute("DELETE FROM done_hashes")
        self._conn.execute("DELETE FROM meta WHERE key='done_watermark'")
        self._conn.commit()

    def add_done_hashes(self, hashes) -> None:
        """Bulk-insert newly-synced done hashes (deduplicated)."""
        self._conn.executemany(
            "INSERT OR IGNORE INTO done_hashes (hash) VALUES (?)",
            [(h,) for h in hashes],
        )
        self._conn.commit()

    def drop_done_hash(self, file_hash: str) -> None:
        """Remove a hash from the mirror (e.g. its output is no longer available)."""
        self._conn.execute("DELETE FROM done_hashes WHERE hash=?", (file_hash,))
        self._conn.commit()

    def is_done(self, file_hash: str) -> bool:
        """Return True if the hash is in the local done-set mirror."""
        row = self._conn.execute("SELECT 1 FROM done_hashes WHERE hash=?", (file_hash,)).fetchone()
        return row is not None

    def done_count(self) -> int:
        row = self._conn.execute("SELECT COUNT(*) FROM done_hashes").fetchone()
        return int(row[0]) if row else 0

    def done_hashes(self) -> List[str]:
        return [row[0] for row in self._conn.execute("SELECT hash FROM done_hashes")]

    def file_hashes(self) -> Dict[str, str]:
        return {path: file_hash for path, file_hash in self._conn.execute("SELECT path, hash FROM file_hashes")}