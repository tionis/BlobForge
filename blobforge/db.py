"""
BlobForge DatabaseClient - PostgreSQL backend for metadata and queue state.

S3 remains the authoritative blob store (raw PDFs, conversion output zips).
This module manages:
  - File manifest (files, paths, tags)
  - Job queue state (todo, processing, failed, dead, done)
  - Worker registration and heartbeats
  - Job logs and error details
  - Remote configuration

When BLOBFORGE_DATABASE_URL is set, the DatabaseClient is preferred for all
metadata and queue operations. The S3Client continues to handle blob I/O.
"""

import json
import logging
import os
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from .config import PRIORITIES, DEFAULT_PRIORITY, WORKER_ID, get_worker_metadata

logger = logging.getLogger(__name__)

try:
    import psycopg2
    import psycopg2.extras

    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

DATABASE_URL = os.getenv("BLOBFORGE_DATABASE_URL", "")
DATABASE_ROLE = os.getenv("BLOBFORGE_ROLE", "operator")

MIGRATIONS_DIR = os.path.join(os.path.dirname(__file__), "migrations")

ACTIONS = {
    "operator": {
        "ingest",
        "retry",
        "cancel",
        "remove",
        "config",
        "migrate",
        "dashboard",
        "status",
        "search",
        "lookup",
        "manifest",
        "hydrate",
        "repair_metadata",
        "reset_status",
    },
    "worker": {
        "claim_job",
        "heartbeat",
        "complete_job",
        "fail_job",
        "register_worker",
        "deregister_worker",
    },
    "viewer": {
        "dashboard",
        "status",
        "search",
        "lookup",
        "manifest",
    },
}


def check_permission(role: str, action: str) -> bool:
    return action in ACTIONS.get(role, set())


class DatabaseClient:
    """
    PostgreSQL client for BlobForge metadata and queue operations.

    Falls back to no-op behaviour when psycopg2 is unavailable or
    BLOBFORGE_DATABASE_URL is not configured.
    """

    def __init__(self, database_url: Optional[str] = None):
        self.database_url = database_url or DATABASE_URL
        self._pool = None
        self._connected = False

        if not HAS_PSYCOPG2:
            logger.debug("psycopg2 not available; database client disabled")
            return

        if not self.database_url:
            logger.debug("BLOBFORGE_DATABASE_URL not set; database client disabled")
            return

        self._connect()

    def _connect(self):
        if not HAS_PSYCOPG2 or not self.database_url:
            return
        try:
            from psycopg2 import pool

            self._pool = pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=8,
                dsn=self.database_url,
            )
            self._connected = True
            logger.info("Database connection pool established")
        except Exception as e:
            logger.warning(f"Failed to create database connection pool: {e}")
            self._connected = False

    @property
    def available(self) -> bool:
        return self._connected and self._pool is not None

    @contextmanager
    def _cursor(self, commit: bool = True):
        if not self.available:
            raise RuntimeError("Database not available")
        conn = self._pool.getconn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            try:
                yield cur
                if commit:
                    conn.commit()
            finally:
                cur.close()
        except Exception:
            if commit:
                conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)

    # ------------------------------------------------------------------
    # Migrations
    # ------------------------------------------------------------------

    def current_schema_version(self) -> int:
        if not self.available:
            return 0
        with self._cursor() as cur:
            cur.execute(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
                "WHERE table_name = 'schema_migrations')"
            )
            if not cur.fetchone()["exists"]:
                return 0
            cur.execute("SELECT COALESCE(MAX(version), 0) AS v FROM schema_migrations")
            return cur.fetchone()["v"]

    def run_migrations(self):
        if not self.available:
            logger.warning("Database not available; skipping migrations")
            return

        current = self.current_schema_version()

        migration_files = sorted(
            f
            for f in os.listdir(MIGRATIONS_DIR)
            if f.endswith(".sql") and f[:-4].split("_", 1)[0].isdigit()
        )

        for mf in migration_files:
            version = int(mf[:-4].split("_", 1)[0])
            if version <= current:
                continue
            path = os.path.join(MIGRATIONS_DIR, mf)
            with open(path, "r") as f:
                sql = f.read()
            logger.info(f"Running migration {mf}...")
            with self._cursor() as cur:
                cur.execute(sql)
            logger.info(f"Migration {mf} applied")

    def init_db(self):
        """Initialize database: run all pending migrations."""
        self.run_migrations()

    # ------------------------------------------------------------------
    # Files (Manifest)
    # ------------------------------------------------------------------

    def upsert_file(
        self, file_hash: str, size_bytes: int = 0, source: Optional[str] = None
    ) -> bool:
        if not self.available:
            return False
        with self._cursor() as cur:
            cur.execute(
                "INSERT INTO files (hash, size_bytes, source) "
                "VALUES (%s, %s, %s) "
                "ON CONFLICT (hash) DO UPDATE SET "
                "size_bytes = EXCLUDED.size_bytes, "
                "source = COALESCE(EXCLUDED.source, files.source)",
                (file_hash, size_bytes, source),
            )
        return True

    def add_file_path(self, file_hash: str, path: str) -> bool:
        if not self.available:
            return False
        with self._cursor() as cur:
            cur.execute(
                "INSERT INTO file_paths (file_hash, path) "
                "VALUES (%s, %s) ON CONFLICT (path) DO NOTHING",
                (file_hash, path),
            )
        return True

    def add_file_tags(self, file_hash: str, tags: List[str]) -> bool:
        if not self.available:
            return False
        with self._cursor() as cur:
            for tag in tags:
                cur.execute(
                    "INSERT INTO file_tags (file_hash, tag) "
                    "VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (file_hash, tag),
                )
        return True

    def ingest_file(
        self,
        file_hash: str,
        paths: List[str],
        size_bytes: int = 0,
        tags: Optional[List[str]] = None,
        source: Optional[str] = None,
    ) -> bool:
        """Add or update a file entry with paths and tags (atomic)."""
        if not self.available:
            return False
        with self._cursor() as cur:
            cur.execute(
                "INSERT INTO files (hash, size_bytes, source) "
                "VALUES (%s, %s, %s) "
                "ON CONFLICT (hash) DO UPDATE SET "
                "size_bytes = EXCLUDED.size_bytes, "
                "source = COALESCE(EXCLUDED.source, files.source)",
                (file_hash, size_bytes, source),
            )
            for path in paths:
                cur.execute(
                    "INSERT INTO file_paths (file_hash, path) "
                    "VALUES (%s, %s) ON CONFLICT (path) DO NOTHING",
                    (file_hash, path),
                )
            if tags:
                for tag in tags:
                    cur.execute(
                        "INSERT INTO file_tags (file_hash, tag) "
                        "VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (file_hash, tag),
                    )
        return True

    def lookup_by_hash(self, file_hash: str) -> Optional[Dict[str, Any]]:
        if not self.available:
            return None
        with self._cursor(commit=False) as cur:
            cur.execute("SELECT * FROM files WHERE hash = %s", (file_hash,))
            file_row = cur.fetchone()
            if not file_row:
                return None
            cur.execute(
                "SELECT path FROM file_paths WHERE file_hash = %s", (file_hash,)
            )
            paths = [r["path"] for r in cur.fetchall()]
            cur.execute("SELECT tag FROM file_tags WHERE file_hash = %s", (file_hash,))
            tags = [r["tag"] for r in cur.fetchall()]
            return {
                "hash": file_row["hash"],
                "paths": paths,
                "tags": tags,
                "size": file_row["size_bytes"],
                "source": file_row["source"],
                "ingested_at": file_row["ingested_at"].isoformat()
                if file_row["ingested_at"]
                else None,
            }

    def lookup_by_path(self, path: str) -> Optional[str]:
        if not self.available:
            return None
        with self._cursor(commit=False) as cur:
            cur.execute("SELECT file_hash FROM file_paths WHERE path = %s", (path,))
            row = cur.fetchone()
            return row["file_hash"] if row else None

    def search_manifest(self, query: str, limit: int = 100) -> List[Dict[str, Any]]:
        if not self.available:
            return []
        pattern = f"%{query}%"
        with self._cursor(commit=False) as cur:
            cur.execute(
                "SELECT DISTINCT f.hash, f.size_bytes, f.source, f.ingested_at "
                "FROM files f "
                "LEFT JOIN file_paths fp ON f.hash = fp.file_hash "
                "LEFT JOIN file_tags ft ON f.hash = ft.file_hash "
                "WHERE fp.path ILIKE %s OR ft.tag ILIKE %s "
                "ORDER BY f.ingested_at DESC LIMIT %s",
                (pattern, pattern, limit),
            )
            results = []
            for row in cur.fetchall():
                file_hash = row["hash"]
                cur2 = cur.connection.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                )
                cur2.execute(
                    "SELECT path FROM file_paths WHERE file_hash = %s", (file_hash,)
                )
                paths = [r["path"] for r in cur2.fetchall()]
                cur2.execute(
                    "SELECT tag FROM file_tags WHERE file_hash = %s", (file_hash,)
                )
                tags = [r["tag"] for r in cur2.fetchall()]
                cur2.close()
                results.append(
                    {
                        "hash": file_hash,
                        "paths": paths,
                        "tags": tags,
                        "size": row["size_bytes"],
                        "source": row["source"],
                        "ingested_at": row["ingested_at"].isoformat()
                        if row["ingested_at"]
                        else None,
                    }
                )
            return results

    def remove_file(self, file_hash: str) -> bool:
        if not self.available:
            return False
        with self._cursor() as cur:
            cur.execute("DELETE FROM files WHERE hash = %s", (file_hash,))
        return True

    def file_count(self) -> int:
        if not self.available:
            return 0
        with self._cursor(commit=False) as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM files")
            return cur.fetchone()["cnt"]

    # ------------------------------------------------------------------
    # Jobs (Queue State)
    # ------------------------------------------------------------------

    def create_job(self, file_hash: str, priority: str = DEFAULT_PRIORITY) -> bool:
        """Create a todo job for a file. No-op if job already exists."""
        if not self.available:
            return False
        with self._cursor() as cur:
            cur.execute(
                "INSERT INTO jobs (file_hash, status, priority) "
                "VALUES (%s, 'todo', %s) "
                "ON CONFLICT (file_hash) DO NOTHING",
                (file_hash, priority),
            )
            return cur.rowcount > 0
        return False

    def claim_job(
        self, worker_id: str, priority: Optional[str] = None
    ) -> Optional[str]:
        """
        Atomically claim the next todo job.

        Uses SELECT ... FOR UPDATE SKIP LOCKED to safely distribute
        work across concurrent workers without race conditions.
        """
        if not self.available:
            return None

        with self._cursor() as cur:
            if priority:
                priorities = [priority]
            else:
                priorities = list(PRIORITIES)

            for prio in priorities:
                cur.execute(
                    "SELECT file_hash FROM jobs "
                    "WHERE status = 'todo' AND priority = %s "
                    "ORDER BY created_at ASC "
                    "FOR UPDATE SKIP LOCKED LIMIT 1",
                    (prio,),
                )
                row = cur.fetchone()
                if row:
                    file_hash = row["file_hash"]
                    cur.execute(
                        "UPDATE jobs SET status = 'processing', worker_id = %s, "
                        "started_at = NOW(), updated_at = NOW(), progress = '{}' "
                        "WHERE file_hash = %s AND status = 'todo' "
                        "RETURNING file_hash",
                        (worker_id, file_hash),
                    )
                    result = cur.fetchone()
                    if result:
                        return result["file_hash"]
        return None

    def complete_job(self, file_hash: str) -> bool:
        if not self.available:
            return False
        with self._cursor() as cur:
            cur.execute(
                "UPDATE jobs SET status = 'done', completed_at = NOW(), "
                "updated_at = NOW(), progress = '{}' WHERE file_hash = %s",
                (file_hash,),
            )
            return cur.rowcount > 0

    def fail_job(
        self,
        file_hash: str,
        error_message: str,
        worker_id: Optional[str] = None,
        retry_count: Optional[int] = None,
    ) -> bool:
        if not self.available:
            return False
        with self._cursor() as cur:
            if retry_count is not None:
                cur.execute(
                    "UPDATE jobs SET status = 'failed', error_message = %s, "
                    "worker_id = %s, retry_count = %s, updated_at = NOW() "
                    "WHERE file_hash = %s",
                    (error_message, worker_id, retry_count, file_hash),
                )
            else:
                cur.execute(
                    "UPDATE jobs SET status = 'failed', error_message = %s, "
                    "worker_id = %s, retry_count = retry_count + 1, updated_at = NOW() "
                    "WHERE file_hash = %s",
                    (error_message, worker_id, file_hash),
                )
            return cur.rowcount > 0

    def dead_job(
        self, file_hash: str, error_message: str, retry_count: Optional[int] = None
    ) -> bool:
        if not self.available:
            return False
        with self._cursor() as cur:
            if retry_count is not None:
                cur.execute(
                    "UPDATE jobs SET status = 'dead', error_message = %s, "
                    "retry_count = %s, updated_at = NOW() WHERE file_hash = %s",
                    (error_message, retry_count, file_hash),
                )
            else:
                cur.execute(
                    "UPDATE jobs SET status = 'dead', error_message = %s, "
                    "retry_count = retry_count + 1, updated_at = NOW() "
                    "WHERE file_hash = %s",
                    (error_message, file_hash),
                )
            return cur.rowcount > 0

    def requeue_job(
        self,
        file_hash: str,
        priority: str = DEFAULT_PRIORITY,
        increment_retry: bool = True,
    ) -> bool:
        """Move a job back to todo (for retry)."""
        if not self.available:
            return False
        with self._cursor() as cur:
            if increment_retry:
                cur.execute(
                    "UPDATE jobs SET status = 'todo', priority = %s, "
                    "worker_id = NULL, error_message = NULL, "
                    "retry_count = retry_count + 1, started_at = NULL, "
                    "updated_at = NOW() WHERE file_hash = %s",
                    (priority, file_hash),
                )
            else:
                cur.execute(
                    "UPDATE jobs SET status = 'todo', priority = %s, "
                    "worker_id = NULL, error_message = NULL, "
                    "started_at = NULL, updated_at = NOW() "
                    "WHERE file_hash = %s",
                    (priority, file_hash),
                )
            return cur.rowcount > 0

    def update_job_progress(self, file_hash: str, progress: Dict[str, Any]) -> bool:
        if not self.available:
            return False
        with self._cursor() as cur:
            cur.execute(
                "UPDATE jobs SET progress = %s, updated_at = NOW() "
                "WHERE file_hash = %s AND status = 'processing'",
                (json.dumps(progress), file_hash),
            )
            return cur.rowcount > 0

    def get_job(self, file_hash: str) -> Optional[Dict[str, Any]]:
        if not self.available:
            return None
        with self._cursor(commit=False) as cur:
            cur.execute("SELECT * FROM jobs WHERE file_hash = %s", (file_hash,))
            row = cur.fetchone()
            if not row:
                return None
            result = dict(row)
            for key in ("created_at", "updated_at", "started_at", "completed_at"):
                if result.get(key):
                    result[key] = (
                        result[key].isoformat()
                        if hasattr(result[key], "isoformat")
                        else str(result[key])
                    )
            if isinstance(result.get("status"), str):
                pass
            else:
                result["status"] = str(result.get("status", ""))
            return result

    def cancel_job(self, file_hash: str) -> bool:
        """Cancel a processing job and requeue it."""
        if not self.available:
            return False
        with self._cursor() as cur:
            cur.execute(
                "UPDATE jobs SET status = 'todo', worker_id = NULL, "
                "started_at = NULL, updated_at = NOW() "
                "WHERE file_hash = %s AND status = 'processing'",
                (file_hash,),
            )
            return cur.rowcount > 0

    def remove_job(self, file_hash: str) -> bool:
        """Remove a job from any state."""
        if not self.available:
            return False
        with self._cursor() as cur:
            cur.execute("DELETE FROM jobs WHERE file_hash = %s", (file_hash,))
            return cur.rowcount > 0

    def count_jobs_by_status(self, status: str, priority: Optional[str] = None) -> int:
        if not self.available:
            return 0
        with self._cursor(commit=False) as cur:
            if priority:
                cur.execute(
                    "SELECT COUNT(*) AS cnt FROM jobs WHERE status = %s AND priority = %s",
                    (status, priority),
                )
            else:
                cur.execute(
                    "SELECT COUNT(*) AS cnt FROM jobs WHERE status = %s", (status,)
                )
            return cur.fetchone()["cnt"]

    def get_processing_jobs(
        self, stale_minutes: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get currently processing jobs, optionally filtered by staleness."""
        if not self.available:
            return []
        with self._cursor(commit=False) as cur:
            if stale_minutes:
                cutoff = datetime.now(timezone.utc) - timedelta(minutes=stale_minutes)
                cur.execute(
                    "SELECT * FROM jobs WHERE status = 'processing' AND updated_at < %s "
                    "ORDER BY started_at",
                    (cutoff,),
                )
            else:
                cur.execute(
                    "SELECT * FROM jobs WHERE status = 'processing' ORDER BY started_at"
                )
            results = []
            for row in cur.fetchall():
                r = dict(row)
                for key in ("created_at", "updated_at", "started_at", "completed_at"):
                    if r.get(key) and hasattr(r[key], "isoformat"):
                        r[key] = r[key].isoformat()
                r["status"] = str(r.get("status", ""))
                r["progress"] = r.get("progress") or {}
                results.append(r)
            return results

    def get_stale_jobs(self, stale_minutes: int) -> List[Dict[str, Any]]:
        """Get processing jobs with no heartbeat update for stale_minutes."""
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=stale_minutes)
        if not self.available:
            return []
        with self._cursor(commit=False) as cur:
            cur.execute(
                "SELECT * FROM jobs WHERE status = 'processing' AND updated_at < %s",
                (cutoff,),
            )
            results = []
            for row in cur.fetchall():
                r = dict(row)
                for key in ("created_at", "updated_at", "started_at", "completed_at"):
                    if r.get(key) and hasattr(r[key], "isoformat"):
                        r[key] = r[key].isoformat()
                r["status"] = str(r.get("status", ""))
                results.append(r)
            return results

    # ------------------------------------------------------------------
    # Workers
    # ------------------------------------------------------------------

    def register_worker(self, extra_metadata: Optional[Dict[str, Any]] = None) -> bool:
        if not self.available:
            return False
        meta = get_worker_metadata()
        if extra_metadata:
            meta.update(extra_metadata)
        hostname = meta.get("hostname", "?")
        metadata_json = json.dumps(
            {k: v for k, v in meta.items() if k not in ("worker_id", "hostname")}
        )
        with self._cursor() as cur:
            cur.execute(
                "INSERT INTO workers (worker_id, hostname, status, metadata) "
                "VALUES (%s, %s, 'active', %s) "
                "ON CONFLICT (worker_id) DO UPDATE SET "
                "hostname = EXCLUDED.hostname, status = 'active', "
                "metadata = EXCLUDED.metadata, last_heartbeat = NOW()",
                (WORKER_ID, hostname, metadata_json),
            )
        return True

    def update_worker_heartbeat(
        self,
        current_job: Optional[str] = None,
        system_metrics: Optional[Dict[str, Any]] = None,
    ) -> bool:
        if not self.available:
            return False
        status = "processing" if current_job else "idle"
        with self._cursor() as cur:
            updates = "last_heartbeat = NOW(), status = %s"
            params: list = [status]
            if system_metrics:
                updates += ", system = %s"
                params.append(json.dumps(system_metrics))
            if current_job:
                updates += ", metadata = jsonb_set(COALESCE(metadata, '{}'), '{current_job}', %s)"
                params.append(json.dumps(current_job))
            params.append(WORKER_ID)
            cur.execute(f"UPDATE workers SET {updates} WHERE worker_id = %s", params)
        return True

    def update_worker_metrics(self, metrics: Dict[str, Any]) -> bool:
        if not self.available:
            return False
        with self._cursor() as cur:
            cur.execute(
                "UPDATE workers SET metrics = %s, last_heartbeat = NOW() "
                "WHERE worker_id = %s",
                (json.dumps(metrics), WORKER_ID),
            )
        return True

    def deregister_worker(self) -> bool:
        if not self.available:
            return False
        with self._cursor() as cur:
            cur.execute(
                "UPDATE workers SET status = 'stopped', last_heartbeat = NOW() "
                "WHERE worker_id = %s",
                (WORKER_ID,),
            )
        return True

    def list_workers(self) -> List[Dict[str, Any]]:
        if not self.available:
            return []
        with self._cursor(commit=False) as cur:
            cur.execute("SELECT * FROM workers ORDER BY registered_at")
            results = []
            for row in cur.fetchall():
                r = dict(row)
                r["worker_id"] = r.get("worker_id", "?")
                r["hostname"] = r.get("hostname", "?")
                r["status"] = r.get("status", "?")
                if isinstance(r.get("metadata"), str):
                    try:
                        r["metadata"] = json.loads(r["metadata"])
                    except (json.JSONDecodeError, TypeError):
                        r["metadata"] = {}
                if isinstance(r.get("metrics"), str):
                    try:
                        r["metrics"] = json.loads(r["metrics"])
                    except (json.JSONDecodeError, TypeError):
                        r["metrics"] = {}
                if isinstance(r.get("system"), str):
                    try:
                        r["system"] = json.loads(r["system"])
                    except (json.JSONDecodeError, TypeError):
                        r["system"] = {}
                for key in ("registered_at", "last_heartbeat"):
                    if r.get(key) and hasattr(r[key], "isoformat"):
                        r[key] = r[key].isoformat() + "Z"
                results.append(r)
            return results

    def get_active_workers(self, stale_minutes: int = 15) -> List[Dict[str, Any]]:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=stale_minutes)
        if not self.available:
            return []
        with self._cursor(commit=False) as cur:
            cur.execute(
                "SELECT * FROM workers WHERE last_heartbeat > %s AND status != 'stopped'",
                (cutoff,),
            )
            results = []
            for row in cur.fetchall():
                r = dict(row)
                r["worker_id"] = r.get("worker_id", "?")
                r["hostname"] = r.get("hostname", "?")
                r["status"] = r.get("status", "?")
                if isinstance(r.get("metadata"), str):
                    try:
                        r["metadata"] = json.loads(r["metadata"])
                    except (json.JSONDecodeError, TypeError):
                        r["metadata"] = {}
                if isinstance(r.get("metrics"), str):
                    try:
                        r["metrics"] = json.loads(r["metrics"])
                    except (json.JSONDecodeError, TypeError):
                        r["metrics"] = {}
                if isinstance(r.get("system"), str):
                    try:
                        r["system"] = json.loads(r["system"])
                    except (json.JSONDecodeError, TypeError):
                        r["system"] = {}
                for key in ("registered_at", "last_heartbeat"):
                    if r.get(key) and hasattr(r[key], "isoformat"):
                        r[key] = r[key].isoformat() + "Z"
                results.append(r)
            return results

    # ------------------------------------------------------------------
    # Job Logs
    # ------------------------------------------------------------------

    def save_job_error(
        self,
        file_hash: str,
        error: str,
        traceback: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        if not self.available:
            return False
        error_json = json.dumps(
            {
                "error": error,
                "traceback": traceback,
                "context": context or {},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )
        with self._cursor() as cur:
            cur.execute(
                "INSERT INTO job_logs (file_hash, log_type, error_json) "
                "VALUES (%s, 'error', %s)",
                (file_hash, error_json),
            )
        return True

    def get_job_error(self, file_hash: str) -> Optional[Dict[str, Any]]:
        if not self.available:
            return None
        with self._cursor(commit=False) as cur:
            cur.execute(
                "SELECT error_json FROM job_logs "
                "WHERE file_hash = %s AND log_type = 'error' "
                "ORDER BY created_at DESC LIMIT 1",
                (file_hash,),
            )
            row = cur.fetchone()
            if row and row.get("error_json"):
                data = row["error_json"]
                if isinstance(data, str):
                    return json.loads(data)
                return dict(data)
            return None

    # ------------------------------------------------------------------
    # Config
    # ------------------------------------------------------------------

    def get_config(self) -> Dict[str, Any]:
        if not self.available:
            return {}
        with self._cursor(commit=False) as cur:
            cur.execute("SELECT key, value FROM config")
            result = {}
            for row in cur.fetchall():
                val = row["value"]
                if isinstance(val, str):
                    try:
                        val = json.loads(val)
                    except (json.JSONDecodeError, TypeError):
                        pass
                result[row["key"]] = val
            return result

    def set_config(self, key: str, value: Any) -> bool:
        if not self.available:
            return False
        with self._cursor() as cur:
            cur.execute(
                "INSERT INTO config (key, value, updated_at) "
                "VALUES (%s, %s, NOW()) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()",
                (key, json.dumps(value) if not isinstance(value, str) else value),
            )
        return True

    # ------------------------------------------------------------------
    # Dashboard helpers (aggregate queries)
    # ------------------------------------------------------------------

    def get_queue_counts(self) -> Dict[str, Any]:
        """Get counts for all job statuses, broken down by priority for todo."""
        if not self.available:
            return {}
        with self._cursor(commit=False) as cur:
            result = {}
            for status in ("todo", "processing", "failed", "dead", "done"):
                if status == "todo":
                    by_prio = {}
                    for prio in PRIORITIES:
                        cur.execute(
                            "SELECT COUNT(*) AS cnt FROM jobs WHERE status = 'todo' AND priority = %s",
                            (prio,),
                        )
                        by_prio[prio] = cur.fetchone()["cnt"]
                    result["todo"] = by_prio
                    cur.execute(
                        "SELECT COUNT(*) AS cnt FROM jobs WHERE status = 'todo'"
                    )
                    result["todo_total"] = cur.fetchone()["cnt"]
                else:
                    cur.execute(
                        "SELECT COUNT(*) AS cnt FROM jobs WHERE status = %s", (status,)
                    )
                    result[status] = cur.fetchone()["cnt"]
            return result

    # ------------------------------------------------------------------
    # Migration from S3
    # ------------------------------------------------------------------

    def migrate_manifest(self, manifest: Dict[str, Any]) -> int:
        """Migrate an S3 manifest into the database. Returns count of entries."""
        if not self.available:
            return 0

        entries = manifest.get("entries", {})
        count = 0

        with self._cursor() as cur:
            for file_hash, entry in entries.items():
                paths = entry.get("paths", [])
                tags = entry.get("tags", [])
                size = entry.get("size", 0)
                source = entry.get("source", "")
                ingested_at = entry.get("ingested_at")

                cur.execute(
                    "INSERT INTO files (hash, size_bytes, source, ingested_at) "
                    "VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT (hash) DO UPDATE SET "
                    "size_bytes = EXCLUDED.size_bytes, "
                    "source = COALESCE(EXCLUDED.source, files.source)",
                    (
                        file_hash,
                        size,
                        source,
                        ingested_at if ingested_at else datetime.now(timezone.utc),
                    ),
                )
                for path in paths:
                    cur.execute(
                        "INSERT INTO file_paths (file_hash, path) "
                        "VALUES (%s, %s) ON CONFLICT (path) DO NOTHING",
                        (file_hash, path),
                    )
                for tag in tags:
                    cur.execute(
                        "INSERT INTO file_tags (file_hash, tag) "
                        "VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (file_hash, tag),
                    )
                count += 1

        return count

    def migrate_queues(self, s3) -> Dict[str, int]:
        """Migrate S3 queue state into the database. Returns counts by queue."""
        from .config import (
            S3_PREFIX_TODO,
            S3_PREFIX_PROCESSING,
            S3_PREFIX_DONE,
            S3_PREFIX_FAILED,
            S3_PREFIX_DEAD,
        )

        counts = {"todo": 0, "processing": 0, "done": 0, "failed": 0, "dead": 0}

        if not self.available:
            return counts

        # Todo queues
        for prio in PRIORITIES:
            keys = s3.list_keys(f"{S3_PREFIX_TODO}/{prio}/")
            for key in keys:
                file_hash = key.split("/")[-1]
                with self._cursor() as cur:
                    cur.execute(
                        "INSERT INTO jobs (file_hash, status, priority) "
                        "VALUES (%s, 'todo', %s) ON CONFLICT (file_hash) DO NOTHING",
                        (file_hash, prio),
                    )
                counts["todo"] += 1

        # Processing
        for obj in s3.list_processing():
            key = obj["Key"]
            file_hash = key.split("/")[-1]
            data = s3.get_object_json(key)
            worker_id = data.get("worker", "unknown") if data else "unknown"
            priority = (
                data.get("priority", DEFAULT_PRIORITY) if data else DEFAULT_PRIORITY
            )
            with self._cursor() as cur:
                cur.execute(
                    "INSERT INTO jobs (file_hash, status, priority, worker_id, started_at) "
                    "VALUES (%s, 'processing', %s, %s, NOW()) "
                    "ON CONFLICT (file_hash) DO NOTHING",
                    (file_hash, priority, worker_id),
                )
            counts["processing"] += 1

        # Done
        done_hashes = s3.list_done_hashes()
        for file_hash in done_hashes:
            with self._cursor() as cur:
                cur.execute(
                    "INSERT INTO jobs (file_hash, status, completed_at) "
                    "VALUES (%s, 'done', NOW()) "
                    "ON CONFLICT (file_hash) DO NOTHING",
                    (file_hash,),
                )
            counts["done"] += 1

        # Failed
        for obj in s3.list_failed():
            key = obj["Key"]
            file_hash = key.split("/")[-1]
            data = s3.get_object_json(key)
            error = data.get("error", "Unknown") if data else "Unknown"
            retries = data.get("retries", 0) if data else 0
            with self._cursor() as cur:
                cur.execute(
                    "INSERT INTO jobs (file_hash, status, error_message, retry_count) "
                    "VALUES (%s, 'failed', %s, %s) "
                    "ON CONFLICT (file_hash) DO NOTHING",
                    (file_hash, error, retries),
                )
            counts["failed"] += 1

        # Dead
        for obj in s3.list_dead():
            key = obj["Key"]
            file_hash = key.split("/")[-1]
            data = s3.get_object_json(key)
            error = data.get("error", "Unknown") if data else "Unknown"
            retries = data.get("total_retries", 0) if data else 0
            with self._cursor() as cur:
                cur.execute(
                    "INSERT INTO jobs (file_hash, status, error_message, retry_count) "
                    "VALUES (%s, 'dead', %s, %s) "
                    "ON CONFLICT (file_hash) DO NOTHING",
                    (file_hash, error, retries),
                )
            counts["dead"] += 1

        return counts
