"""SQLite state store for the self-hosted coordinator."""

from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Mapping


def now_ms() -> int:
    return int(time.time() * 1000)


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


class Conflict(RuntimeError):
    pass


class Database:
    def __init__(self, path: Path, *, lease_seconds: int, max_retries: int):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.lease_ms = lease_seconds * 1000
        self.max_retries = max_retries
        self.initialize()

    def _open(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA busy_timeout=30000")
        return db

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        db = self._open()
        try:
            yield db
        finally:
            db.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        db = self._open()
        try:
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def initialize(self) -> None:
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS sources (
                    source_key TEXT PRIMARY KEY, digest_algorithm TEXT NOT NULL,
                    digest TEXT NOT NULL, media_type TEXT NOT NULL,
                    original_name TEXT NOT NULL DEFAULT '', size_bytes INTEGER NOT NULL DEFAULT 0,
                    source TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
                    UNIQUE(digest_algorithm, digest)
                );
                CREATE TABLE IF NOT EXISTS source_aliases (
                    algorithm TEXT NOT NULL, digest TEXT NOT NULL, source_key TEXT NOT NULL REFERENCES sources(source_key),
                    PRIMARY KEY (algorithm, digest)
                );
                CREATE TABLE IF NOT EXISTS jobs (
                    source_key TEXT PRIMARY KEY REFERENCES sources(source_key),
                    status TEXT NOT NULL, priority TEXT NOT NULL, paths_json TEXT NOT NULL DEFAULT '[]',
                    tags_json TEXT NOT NULL DEFAULT '[]', retry_count INTEGER NOT NULL DEFAULT 0,
                    recipe_digest TEXT, recipe_json TEXT, worker_id TEXT, lease_token TEXT,
                    lease_expires_at INTEGER, progress_json TEXT, error_message TEXT,
                    created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
                    completed_at INTEGER, done_seq INTEGER
                );
                CREATE INDEX IF NOT EXISTS jobs_claim_idx ON jobs(status, priority, created_at);
                CREATE INDEX IF NOT EXISTS jobs_done_idx ON jobs(status, done_seq);
                CREATE TABLE IF NOT EXISTS artifacts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, source_key TEXT NOT NULL REFERENCES sources(source_key),
                    recipe_digest TEXT NOT NULL, identity TEXT NOT NULL, storage_path TEXT NOT NULL,
                    media_type TEXT NOT NULL, artifact_type TEXT NOT NULL DEFAULT 'legacy-archive',
                    size_bytes INTEGER NOT NULL, sha256 TEXT NOT NULL, blake3 TEXT NOT NULL,
                    provenance_json TEXT NOT NULL DEFAULT '{}', created_at INTEGER NOT NULL,
                    UNIQUE(source_key, recipe_digest)
                );
                CREATE TABLE IF NOT EXISTS recipes (
                    recipe_digest TEXT PRIMARY KEY, backend TEXT NOT NULL,
                    recipe_json TEXT NOT NULL, media_types_json TEXT NOT NULL,
                    artifact_type TEXT NOT NULL, last_seen INTEGER NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1, display_name TEXT NOT NULL DEFAULT '',
                    notes TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS worker_recipes (
                    worker_id TEXT NOT NULL REFERENCES workers(worker_id) ON DELETE CASCADE,
                    recipe_digest TEXT NOT NULL REFERENCES recipes(recipe_digest) ON DELETE CASCADE,
                    last_seen INTEGER NOT NULL,
                    PRIMARY KEY(worker_id, recipe_digest)
                );
                CREATE TABLE IF NOT EXISTS workers (
                    worker_id TEXT PRIMARY KEY, token_hash TEXT NOT NULL, hostname TEXT,
                    status TEXT NOT NULL DEFAULT 'offline', metadata_json TEXT NOT NULL DEFAULT '{}',
                    current_job TEXT, last_seen INTEGER, created_at INTEGER NOT NULL,
                    revoked INTEGER NOT NULL DEFAULT 0,
                    managed_by TEXT NOT NULL DEFAULT 'dynamic'
                );
                CREATE TABLE IF NOT EXISTS job_failures (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, source_key TEXT NOT NULL,
                    worker_id TEXT, attempt INTEGER NOT NULL, error TEXT NOT NULL,
                    traceback TEXT, context_json TEXT NOT NULL DEFAULT '{}', created_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value_json TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS scim_users (
                    id TEXT PRIMARY KEY, external_id TEXT UNIQUE, user_name TEXT NOT NULL UNIQUE,
                    display_name TEXT NOT NULL DEFAULT '', active INTEGER NOT NULL DEFAULT 1,
                    emails_json TEXT NOT NULL DEFAULT '[]', raw_json TEXT NOT NULL DEFAULT '{}',
                    version INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS scim_groups (
                    id TEXT PRIMARY KEY, external_id TEXT UNIQUE, display_name TEXT NOT NULL UNIQUE,
                    raw_json TEXT NOT NULL DEFAULT '{}', version INTEGER NOT NULL DEFAULT 1,
                    created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS scim_group_members (
                    group_id TEXT NOT NULL REFERENCES scim_groups(id) ON DELETE CASCADE,
                    user_id TEXT NOT NULL REFERENCES scim_users(id) ON DELETE CASCADE,
                    PRIMARY KEY(group_id,user_id)
                );
                CREATE TABLE IF NOT EXISTS admin_tokens (
                    id TEXT PRIMARY KEY, label TEXT NOT NULL, token_prefix TEXT NOT NULL,
                    token_hash TEXT NOT NULL UNIQUE, created_at INTEGER NOT NULL,
                    last_used INTEGER, expires_at INTEGER, revoked_at INTEGER
                );
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, principal TEXT NOT NULL,
                    action TEXT NOT NULL, target TEXT NOT NULL,
                    detail_json TEXT NOT NULL DEFAULT '{}', created_at INTEGER NOT NULL
                );
            """)
            artifact_columns = {row[1] for row in db.execute("PRAGMA table_info(artifacts)")}
            for name, declaration in (
                ("legacy", "INTEGER NOT NULL DEFAULT 0"),
                ("converter_backend", "TEXT"),
                ("converter_version", "TEXT"),
            ):
                if name not in artifact_columns:
                    db.execute(f"ALTER TABLE artifacts ADD COLUMN {name} {declaration}")
            recipe_columns = {row[1] for row in db.execute("PRAGMA table_info(recipes)")}
            for name, declaration in (
                ("enabled", "INTEGER NOT NULL DEFAULT 1"),
                ("display_name", "TEXT NOT NULL DEFAULT ''"),
                ("notes", "TEXT NOT NULL DEFAULT ''"),
            ):
                if name not in recipe_columns:
                    db.execute(f"ALTER TABLE recipes ADD COLUMN {name} {declaration}")
            worker_columns = {row[1] for row in db.execute("PRAGMA table_info(workers)")}
            if "managed_by" not in worker_columns:
                db.execute("ALTER TABLE workers ADD COLUMN managed_by TEXT NOT NULL DEFAULT 'dynamic'")
                # Before dynamic enrollment existed every worker row came from environment config.
                db.execute("UPDATE workers SET managed_by='environment'")

    def bootstrap_workers(self, worker_tokens: Mapping[str, str]) -> None:
        timestamp = now_ms()
        with self.transaction() as db:
            db.execute("UPDATE workers SET revoked=1 WHERE managed_by='environment'")
            for worker_id, token in worker_tokens.items():
                db.execute("""INSERT INTO workers(worker_id,token_hash,created_at,revoked,managed_by) VALUES(?,?,?,0,'environment')
                    ON CONFLICT(worker_id) DO UPDATE SET token_hash=excluded.token_hash,revoked=0,managed_by='environment'""",
                    (worker_id, token_hash(token), timestamp))

    def create_worker(self, worker_id: str) -> dict[str, Any]:
        token = "bfw_" + secrets.token_urlsafe(32)
        timestamp = now_ms()
        try:
            with self.transaction() as db:
                db.execute("""INSERT INTO workers(worker_id,token_hash,status,created_at,revoked,managed_by)
                    VALUES(?,?,'offline',?,0,'dynamic')""", (worker_id, token_hash(token), timestamp))
        except sqlite3.IntegrityError as exc:
            raise Conflict(f"worker {worker_id!r} already exists") from exc
        return {"worker_id": worker_id, "token": token, "created_at": timestamp}

    def rotate_worker_token(self, worker_id: str) -> dict[str, Any]:
        token = "bfw_" + secrets.token_urlsafe(32)
        with self.transaction() as db:
            row = db.execute("SELECT managed_by FROM workers WHERE worker_id=?", (worker_id,)).fetchone()
            if not row:
                raise KeyError(worker_id)
            if row["managed_by"] == "environment":
                raise Conflict("environment-managed worker credentials must be changed in deployment configuration")
            changed = db.execute("""UPDATE workers SET token_hash=?,revoked=0,managed_by='dynamic'
                WHERE worker_id=?""", (token_hash(token), worker_id)).rowcount
        if not changed:
            raise KeyError(worker_id)
        return {"worker_id": worker_id, "token": token}

    def revoke_worker(self, worker_id: str) -> None:
        with self.transaction() as db:
            row = db.execute("SELECT managed_by FROM workers WHERE worker_id=?", (worker_id,)).fetchone()
            if not row:
                raise KeyError(worker_id)
            if row["managed_by"] == "environment":
                raise Conflict("environment-managed workers must be removed from deployment configuration")
            changed = db.execute("UPDATE workers SET revoked=1,status='offline' WHERE worker_id=?", (worker_id,)).rowcount
        if not changed:
            raise KeyError(worker_id)

    def workers(self, include_revoked: bool = True) -> list[dict[str, Any]]:
        clause = "" if include_revoked else "WHERE revoked=0"
        with self.connect() as db:
            rows = list(db.execute(f"""SELECT worker_id,hostname,status,current_job,last_seen,
                created_at,revoked,managed_by,metadata_json FROM workers {clause} ORDER BY revoked,worker_id"""))
        result = []
        for row in rows:
            value = dict(row)
            value["revoked"] = bool(value["revoked"])
            value["metadata"] = json.loads(value.pop("metadata_json") or "{}")
            result.append(value)
        return result

    def create_admin_token(self, label: str, expires_at: int | None = None) -> dict[str, Any]:
        identifier = secrets.token_hex(8)
        token = f"bfa_{identifier}_{secrets.token_urlsafe(32)}"
        timestamp = now_ms()
        with self.transaction() as db:
            db.execute("""INSERT INTO admin_tokens(id,label,token_prefix,token_hash,created_at,expires_at)
                VALUES(?,?,?,?,?,?)""", (identifier, label, token[:20], token_hash(token), timestamp, expires_at))
        return {"id": identifier, "label": label, "token": token, "token_prefix": token[:20],
                "created_at": timestamp, "expires_at": expires_at}

    def admin_token(self, token: str) -> dict[str, Any] | None:
        timestamp = now_ms()
        digest = token_hash(token)
        with self.transaction() as db:
            row = db.execute("""SELECT id,label,token_prefix,created_at,last_used,expires_at FROM admin_tokens
                WHERE token_hash=? AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at>?)""",
                (digest, timestamp)).fetchone()
            if row:
                db.execute("UPDATE admin_tokens SET last_used=? WHERE id=?", (timestamp, row["id"]))
        return dict(row) if row else None

    def admin_tokens(self) -> list[dict[str, Any]]:
        with self.connect() as db:
            return [dict(row) for row in db.execute("""SELECT id,label,token_prefix,created_at,last_used,
                expires_at,revoked_at FROM admin_tokens ORDER BY created_at DESC""")]

    def revoke_admin_token(self, identifier: str) -> None:
        with self.transaction() as db:
            changed = db.execute("UPDATE admin_tokens SET revoked_at=? WHERE id=? AND revoked_at IS NULL",
                                 (now_ms(), identifier)).rowcount
        if not changed:
            raise KeyError(identifier)

    def audit(self, principal: str, action: str, target: str, detail: Mapping[str, Any] | None = None) -> None:
        with self.transaction() as db:
            db.execute("INSERT INTO audit_log(principal,action,target,detail_json,created_at) VALUES(?,?,?,?,?)",
                       (principal, action, target, json.dumps(detail or {}, sort_keys=True), now_ms()))

    def audit_events(self, limit: int = 100) -> list[dict[str, Any]]:
        with self.connect() as db:
            rows = [dict(row) for row in db.execute("SELECT * FROM audit_log ORDER BY id DESC LIMIT ?", (limit,))]
        for row in rows:
            row["detail"] = json.loads(row.pop("detail_json") or "{}")
        return rows

    def worker_for_token(self, token: str) -> str | None:
        with self.connect() as db:
            row = db.execute("SELECT worker_id FROM workers WHERE token_hash=? AND revoked=0", (token_hash(token),)).fetchone()
        return str(row[0]) if row else None

    @staticmethod
    def _normalize_capability(capability: Mapping[str, Any]) -> dict[str, Any]:
        digest = str(capability.get("recipe_digest") or "")
        recipe = capability.get("recipe") if isinstance(capability.get("recipe"), dict) else {}
        backend = str(capability.get("backend") or recipe.get("engine") or "unknown").strip().lower()
        media_types = sorted({str(value) for value in capability.get("media_types", []) if value})
        if not digest or not media_types:
            raise ValueError("each worker capability needs recipe_digest and media_types")
        return {
            "recipe_digest": digest,
            "backend": backend,
            "recipe": recipe,
            "media_types": media_types,
            "artifact_type": str(capability.get("artifact_type") or "mdaf/v1"),
        }

    def register_capabilities(self, worker_id: str, capabilities: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
        normalized = [self._normalize_capability(value) for value in capabilities]
        timestamp = now_ms()
        with self.transaction() as db:
            db.execute("DELETE FROM worker_recipes WHERE worker_id=?", (worker_id,))
            for value in normalized:
                db.execute("""INSERT INTO recipes(recipe_digest,backend,recipe_json,media_types_json,artifact_type,last_seen)
                    VALUES(?,?,?,?,?,?) ON CONFLICT(recipe_digest) DO UPDATE SET backend=excluded.backend,
                    recipe_json=excluded.recipe_json,media_types_json=excluded.media_types_json,
                    artifact_type=excluded.artifact_type,last_seen=excluded.last_seen""",
                    (value["recipe_digest"], value["backend"], json.dumps(value["recipe"], sort_keys=True),
                     json.dumps(value["media_types"]), value["artifact_type"], timestamp))
                db.execute("INSERT INTO worker_recipes(worker_id,recipe_digest,last_seen) VALUES(?,?,?)",
                           (worker_id, value["recipe_digest"], timestamp))
        return normalized

    def recipes(self, media_type: str | None = None) -> list[dict[str, Any]]:
        with self.connect() as db:
            rows = list(db.execute("""SELECT r.*,COUNT(w.worker_id) worker_count FROM recipes r
                LEFT JOIN worker_recipes wr USING(recipe_digest)
                LEFT JOIN workers w ON w.worker_id=wr.worker_id AND w.revoked=0
                GROUP BY r.recipe_digest
                ORDER BY r.backend,r.recipe_digest"""))
        result = []
        for row in rows:
            value = dict(row)
            value["recipe"] = json.loads(value.pop("recipe_json"))
            value["media_types"] = json.loads(value.pop("media_types_json"))
            value["enabled"] = bool(value["enabled"])
            if media_type is None or media_type in value["media_types"]:
                result.append(value)
        return result

    def resolve_backend(self, backend: str, media_type: str) -> str:
        matches = [item for item in self.recipes(media_type) if item["backend"] == backend.lower() and item["worker_count"] and item["enabled"]]
        if not matches:
            raise KeyError(backend)
        if len(matches) != 1:
            raise Conflict(f"backend {backend!r} has multiple active recipes; specify recipe_digest")
        return str(matches[0]["recipe_digest"])

    def oidc_principal(self, subject: str, role_groups: Mapping[str, str]) -> dict[str, Any] | None:
        with self.connect() as db:
            user = db.execute("SELECT * FROM scim_users WHERE external_id=? AND active=1", (subject,)).fetchone()
            if not user:
                return None
            groups = [str(row[0]) for row in db.execute("""SELECT g.display_name FROM scim_groups g
                JOIN scim_group_members gm ON gm.group_id=g.id WHERE gm.user_id=?""", (user["id"],))]
        roles = sorted({role_groups[name] for name in groups if name in role_groups})
        if not roles:
            return None
        return {"id": user["id"], "sub": subject, "user_name": user["user_name"],
                "display_name": user["display_name"], "groups": groups, "roles": roles}

    def enqueue(self, key: str, body: dict[str, Any]) -> dict[str, Any]:
        timestamp = now_ms()
        algorithm = str(body.get("digest_algorithm") or "sha256")
        digest = str(body.get("digest") or key)
        media_type = str(body.get("media_type") or "application/pdf")
        with self.transaction() as db:
            db.execute("""INSERT INTO sources(source_key,digest_algorithm,digest,media_type,original_name,size_bytes,source,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(source_key) DO UPDATE SET media_type=excluded.media_type,
                original_name=excluded.original_name,size_bytes=excluded.size_bytes,source=excluded.source,updated_at=excluded.updated_at""",
                (key, algorithm, digest, media_type, str(body.get("original_name") or ""), int(body.get("size_bytes") or 0), body.get("source"), timestamp, timestamp))
            db.execute("INSERT OR IGNORE INTO source_aliases VALUES(?,?,?)", (algorithm, digest, key))
            for alias_algorithm, alias_digest in (body.get("aliases") or {}).items():
                db.execute("INSERT OR IGNORE INTO source_aliases VALUES(?,?,?)", (alias_algorithm, alias_digest, key))
            db.execute("""INSERT INTO jobs(source_key,status,priority,paths_json,tags_json,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?) ON CONFLICT(source_key) DO UPDATE SET priority=excluded.priority,
                paths_json=excluded.paths_json,tags_json=excluded.tags_json,updated_at=excluded.updated_at""",
                (key, "todo", str(body.get("priority") or "3_normal"), json.dumps(body.get("paths") or []), json.dumps(body.get("tags") or []), timestamp, timestamp))
        return self.get_job(key)

    def get_job(self, key: str) -> dict[str, Any]:
        with self.connect() as db:
            row = db.execute("""SELECT j.*,s.digest_algorithm,s.digest,s.media_type,s.original_name,s.size_bytes,s.source
                FROM jobs j JOIN sources s USING(source_key) WHERE source_key=?""", (key,)).fetchone()
        if not row:
            raise KeyError(key)
        return self._job(row)

    def list_jobs(
        self, *, search: str = "", status: str = "", priority: str = "", media_type: str = "",
        limit: int = 50, offset: int = 0,
    ) -> dict[str, Any]:
        conditions: list[str] = []
        params: list[Any] = []
        if search:
            conditions.append("(LOWER(s.original_name) LIKE ? OR LOWER(j.source_key) LIKE ? OR LOWER(j.paths_json) LIKE ? OR LOWER(j.tags_json) LIKE ?)")
            needle = f"%{search.lower()}%"
            params.extend([needle, needle, needle, needle])
        if status:
            conditions.append("j.status=?"); params.append(status)
        if priority:
            conditions.append("j.priority=?"); params.append(priority)
        if media_type:
            conditions.append("s.media_type=?"); params.append(media_type)
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        with self.connect() as db:
            total = int(db.execute(f"SELECT COUNT(*) FROM jobs j JOIN sources s USING(source_key) {where}", params).fetchone()[0])
            rows = list(db.execute(f"""SELECT j.*,s.digest_algorithm,s.digest,s.media_type,
                s.original_name,s.size_bytes,s.source FROM jobs j JOIN sources s USING(source_key) {where}
                ORDER BY j.updated_at DESC LIMIT ? OFFSET ?""", (*params, limit, offset)))
        return {"total": total, "limit": limit, "offset": offset, "jobs": [self._job(row) for row in rows]}

    def job_failures(self, key: str) -> list[dict[str, Any]]:
        with self.connect() as db:
            rows = [dict(row) for row in db.execute("SELECT * FROM job_failures WHERE source_key=? ORDER BY id DESC", (key,))]
        for row in rows:
            row["context"] = json.loads(row.pop("context_json") or "{}")
        return rows

    def set_priority(self, key: str, priority: str) -> dict[str, Any]:
        with self.transaction() as db:
            changed = db.execute("UPDATE jobs SET priority=?,updated_at=? WHERE source_key=?",
                                 (priority, now_ms(), key)).rowcount
        if not changed:
            raise KeyError(key)
        return self.get_job(key)

    def requeue_job(self, key: str, *, reset_retries: bool = False) -> dict[str, Any]:
        with self.transaction() as db:
            row = db.execute("SELECT status FROM jobs WHERE source_key=?", (key,)).fetchone()
            if not row:
                raise KeyError(key)
            if row["status"] == "done":
                raise Conflict("completed jobs require a different recipe conversion request")
            retry_sql = ",retry_count=0" if reset_retries else ""
            db.execute(f"""UPDATE jobs SET status='todo',worker_id=NULL,lease_token=NULL,
                lease_expires_at=NULL,progress_json=NULL,error_message=NULL,updated_at=?{retry_sql}
                WHERE source_key=?""", (now_ms(), key))
        return self.get_job(key)

    def delete_job(self, key: str) -> dict[str, Any]:
        with self.transaction() as db:
            source = db.execute("SELECT * FROM sources WHERE source_key=?", (key,)).fetchone()
            if not source:
                raise KeyError(key)
            job = db.execute("SELECT status FROM jobs WHERE source_key=?", (key,)).fetchone()
            if job and job["status"] == "processing":
                raise Conflict("processing jobs must be requeued before deletion")
            artifacts = [dict(row) for row in db.execute("SELECT storage_path FROM artifacts WHERE source_key=?", (key,))]
            db.execute("UPDATE workers SET current_job=NULL WHERE current_job=?", (key,))
            db.execute("DELETE FROM job_failures WHERE source_key=?", (key,))
            db.execute("DELETE FROM artifacts WHERE source_key=?", (key,))
            db.execute("DELETE FROM source_aliases WHERE source_key=?", (key,))
            db.execute("DELETE FROM jobs WHERE source_key=?", (key,))
            db.execute("DELETE FROM sources WHERE source_key=?", (key,))
        return {"source": dict(source), "artifacts": artifacts}

    def update_recipe(self, digest: str, body: Mapping[str, Any]) -> dict[str, Any]:
        fields: list[str] = []
        params: list[Any] = []
        if "enabled" in body:
            fields.append("enabled=?"); params.append(int(bool(body["enabled"])))
        if "display_name" in body:
            fields.append("display_name=?"); params.append(str(body["display_name"]).strip()[:160])
        if "notes" in body:
            fields.append("notes=?"); params.append(str(body["notes"]).strip()[:4000])
        if not fields:
            raise ValueError("enabled, display_name, or notes is required")
        with self.transaction() as db:
            changed = db.execute(f"UPDATE recipes SET {','.join(fields)} WHERE recipe_digest=?",
                                 (*params, digest)).rowcount
        if not changed:
            raise KeyError(digest)
        return next(item for item in self.recipes() if item["recipe_digest"] == digest)

    @staticmethod
    def _job(row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        result["hash"] = result.pop("source_key")
        for old, new in (("paths_json", "paths"), ("tags_json", "tags"), ("progress_json", "progress"), ("recipe_json", "recipe")):
            raw = result.pop(old, None)
            result[new] = json.loads(raw) if raw else ([] if new in {"paths", "tags"} else None)
        return result

    def recover_expired(self, db: sqlite3.Connection) -> int:
        timestamp = now_ms()
        cursor = db.execute("""UPDATE jobs SET status='todo',worker_id=NULL,lease_token=NULL,lease_expires_at=NULL,
            updated_at=? WHERE status='processing' AND lease_expires_at<?""", (timestamp, timestamp))
        return cursor.rowcount

    def claim(self, worker_id: str, priorities: list[str], capabilities: list[Mapping[str, Any]]) -> dict[str, Any] | None:
        timestamp = now_ms()
        normalized = [self._normalize_capability(value) for value in capabilities]
        if not normalized:
            return None
        with self.transaction() as db:
            self.recover_expired(db)
            placeholders = ",".join("?" for _ in priorities) or "''"
            predicates: list[str] = []
            capability_params: list[Any] = []
            for capability in normalized:
                media_placeholders = ",".join("?" for _ in capability["media_types"])
                predicates.append(f"(s.media_type IN ({media_placeholders}) AND (j.recipe_digest IS NULL OR j.recipe_digest=?))")
                capability_params.extend(capability["media_types"])
                capability_params.append(capability["recipe_digest"])
            row = db.execute(f"""SELECT j.source_key FROM jobs j JOIN sources s USING(source_key)
                WHERE j.status='todo' AND j.priority IN ({placeholders}) AND ({' OR '.join(predicates)})
                ORDER BY CASE j.priority WHEN '1_urgent' THEN 1 WHEN '2_high' THEN 2 WHEN '3_normal' THEN 3 ELSE 4 END,j.created_at LIMIT 1""",
                (*priorities, *capability_params)).fetchone()
            if not row:
                return None
            key = str(row[0]); lease = secrets.token_urlsafe(24)
            media_type, requested_recipe = db.execute("""SELECT s.media_type,j.recipe_digest FROM jobs j
                JOIN sources s USING(source_key) WHERE j.source_key=?""", (key,)).fetchone()
            selected = next(value for value in normalized if media_type in value["media_types"] and
                            (requested_recipe is None or requested_recipe == value["recipe_digest"]))
            db.execute("""UPDATE jobs SET status='processing',worker_id=?,lease_token=?,lease_expires_at=?,
                recipe_digest=COALESCE(recipe_digest,?),recipe_json=COALESCE(recipe_json,?),updated_at=? WHERE source_key=?""",
                (worker_id, lease, timestamp + self.lease_ms, selected["recipe_digest"], json.dumps(selected["recipe"]), timestamp, key))
        job = self.get_job(key)
        job["lease_token"] = lease
        job["capability"] = selected
        return job

    def require_lease(self, db: sqlite3.Connection, key: str, worker_id: str, lease: str) -> sqlite3.Row:
        row = db.execute("SELECT * FROM jobs WHERE source_key=?", (key,)).fetchone()
        if not row or row["status"] != "processing" or row["worker_id"] != worker_id or row["lease_token"] != lease or int(row["lease_expires_at"] or 0) < now_ms():
            raise Conflict("lease is missing, expired, or owned by another worker")
        return row

    def lease_valid(self, key: str, worker_id: str, lease: str) -> bool:
        with self.connect() as db:
            try: self.require_lease(db, key, worker_id, lease)
            except Conflict: return False
        return True

    def heartbeat(self, key: str, worker_id: str, lease: str, progress: Any) -> None:
        with self.transaction() as db:
            self.require_lease(db, key, worker_id, lease)
            db.execute("UPDATE jobs SET lease_expires_at=?,progress_json=?,updated_at=? WHERE source_key=?", (now_ms()+self.lease_ms, json.dumps(progress or {}), now_ms(), key))

    def release(self, key: str, worker_id: str, lease: str) -> None:
        with self.transaction() as db:
            self.require_lease(db, key, worker_id, lease)
            db.execute("UPDATE jobs SET status='todo',worker_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=? WHERE source_key=?", (now_ms(), key))

    def fail(self, key: str, worker_id: str, lease: str, body: dict[str, Any]) -> str:
        with self.transaction() as db:
            row = self.require_lease(db, key, worker_id, lease)
            retry = int(row["retry_count"]) + 1
            status = "dead" if retry > self.max_retries else "failed"
            db.execute("""UPDATE jobs SET status=?,retry_count=?,error_message=?,worker_id=NULL,lease_token=NULL,
                lease_expires_at=NULL,updated_at=? WHERE source_key=?""", (status, retry, str(body.get("error") or "conversion failed"), now_ms(), key))
            db.execute("INSERT INTO job_failures(source_key,worker_id,attempt,error,traceback,context_json,created_at) VALUES(?,?,?,?,?,?,?)",
                (key, worker_id, retry, str(body.get("error") or "conversion failed"), body.get("traceback"), json.dumps(body.get("context") or {}), now_ms()))
        return status

    def complete(self, key: str, worker_id: str, lease: str, artifact: dict[str, Any], result: dict[str, Any]) -> None:
        timestamp = now_ms()
        with self.transaction() as db:
            row = self.require_lease(db, key, worker_id, lease)
            recipe = str(row["recipe_digest"] or "legacy")
            provenance = result or {}
            legacy = bool(provenance.get("legacy"))
            backend = provenance.get("converter_backend")
            converter_version = provenance.get("converter_version")
            db.execute("""INSERT INTO artifacts(source_key,recipe_digest,identity,storage_path,media_type,artifact_type,size_bytes,sha256,blake3,provenance_json,created_at,legacy,converter_backend,converter_version)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(source_key,recipe_digest) DO NOTHING""",
                (key, recipe, artifact["identity"], artifact["storage_path"], artifact["media_type"], artifact.get("artifact_type", "legacy-archive"), artifact["size_bytes"], artifact["sha256"], artifact["blake3"], json.dumps(provenance), timestamp, int(legacy), backend, converter_version))
            done_seq = int(db.execute("SELECT COALESCE(MAX(done_seq),0)+1 FROM jobs").fetchone()[0])
            db.execute("""UPDATE jobs SET status='done',worker_id=NULL,lease_token=NULL,lease_expires_at=NULL,
                completed_at=?,done_seq=?,updated_at=? WHERE source_key=?""", (timestamp, done_seq, timestamp, key))

    def artifact(self, key: str, recipe: str | None = None) -> dict[str, Any] | None:
        with self.connect() as db:
            if recipe:
                row = db.execute("SELECT * FROM artifacts WHERE source_key=? AND recipe_digest=?", (key, recipe)).fetchone()
            else:
                row = db.execute("SELECT * FROM artifacts WHERE source_key=? ORDER BY created_at DESC LIMIT 1", (key,)).fetchone()
        return dict(row) if row else None

    def artifacts(self, key: str) -> list[dict[str, Any]]:
        with self.connect() as db:
            rows = [dict(row) for row in db.execute("SELECT * FROM artifacts WHERE source_key=? ORDER BY created_at DESC", (key,))]
        for row in rows:
            row["provenance"] = json.loads(row.pop("provenance_json") or "{}")
            row["legacy"] = bool(row["legacy"])
        return rows

    def request_conversion(self, key: str, recipe: str) -> dict[str, Any]:
        timestamp = now_ms()
        with self.transaction() as db:
            source = db.execute("SELECT 1 FROM jobs WHERE source_key=?", (key,)).fetchone()
            if not source:
                raise KeyError(key)
            artifact = db.execute("SELECT 1 FROM artifacts WHERE source_key=? AND recipe_digest=?", (key, recipe)).fetchone()
            if artifact:
                db.execute("UPDATE jobs SET status='done',recipe_digest=?,updated_at=? WHERE source_key=?", (recipe, timestamp, key))
                action = "selected"
            else:
                db.execute("""UPDATE jobs SET status='todo',recipe_digest=?,recipe_json=NULL,worker_id=NULL,
                    lease_token=NULL,lease_expires_at=NULL,error_message=NULL,updated_at=? WHERE source_key=?""", (recipe, timestamp, key))
                action = "queued"
        return {"action": action, "job": self.get_job(key)}

    def statuses(self, keys: list[str]) -> dict[str, Any]:
        if not keys: return {}
        with self.connect() as db:
            rows = db.execute(f"SELECT source_key,status,recipe_digest,completed_at FROM jobs WHERE source_key IN ({','.join('?' for _ in keys)})", keys)
            found = {row["source_key"]: {"status": row["status"], "done": row["status"] == "done", "recipe_digest": row["recipe_digest"], "completed_at": row["completed_at"]} for row in rows}
        return {key: found.get(key, {"status": "missing", "done": False}) for key in keys}

    def snapshot(self) -> dict[str, Any]:
        with self.connect() as db:
            counts = {row["status"]: row["count"] for row in db.execute("SELECT status,COUNT(*) count FROM jobs GROUP BY status")}
            priority = {row["priority"]: row["count"] for row in db.execute("SELECT priority,COUNT(*) count FROM jobs WHERE status IN ('todo','failed') GROUP BY priority")}
            job_rows = list(db.execute("""SELECT j.*,s.original_name,s.size_bytes,s.media_type FROM jobs j
                JOIN sources s USING(source_key) ORDER BY j.updated_at DESC LIMIT 250"""))
            workers = [dict(row) for row in db.execute("SELECT worker_id,hostname,status,current_job,last_seen AS last_heartbeat FROM workers WHERE revoked=0 ORDER BY worker_id")]
        return {"counts": counts, "priority": priority, "jobs": [self._job(row) for row in job_rows], "workers": workers, "backend": "sqlite-filesystem", "generated_at": now_ms()}
