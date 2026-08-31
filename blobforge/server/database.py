"""SQLite state store for the self-hosted coordinator."""

from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
import time
from calendar import monthrange
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..converters.contract import PROVIDER_ATTEMPT_CONTRACT, PROVIDER_PROBE_CONTRACT
from ..mdaf import blake3_bytes, canonical_json_bytes
from ..recipe_lifecycle import assert_reprocessable, load_known_recipe


def now_ms() -> int:
    return int(time.time() * 1000)


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def monthly_quota_window(
    timestamp: int, *, reset_day: int, timezone_name: str
) -> tuple[int, int]:
    """Return the reset-day billing window containing a millisecond timestamp."""
    if isinstance(reset_day, bool) or not isinstance(reset_day, int) or not 1 <= reset_day <= 28:
        raise ValueError("reset_day must be an integer from 1 through 28")
    try:
        zone = ZoneInfo(timezone_name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError("timezone must be a valid IANA timezone") from exc
    current = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc).astimezone(zone)

    def boundary(year: int, month: int) -> datetime:
        day = min(reset_day, monthrange(year, month)[1])
        return datetime(year, month, day, tzinfo=zone)

    candidate = boundary(current.year, current.month)
    if current < candidate:
        if current.month == 1:
            start = boundary(current.year - 1, 12)
        else:
            start = boundary(current.year, current.month - 1)
    else:
        start = candidate
    if start.month == 12:
        end = boundary(start.year + 1, 1)
    else:
        end = boundary(start.year, start.month + 1)
    return round(start.timestamp() * 1000), round(end.timestamp() * 1000)


class Conflict(RuntimeError):
    pass


DEFAULT_SNAPSHOT_MAX_AGE_MS = 6 * 60 * 60 * 1000


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
                    completed_at INTEGER, done_seq INTEGER,
                    input_kind TEXT NOT NULL DEFAULT 'source',
                    input_artifact_id INTEGER,
                    parent_recipe_digest TEXT,
                    not_before INTEGER,
                    blocked_reason TEXT
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
                    notes TEXT NOT NULL DEFAULT '',
                    input_kinds_json TEXT NOT NULL DEFAULT '["source"]',
                    provider_account TEXT,
                    provider TEXT
                );
                CREATE TABLE IF NOT EXISTS worker_recipes (
                    worker_id TEXT NOT NULL REFERENCES workers(worker_id) ON DELETE CASCADE,
                    recipe_digest TEXT NOT NULL REFERENCES recipes(recipe_digest) ON DELETE CASCADE,
                    last_seen INTEGER NOT NULL,
                    input_kinds_json TEXT NOT NULL DEFAULT '["source"]',
                    provider_account TEXT,
                    provider TEXT,
                    claim_unassigned INTEGER NOT NULL DEFAULT 1,
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
                CREATE TABLE IF NOT EXISTS provider_accounts (
                    account_key TEXT PRIMARY KEY, provider TEXT NOT NULL,
                    currency TEXT NOT NULL DEFAULT 'USD',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    concurrency_limit INTEGER NOT NULL DEFAULT 1,
                    usage_basis TEXT NOT NULL DEFAULT 'reservation_estimate',
                    snapshot_max_age_ms INTEGER NOT NULL DEFAULT 21600000,
                    cooldown_until INTEGER, cooldown_reason TEXT,
                    created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS quota_policies (
                    id TEXT PRIMARY KEY, account_key TEXT NOT NULL REFERENCES provider_accounts(account_key),
                    revision INTEGER NOT NULL, window_start INTEGER NOT NULL,
                    window_end INTEGER NOT NULL, label TEXT NOT NULL DEFAULT '',
                    limit_requests INTEGER, limit_pages INTEGER,
                    limit_estimated_micro_usd INTEGER, limit_billed_micro_usd INTEGER,
                    currency TEXT NOT NULL DEFAULT 'USD',
                    created_at INTEGER NOT NULL, superseded_at INTEGER,
                    superseded_by TEXT REFERENCES quota_policies(id),
                    supersession_reason TEXT, UNIQUE(account_key, revision)
                );
                CREATE TABLE IF NOT EXISTS quota_schedules (
                    account_key TEXT PRIMARY KEY REFERENCES provider_accounts(account_key),
                    timezone TEXT NOT NULL, reset_day INTEGER NOT NULL,
                    label TEXT NOT NULL DEFAULT '', enabled INTEGER NOT NULL DEFAULT 1,
                    limit_requests INTEGER, limit_pages INTEGER,
                    limit_estimated_micro_usd INTEGER, limit_billed_micro_usd INTEGER,
                    created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS job_quota_overrides (
                    id TEXT PRIMARY KEY, source_key TEXT NOT NULL REFERENCES sources(source_key),
                    recipe_digest TEXT NOT NULL, extra_requests INTEGER NOT NULL DEFAULT 0,
                    extra_pages INTEGER NOT NULL DEFAULT 0,
                    extra_micro_usd INTEGER NOT NULL DEFAULT 0,
                    reason TEXT NOT NULL, actor TEXT NOT NULL, expires_at INTEGER NOT NULL,
                    consumed_by TEXT, revoked_at INTEGER, created_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS quota_reservations (
                    id TEXT PRIMARY KEY, source_key TEXT NOT NULL REFERENCES sources(source_key),
                    recipe_digest TEXT NOT NULL, account_key TEXT NOT NULL REFERENCES provider_accounts(account_key),
                    worker_id TEXT NOT NULL, lease_token_hash TEXT NOT NULL,
                    checkpoint_key TEXT NOT NULL, state TEXT NOT NULL,
                    cache_hit INTEGER NOT NULL DEFAULT 0,
                    reserved_requests INTEGER NOT NULL, reserved_pages INTEGER NOT NULL,
                    reserved_estimated_micro_usd INTEGER NOT NULL,
                    estimate_currency TEXT,
                    reserved_estimate_micro_units INTEGER,
                    fx_rate_id TEXT REFERENCES provider_fx_rates(id),
                    actual_requests INTEGER, actual_pages INTEGER,
                    list_micro_usd INTEGER, list_currency TEXT,
                    billed_micro_usd INTEGER,
                    credits_micro_usd INTEGER, override_id TEXT REFERENCES job_quota_overrides(id),
                    detail TEXT, created_at INTEGER NOT NULL, settled_at INTEGER,
                    reconcile_by INTEGER NOT NULL,
                    UNIQUE(source_key,recipe_digest,lease_token_hash)
                );
                CREATE INDEX IF NOT EXISTS quota_usage_idx
                    ON quota_reservations(account_key,created_at,state);
                CREATE TABLE IF NOT EXISTS provider_usage_snapshots (
                    id TEXT PRIMARY KEY,
                    account_key TEXT NOT NULL REFERENCES provider_accounts(account_key),
                    window_start INTEGER NOT NULL, window_end INTEGER NOT NULL,
                    observed_at INTEGER NOT NULL, coverage_through INTEGER NOT NULL,
                    reported_billed_micro_usd INTEGER NOT NULL,
                    currency TEXT NOT NULL, source TEXT NOT NULL,
                    reason TEXT NOT NULL, actor TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    UNIQUE(account_key,window_start,window_end,observed_at)
                );
                CREATE INDEX IF NOT EXISTS provider_usage_snapshot_lookup_idx
                    ON provider_usage_snapshots(
                        account_key,window_start,window_end,observed_at DESC
                    );
                CREATE TABLE IF NOT EXISTS provider_fx_rates (
                    id TEXT PRIMARY KEY,
                    account_key TEXT NOT NULL REFERENCES provider_accounts(account_key),
                    source_currency TEXT NOT NULL,
                    account_currency TEXT NOT NULL,
                    rate_numerator INTEGER NOT NULL,
                    rate_denominator INTEGER NOT NULL,
                    observed_at INTEGER NOT NULL,
                    valid_until INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    UNIQUE(account_key,source_currency,account_currency,observed_at)
                );
                CREATE INDEX IF NOT EXISTS provider_fx_rate_lookup_idx
                    ON provider_fx_rates(
                        account_key,source_currency,account_currency,
                        observed_at DESC,valid_until
                    );
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
            job_columns = {row[1] for row in db.execute("PRAGMA table_info(jobs)")}
            for name, declaration in (
                ("input_kind", "TEXT NOT NULL DEFAULT 'source'"),
                ("input_artifact_id", "INTEGER"),
                ("parent_recipe_digest", "TEXT"),
                ("not_before", "INTEGER"),
                ("blocked_reason", "TEXT"),
            ):
                if name not in job_columns:
                    db.execute(f"ALTER TABLE jobs ADD COLUMN {name} {declaration}")
            recipe_columns = {row[1] for row in db.execute("PRAGMA table_info(recipes)")}
            for name, declaration in (
                ("enabled", "INTEGER NOT NULL DEFAULT 1"),
                ("display_name", "TEXT NOT NULL DEFAULT ''"),
                ("notes", "TEXT NOT NULL DEFAULT ''"),
                ("provider_account", "TEXT"),
                ("provider", "TEXT"),
            ):
                if name not in recipe_columns:
                    db.execute(f"ALTER TABLE recipes ADD COLUMN {name} {declaration}")
            if "input_kinds_json" not in recipe_columns:
                db.execute(
                    "ALTER TABLE recipes ADD COLUMN input_kinds_json "
                    "TEXT NOT NULL DEFAULT '[\"source\"]'"
                )
            worker_recipe_columns = {
                row[1] for row in db.execute("PRAGMA table_info(worker_recipes)")
            }
            if "input_kinds_json" not in worker_recipe_columns:
                db.execute(
                    "ALTER TABLE worker_recipes ADD COLUMN input_kinds_json "
                    "TEXT NOT NULL DEFAULT '[\"source\"]'"
                )
            for name, declaration in (
                ("provider_account", "TEXT"),
                ("provider", "TEXT"),
                ("claim_unassigned", "INTEGER NOT NULL DEFAULT 1"),
            ):
                if name not in worker_recipe_columns:
                    db.execute(
                        f"ALTER TABLE worker_recipes ADD COLUMN {name} {declaration}"
                    )
            worker_columns = {row[1] for row in db.execute("PRAGMA table_info(workers)")}
            if "managed_by" not in worker_columns:
                db.execute("ALTER TABLE workers ADD COLUMN managed_by TEXT NOT NULL DEFAULT 'dynamic'")
                # Before dynamic enrollment existed every worker row came from environment config.
                db.execute("UPDATE workers SET managed_by='environment'")
            provider_columns = {
                row[1] for row in db.execute("PRAGMA table_info(provider_accounts)")
            }
            if "currency" not in provider_columns:
                db.execute(
                    "ALTER TABLE provider_accounts ADD COLUMN currency "
                    "TEXT NOT NULL DEFAULT 'USD'"
                )
            if "usage_basis" not in provider_columns:
                db.execute(
                    "ALTER TABLE provider_accounts ADD COLUMN usage_basis "
                    "TEXT NOT NULL DEFAULT 'reservation_estimate'"
                )
            if "snapshot_max_age_ms" not in provider_columns:
                db.execute(
                    "ALTER TABLE provider_accounts ADD COLUMN snapshot_max_age_ms "
                    f"INTEGER NOT NULL DEFAULT {DEFAULT_SNAPSHOT_MAX_AGE_MS}"
                )
            reservation_columns = {
                row[1] for row in db.execute("PRAGMA table_info(quota_reservations)")
            }
            for name, declaration in (
                ("estimate_currency", "TEXT"),
                ("reserved_estimate_micro_units", "INTEGER"),
                ("fx_rate_id", "TEXT REFERENCES provider_fx_rates(id)"),
                ("list_currency", "TEXT"),
            ):
                if name not in reservation_columns:
                    db.execute(
                        f"ALTER TABLE quota_reservations ADD COLUMN {name} {declaration}"
                    )
            db.execute(
                """UPDATE quota_reservations SET
                estimate_currency=COALESCE(estimate_currency,(
                    SELECT currency FROM provider_accounts
                    WHERE provider_accounts.account_key=quota_reservations.account_key
                )),
                reserved_estimate_micro_units=COALESCE(
                    reserved_estimate_micro_units,reserved_estimated_micro_usd
                ),
                list_currency=COALESCE(list_currency,(
                    SELECT currency FROM provider_accounts
                    WHERE provider_accounts.account_key=quota_reservations.account_key
                ))"""
            )
            policy_columns = {
                row[1] for row in db.execute("PRAGMA table_info(quota_policies)")
            }
            if "currency" not in policy_columns:
                db.execute(
                    "ALTER TABLE quota_policies ADD COLUMN currency "
                    "TEXT NOT NULL DEFAULT 'USD'"
                )
            if "superseded_at" not in policy_columns:
                db.execute("ALTER TABLE quota_policies ADD COLUMN superseded_at INTEGER")
            if "superseded_by" not in policy_columns:
                db.execute("ALTER TABLE quota_policies ADD COLUMN superseded_by TEXT")
            if "supersession_reason" not in policy_columns:
                db.execute("ALTER TABLE quota_policies ADD COLUMN supersession_reason TEXT")
            db.execute("DROP INDEX IF EXISTS quota_policy_window_idx")
            db.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS quota_policy_active_window_idx "
                "ON quota_policies(account_key,window_start,window_end) "
                "WHERE superseded_at IS NULL"
            )

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

    @staticmethod
    def _quota_integer(value: Any, name: str, *, optional: bool = False) -> int | None:
        if value is None and optional:
            return None
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{name} must be a non-negative integer")
        return value

    def configure_provider_account(
        self,
        account_key: str,
        provider: str,
        *,
        enabled: bool = True,
        concurrency_limit: int = 1,
        currency: str = "USD",
    ) -> dict[str, Any]:
        account_key = account_key.strip().lower()
        provider = provider.strip().lower()
        currency = currency.strip().upper()
        if not account_key or not provider:
            raise ValueError("account_key and provider are required")
        if len(currency) != 3 or not currency.isalpha():
            raise ValueError("currency must be a three-letter ISO 4217 code")
        if isinstance(concurrency_limit, bool) or concurrency_limit < 1:
            raise ValueError("concurrency_limit must be a positive integer")
        timestamp = now_ms()
        with self.transaction() as db:
            existing = db.execute(
                "SELECT provider,currency FROM provider_accounts WHERE account_key=?",
                (account_key,),
            ).fetchone()
            if existing and existing["provider"] != provider and db.execute(
                """SELECT 1 FROM quota_policies WHERE account_key=? UNION ALL
                SELECT 1 FROM quota_reservations WHERE account_key=? LIMIT 1""",
                (account_key, account_key),
            ).fetchone():
                raise Conflict("provider cannot change after an account has quota history")
            if existing and existing["currency"] != currency and db.execute(
                """SELECT 1 FROM quota_policies WHERE account_key=? UNION ALL
                SELECT 1 FROM quota_reservations WHERE account_key=? LIMIT 1""",
                (account_key, account_key),
            ).fetchone():
                raise Conflict("currency cannot change after an account has quota history")
            db.execute(
                """INSERT INTO provider_accounts(account_key,provider,currency,enabled,concurrency_limit,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?) ON CONFLICT(account_key) DO UPDATE SET
                provider=excluded.provider,enabled=excluded.enabled,
                currency=excluded.currency,concurrency_limit=excluded.concurrency_limit,
                updated_at=excluded.updated_at""",
                (account_key, provider, currency, int(enabled), concurrency_limit, timestamp, timestamp),
            )
            row = db.execute(
                "SELECT * FROM provider_accounts WHERE account_key=?", (account_key,)
            ).fetchone()
        value = dict(row)
        value["enabled"] = bool(value["enabled"])
        return value

    def record_provider_fx_rate(
        self,
        account_key: str,
        *,
        source_currency: str,
        rate_numerator: int,
        rate_denominator: int,
        observed_at: int,
        valid_until: int,
        source: str,
        reason: str,
        actor: str,
    ) -> dict[str, Any]:
        """Append an explicit source-price to account-currency conversion rate."""
        account_key = account_key.strip().lower()
        source_currency = source_currency.strip().upper()
        source = source.strip()
        reason = reason.strip()
        actor = actor.strip()
        if len(source_currency) != 3 or not source_currency.isalpha():
            raise ValueError("source_currency must be a three-letter ISO 4217 code")
        numerator = self._quota_integer(rate_numerator, "rate_numerator")
        denominator = self._quota_integer(rate_denominator, "rate_denominator")
        observed = self._quota_integer(observed_at, "observed_at")
        expires = self._quota_integer(valid_until, "valid_until")
        if not numerator or not denominator:
            raise ValueError("FX numerator and denominator must be positive")
        timestamp = now_ms()
        if observed > timestamp + 60_000:
            raise ValueError("observed_at cannot be in the future")
        if expires <= max(observed, timestamp):
            raise ValueError("valid_until must be later than observation time and now")
        if expires - observed > 31 * 86_400_000:
            raise ValueError("an FX rate cannot be valid for more than 31 days")
        if not source or len(source) > 120:
            raise ValueError("source must contain 1-120 characters")
        if not reason or len(reason) > 1000:
            raise ValueError("reason must contain 1-1000 characters")
        if not actor:
            raise ValueError("actor is required")
        identifier = "qfx_" + secrets.token_hex(10)
        with self.transaction() as db:
            account = db.execute(
                "SELECT currency FROM provider_accounts WHERE account_key=?",
                (account_key,),
            ).fetchone()
            if not account:
                raise KeyError(account_key)
            account_currency = str(account["currency"])
            if source_currency == account_currency:
                raise ValueError("same-currency estimates do not require an FX rate")
            try:
                db.execute(
                    """INSERT INTO provider_fx_rates(
                    id,account_key,source_currency,account_currency,
                    rate_numerator,rate_denominator,observed_at,valid_until,
                    source,reason,actor,created_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        identifier,
                        account_key,
                        source_currency,
                        account_currency,
                        numerator,
                        denominator,
                        observed,
                        expires,
                        source,
                        reason,
                        actor,
                        timestamp,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise Conflict("an FX observation already exists at this time") from exc
            released = db.execute(
                """UPDATE jobs SET not_before=NULL,blocked_reason=NULL,updated_at=?
                WHERE status='todo' AND blocked_reason LIKE 'no current % FX rate%'
                AND recipe_digest IN (
                    SELECT recipe_digest FROM recipes WHERE provider_account=?
                )""",
                (timestamp, account_key),
            ).rowcount
            row = db.execute(
                "SELECT * FROM provider_fx_rates WHERE id=?", (identifier,)
            ).fetchone()
        value = dict(row)
        value["released_fx_delays"] = released
        return value

    @staticmethod
    def _converted_estimate(
        db: sqlite3.Connection,
        account: sqlite3.Row,
        source_currency: str,
        source_micro_units: int,
        timestamp: int,
    ) -> tuple[int, str | None]:
        account_currency = str(account["currency"])
        if source_currency == account_currency:
            return source_micro_units, None
        rate = db.execute(
            """SELECT * FROM provider_fx_rates
            WHERE account_key=? AND source_currency=? AND account_currency=?
            AND observed_at<=? AND valid_until>?
            ORDER BY observed_at DESC,created_at DESC LIMIT 1""",
            (
                account["account_key"],
                source_currency,
                account_currency,
                timestamp,
                timestamp,
            ),
        ).fetchone()
        if not rate:
            raise LookupError("no current FX rate for provider estimate")
        numerator = int(rate["rate_numerator"])
        denominator = int(rate["rate_denominator"])
        converted = (source_micro_units * numerator + denominator - 1) // denominator
        if converted > 9_223_372_036_854_775_807:
            raise ValueError("converted provider estimate exceeds SQLite integer range")
        return converted, str(rate["id"])

    def configure_quota_schedule(
        self,
        account_key: str,
        *,
        timezone_name: str,
        reset_day: int,
        label: str = "",
        enabled: bool = True,
        limit_requests: int | None = None,
        limit_pages: int | None = None,
        limit_estimated_micro_usd: int | None = None,
        limit_billed_micro_usd: int | None = None,
    ) -> dict[str, Any]:
        account_key = account_key.strip().lower()
        monthly_quota_window(now_ms(), reset_day=reset_day, timezone_name=timezone_name)
        if not isinstance(enabled, bool):
            raise ValueError("enabled must be a boolean")
        limits = {
            "limit_requests": self._quota_integer(limit_requests, "limit_requests", optional=True),
            "limit_pages": self._quota_integer(limit_pages, "limit_pages", optional=True),
            "limit_estimated_micro_usd": self._quota_integer(
                limit_estimated_micro_usd, "limit_estimated_micro_usd", optional=True
            ),
            "limit_billed_micro_usd": self._quota_integer(
                limit_billed_micro_usd, "limit_billed_micro_usd", optional=True
            ),
        }
        if all(value is None for value in limits.values()):
            raise ValueError("quota schedule needs at least one limit")
        timestamp = now_ms()
        with self.transaction() as db:
            if not db.execute(
                "SELECT 1 FROM provider_accounts WHERE account_key=?", (account_key,)
            ).fetchone():
                raise KeyError(account_key)
            previous = db.execute(
                "SELECT * FROM quota_schedules WHERE account_key=?", (account_key,)
            ).fetchone()
            old_policy = None
            superseded_policy_ids: list[str] = []
            released_quota_delays = 0
            db.execute(
                """INSERT INTO quota_schedules(account_key,timezone,reset_day,label,enabled,
                limit_requests,limit_pages,limit_estimated_micro_usd,
                limit_billed_micro_usd,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(account_key) DO UPDATE SET
                timezone=excluded.timezone,reset_day=excluded.reset_day,label=excluded.label,
                enabled=excluded.enabled,limit_requests=excluded.limit_requests,
                limit_pages=excluded.limit_pages,
                limit_estimated_micro_usd=excluded.limit_estimated_micro_usd,
                limit_billed_micro_usd=excluded.limit_billed_micro_usd,
                updated_at=excluded.updated_at""",
                (
                    account_key,
                    timezone_name,
                    reset_day,
                    label.strip()[:160],
                    int(enabled),
                    limits["limit_requests"],
                    limits["limit_pages"],
                    limits["limit_estimated_micro_usd"],
                    limits["limit_billed_micro_usd"],
                    timestamp,
                    timestamp,
                ),
            )
            schedule = db.execute(
                "SELECT * FROM quota_schedules WHERE account_key=?", (account_key,)
            ).fetchone()
            if enabled:
                replacement_id = self._ensure_scheduled_policy(db, account_key, timestamp)
                if (
                    previous
                    and bool(previous["enabled"])
                    and (
                        previous["timezone"] != timezone_name
                        or int(previous["reset_day"]) != reset_day
                    )
                ):
                    old_start, old_end = monthly_quota_window(
                        timestamp,
                        reset_day=int(previous["reset_day"]),
                        timezone_name=str(previous["timezone"]),
                    )
                    old_policy = db.execute(
                        """SELECT * FROM quota_policies WHERE account_key=?
                        AND window_start=? AND window_end=? AND superseded_at IS NULL""",
                        (account_key, old_start, old_end),
                    ).fetchone()
                    replacement = db.execute(
                        "SELECT * FROM quota_policies WHERE id=?", (replacement_id,)
                    ).fetchone()
                    if old_policy and old_policy["id"] != replacement_id:
                        if int(replacement["window_start"]) > int(old_policy["window_start"]):
                            raise Conflict(
                                "new quota boundary would omit usage from the active policy"
                            )
                        for column in (
                            "limit_requests",
                            "limit_pages",
                            "limit_estimated_micro_usd",
                            "limit_billed_micro_usd",
                        ):
                            old_limit = old_policy[column]
                            replacement_limit = replacement[column]
                            if old_limit is None:
                                if replacement_limit is not None:
                                    continue
                            elif (
                                replacement_limit is not None
                                and int(replacement_limit) <= int(old_limit)
                            ):
                                continue
                            else:
                                raise Conflict(
                                    "new quota boundary cannot weaken the active policy limits"
                                )
                        db.execute(
                            """UPDATE quota_policies SET superseded_at=?,superseded_by=?,
                            supersession_reason=? WHERE id=? AND superseded_at IS NULL""",
                            (
                                timestamp,
                                replacement_id,
                                "recurring schedule boundary realignment",
                                old_policy["id"],
                            ),
                        )
                        superseded_policy_ids.append(str(old_policy["id"]))
                        released_quota_delays = db.execute(
                            """UPDATE jobs SET not_before=NULL,blocked_reason=NULL,updated_at=?
                            WHERE status='todo' AND (
                              blocked_reason='quota' OR (
                                json_valid(blocked_reason)
                                AND json_extract(blocked_reason,'$.kind')='quota'
                              )
                            ) AND recipe_digest IN (
                              SELECT recipe_digest FROM worker_recipes
                              WHERE provider_account=?
                            )""",
                            (timestamp, account_key),
                        ).rowcount
            value = dict(schedule)
            value["superseded_policy_ids"] = superseded_policy_ids
            value["released_quota_delays"] = released_quota_delays
        value["enabled"] = bool(value["enabled"])
        return value

    def _ensure_scheduled_policy(
        self, db: sqlite3.Connection, account_key: str, timestamp: int
    ) -> str | None:
        schedule = db.execute(
            "SELECT * FROM quota_schedules WHERE account_key=? AND enabled=1",
            (account_key,),
        ).fetchone()
        if not schedule:
            return None
        start, end = monthly_quota_window(
            timestamp,
            reset_day=int(schedule["reset_day"]),
            timezone_name=str(schedule["timezone"]),
        )
        existing = db.execute(
            """SELECT 1 FROM quota_policies
            WHERE account_key=? AND window_start=? AND window_end=?
            AND superseded_at IS NULL""",
            (account_key, start, end),
        ).fetchone()
        if existing:
            return str(
                db.execute(
                    """SELECT id FROM quota_policies WHERE account_key=?
                    AND window_start=? AND window_end=?
                    AND superseded_at IS NULL""",
                    (account_key, start, end),
                ).fetchone()[0]
            )
        account = db.execute(
            "SELECT currency FROM provider_accounts WHERE account_key=?", (account_key,)
        ).fetchone()
        revision = int(
            db.execute(
                "SELECT COALESCE(MAX(revision),0)+1 FROM quota_policies WHERE account_key=?",
                (account_key,),
            ).fetchone()[0]
        )
        start_label = datetime.fromtimestamp(start / 1000, tz=timezone.utc).astimezone(
            ZoneInfo(str(schedule["timezone"]))
        ).date()
        identifier = "qpol_" + secrets.token_hex(10)
        db.execute(
            """INSERT INTO quota_policies(id,account_key,revision,window_start,window_end,label,
            limit_requests,limit_pages,limit_estimated_micro_usd,
            limit_billed_micro_usd,currency,created_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                identifier,
                account_key,
                revision,
                start,
                end,
                f"{str(schedule['label']).strip() or 'monthly allowance'} · {start_label.isoformat()}",
                schedule["limit_requests"],
                schedule["limit_pages"],
                schedule["limit_estimated_micro_usd"],
                schedule["limit_billed_micro_usd"],
                account["currency"],
                now_ms(),
            ),
        )
        return identifier

    def create_quota_policy(
        self,
        account_key: str,
        *,
        window_start: int,
        window_end: int,
        label: str = "",
        limit_requests: int | None = None,
        limit_pages: int | None = None,
        limit_estimated_micro_usd: int | None = None,
        limit_billed_micro_usd: int | None = None,
    ) -> dict[str, Any]:
        account_key = account_key.strip().lower()
        if isinstance(window_start, bool) or isinstance(window_end, bool):
            raise ValueError("quota policy window timestamps must be integers")
        if not isinstance(window_start, int) or not isinstance(window_end, int) or window_end <= window_start:
            raise ValueError("quota policy window_end must be after window_start")
        limits = {
            "limit_requests": self._quota_integer(limit_requests, "limit_requests", optional=True),
            "limit_pages": self._quota_integer(limit_pages, "limit_pages", optional=True),
            "limit_estimated_micro_usd": self._quota_integer(
                limit_estimated_micro_usd, "limit_estimated_micro_usd", optional=True
            ),
            "limit_billed_micro_usd": self._quota_integer(
                limit_billed_micro_usd, "limit_billed_micro_usd", optional=True
            ),
        }
        if all(value is None for value in limits.values()):
            raise ValueError("quota policy needs at least one limit")
        timestamp = now_ms()
        identifier = "qpol_" + secrets.token_hex(10)
        with self.transaction() as db:
            account = db.execute(
                "SELECT currency FROM provider_accounts WHERE account_key=?", (account_key,)
            ).fetchone()
            if not account:
                raise KeyError(account_key)
            revision = int(
                db.execute(
                    "SELECT COALESCE(MAX(revision),0)+1 FROM quota_policies WHERE account_key=?",
                    (account_key,),
                ).fetchone()[0]
            )
            db.execute(
                """INSERT INTO quota_policies(id,account_key,revision,window_start,window_end,label,
                limit_requests,limit_pages,limit_estimated_micro_usd,
                limit_billed_micro_usd,currency,created_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    identifier,
                    account_key,
                    revision,
                    window_start,
                    window_end,
                    label.strip()[:160],
                    limits["limit_requests"],
                    limits["limit_pages"],
                    limits["limit_estimated_micro_usd"],
                    limits["limit_billed_micro_usd"],
                    account["currency"],
                    timestamp,
                ),
            )
            row = db.execute("SELECT * FROM quota_policies WHERE id=?", (identifier,)).fetchone()
        return dict(row)

    @staticmethod
    def _latest_usage_snapshot(
        db: sqlite3.Connection, policy: Mapping[str, Any], timestamp: int
    ) -> sqlite3.Row | None:
        return db.execute(
            """SELECT * FROM provider_usage_snapshots
            WHERE account_key=? AND window_start=? AND window_end=?
            AND observed_at<=? ORDER BY observed_at DESC,created_at DESC LIMIT 1""",
            (
                policy["account_key"],
                policy["window_start"],
                policy["window_end"],
                timestamp,
            ),
        ).fetchone()

    def _policy_usage(
        self,
        db: sqlite3.Connection,
        policy: Mapping[str, Any],
        account: Mapping[str, Any],
        timestamp: int,
    ) -> dict[str, Any]:
        row = db.execute(
            """SELECT COALESCE(SUM(reserved_requests),0) requests,
            COALESCE(SUM(reserved_pages),0) pages,
            COALESCE(SUM(reserved_estimated_micro_usd),0) estimated_micro_usd,
            COALESCE(SUM(COALESCE(billed_micro_usd,reserved_estimated_micro_usd)),0)
                billed_exposure_micro_usd
            FROM quota_reservations WHERE account_key=? AND created_at>=? AND created_at<?
            AND state IN ('reserved','committed','ambiguous')""",
            (policy["account_key"], policy["window_start"], policy["window_end"]),
        ).fetchone()
        usage = dict(row)
        usage["billed_basis"] = "reservation_estimate"
        usage["snapshot"] = None
        if account["usage_basis"] != "provider_snapshot":
            return usage
        snapshot = self._latest_usage_snapshot(db, policy, timestamp)
        if not snapshot:
            usage["billed_basis"] = "snapshot_missing"
            return usage
        snapshot_value = dict(snapshot)
        fresh_until = int(snapshot["observed_at"]) + int(account["snapshot_max_age_ms"])
        snapshot_value["fresh_until"] = fresh_until
        snapshot_value["fresh"] = timestamp <= fresh_until
        usage["snapshot"] = snapshot_value
        if not snapshot_value["fresh"]:
            usage["billed_basis"] = "snapshot_stale"
            return usage
        post_snapshot = int(
            db.execute(
                """SELECT COALESCE(SUM(reserved_estimated_micro_usd),0)
                FROM quota_reservations WHERE account_key=? AND created_at>?
                AND created_at>=? AND created_at<?
                AND state IN ('reserved','committed','ambiguous')""",
                (
                    policy["account_key"],
                    snapshot["coverage_through"],
                    policy["window_start"],
                    policy["window_end"],
                ),
            ).fetchone()[0]
        )
        usage["billed_exposure_micro_usd"] = (
            int(snapshot["reported_billed_micro_usd"]) + post_snapshot
        )
        usage["billed_basis"] = "provider_snapshot"
        usage["post_snapshot_estimated_micro_usd"] = post_snapshot
        return usage

    def record_provider_usage_snapshot(
        self,
        account_key: str,
        *,
        reported_billed_micro_usd: int,
        observed_at: int,
        coverage_through: int,
        reason: str,
        actor: str,
        activate_snapshot_accounting: bool,
        snapshot_max_age_ms: int = DEFAULT_SNAPSHOT_MAX_AGE_MS,
    ) -> dict[str, Any]:
        account_key = account_key.strip().lower()
        reported = self._quota_integer(
            reported_billed_micro_usd, "reported_billed_micro_usd"
        )
        if any(isinstance(value, bool) or not isinstance(value, int) for value in (
            observed_at,
            coverage_through,
            snapshot_max_age_ms,
        )):
            raise ValueError("snapshot timestamps and maximum age must be integers")
        timestamp = now_ms()
        if observed_at > timestamp + 60_000:
            raise ValueError("observed_at cannot be in the future")
        if coverage_through > observed_at:
            raise ValueError("coverage_through cannot be after observed_at")
        if not 15 * 60_000 <= snapshot_max_age_ms <= 7 * 24 * 60 * 60 * 1000:
            raise ValueError("snapshot_max_age_ms must be between 15 minutes and 7 days")
        reason = reason.strip()
        if not reason or len(reason) > 1000:
            raise ValueError("reason must contain 1-1000 characters")
        actor = actor.strip()
        if not actor:
            raise ValueError("actor is required")
        identifier = "quse_" + secrets.token_hex(10)
        replacement_policy_id = None
        released_quota_delays = 0
        with self.transaction() as db:
            account = db.execute(
                "SELECT * FROM provider_accounts WHERE account_key=?", (account_key,)
            ).fetchone()
            if not account:
                raise KeyError(account_key)
            policies = list(
                db.execute(
                    """SELECT * FROM quota_policies WHERE account_key=?
                    AND window_start<=? AND window_end>?
                    AND superseded_at IS NULL ORDER BY window_end""",
                    (account_key, observed_at, observed_at),
                )
            )
            billed_policies = [row for row in policies if row["limit_billed_micro_usd"] is not None]
            windows = {(int(row["window_start"]), int(row["window_end"])) for row in billed_policies}
            if len(windows) != 1:
                raise Conflict(
                    "snapshot needs exactly one active billed quota window at observed_at"
                )
            window_start, window_end = next(iter(windows))
            if not window_start <= coverage_through <= observed_at < window_end:
                raise ValueError("snapshot coverage must be inside its quota window")
            unsettled = db.execute(
                """SELECT id FROM quota_reservations WHERE account_key=?
                AND created_at>=? AND created_at<=? AND state IN ('reserved','ambiguous')
                LIMIT 1""",
                (account_key, window_start, coverage_through),
            ).fetchone()
            if unsettled:
                raise Conflict(
                    "cannot cover an unsettled reservation; reconcile or move coverage_through"
                )
            previous = db.execute(
                """SELECT * FROM provider_usage_snapshots WHERE account_key=?
                AND window_start=? AND window_end=?
                ORDER BY observed_at DESC,created_at DESC LIMIT 1""",
                (account_key, window_start, window_end),
            ).fetchone()
            if previous and observed_at <= int(previous["observed_at"]):
                raise Conflict("snapshot observed_at must advance monotonically")
            if previous and coverage_through < int(previous["coverage_through"]):
                raise Conflict("snapshot coverage_through cannot move backwards")
            if previous and reported < int(previous["reported_billed_micro_usd"]):
                raise Conflict("provider-reported usage cannot decrease within a window")
            db.execute(
                """INSERT INTO provider_usage_snapshots(
                id,account_key,window_start,window_end,observed_at,coverage_through,
                reported_billed_micro_usd,currency,source,reason,actor,created_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    identifier,
                    account_key,
                    window_start,
                    window_end,
                    observed_at,
                    coverage_through,
                    reported,
                    account["currency"],
                    "manual-console",
                    reason,
                    actor,
                    timestamp,
                ),
            )
            if activate_snapshot_accounting and account["usage_basis"] != "provider_snapshot":
                if len(billed_policies) != 1:
                    raise Conflict(
                        "snapshot accounting activation needs one active billed policy"
                    )
                old_policy = billed_policies[0]
                revision = int(
                    db.execute(
                        "SELECT COALESCE(MAX(revision),0)+1 FROM quota_policies WHERE account_key=?",
                        (account_key,),
                    ).fetchone()[0]
                )
                replacement_policy_id = "qpol_" + secrets.token_hex(10)
                db.execute(
                    """UPDATE quota_policies SET superseded_at=?,supersession_reason=?
                    WHERE id=? AND superseded_at IS NULL""",
                    (timestamp, "provider snapshot accounting activated: " + reason, old_policy["id"]),
                )
                db.execute(
                    """INSERT INTO quota_policies(
                    id,account_key,revision,window_start,window_end,label,
                    limit_requests,limit_pages,limit_estimated_micro_usd,
                    limit_billed_micro_usd,currency,created_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        replacement_policy_id,
                        account_key,
                        revision,
                        old_policy["window_start"],
                        old_policy["window_end"],
                        (str(old_policy["label"]) + " · provider-reported usage")[:160],
                        old_policy["limit_requests"],
                        old_policy["limit_pages"],
                        None,
                        old_policy["limit_billed_micro_usd"],
                        old_policy["currency"],
                        timestamp,
                    ),
                )
                db.execute(
                    "UPDATE quota_policies SET superseded_by=? WHERE id=?",
                    (replacement_policy_id, old_policy["id"]),
                )
                db.execute(
                    """UPDATE quota_schedules SET limit_estimated_micro_usd=NULL,
                    updated_at=? WHERE account_key=?""",
                    (timestamp, account_key),
                )
            if activate_snapshot_accounting or account["usage_basis"] == "provider_snapshot":
                db.execute(
                    """UPDATE provider_accounts SET usage_basis='provider_snapshot',
                    snapshot_max_age_ms=?,updated_at=? WHERE account_key=?""",
                    (snapshot_max_age_ms, timestamp, account_key),
                )
            released_quota_delays = db.execute(
                """UPDATE jobs SET not_before=NULL,blocked_reason=NULL,updated_at=?
                WHERE status='todo' AND (
                  blocked_reason='quota' OR (
                    json_valid(blocked_reason)
                    AND json_extract(blocked_reason,'$.kind')='quota'
                  )
                ) AND recipe_digest IN (
                  SELECT recipe_digest FROM worker_recipes WHERE provider_account=?
                )""",
                (timestamp, account_key),
            ).rowcount
            value = dict(
                db.execute(
                    "SELECT * FROM provider_usage_snapshots WHERE id=?", (identifier,)
                ).fetchone()
            )
        value["replacement_policy_id"] = replacement_policy_id
        value["released_quota_delays"] = released_quota_delays
        return value

    def create_quota_override(
        self,
        key: str,
        recipe_digest: str,
        *,
        extra_requests: int,
        extra_pages: int,
        extra_micro_usd: int,
        reason: str,
        actor: str,
        expires_at: int,
    ) -> dict[str, Any]:
        values = {
            "extra_requests": self._quota_integer(extra_requests, "extra_requests"),
            "extra_pages": self._quota_integer(extra_pages, "extra_pages"),
            "extra_micro_usd": self._quota_integer(extra_micro_usd, "extra_micro_usd"),
        }
        reason = reason.strip()
        if not reason or len(reason) > 1000:
            raise ValueError("reason must contain 1-1000 characters")
        if not recipe_digest or all(value == 0 for value in values.values()):
            raise ValueError("an exact recipe and at least one positive allowance are required")
        if isinstance(expires_at, bool) or not isinstance(expires_at, int) or expires_at <= now_ms():
            raise ValueError("expires_at must be a future millisecond timestamp")
        identifier = "qovr_" + secrets.token_hex(10)
        timestamp = now_ms()
        with self.transaction() as db:
            job = db.execute(
                "SELECT recipe_digest,status FROM jobs WHERE source_key=?", (key,)
            ).fetchone()
            if not job:
                raise KeyError(key)
            if job["recipe_digest"] != recipe_digest:
                raise Conflict("quota override recipe must match the exact queued job recipe")
            if job["status"] in {"processing", "done"}:
                raise Conflict("quota overrides can be attached only before a provider attempt")
            db.execute(
                """INSERT INTO job_quota_overrides(id,source_key,recipe_digest,extra_requests,
                extra_pages,extra_micro_usd,reason,actor,expires_at,created_at)
                VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (
                    identifier,
                    key,
                    recipe_digest,
                    values["extra_requests"],
                    values["extra_pages"],
                    values["extra_micro_usd"],
                    reason,
                    actor,
                    expires_at,
                    timestamp,
                ),
            )
            db.execute(
                "UPDATE jobs SET not_before=NULL,blocked_reason=NULL,updated_at=? WHERE source_key=?",
                (timestamp, key),
            )
            row = db.execute(
                "SELECT * FROM job_quota_overrides WHERE id=?", (identifier,)
            ).fetchone()
        return dict(row)

    def revoke_quota_override(self, identifier: str) -> None:
        with self.transaction() as db:
            changed = db.execute(
                """UPDATE job_quota_overrides SET revoked_at=?
                WHERE id=? AND revoked_at IS NULL AND consumed_by IS NULL""",
                (now_ms(), identifier),
            ).rowcount
        if not changed:
            raise KeyError(identifier)

    def quota_records(self, key: str) -> dict[str, Any]:
        with self.connect() as db:
            reservations = [
                dict(row)
                for row in db.execute(
                    "SELECT * FROM quota_reservations WHERE source_key=? ORDER BY created_at DESC",
                    (key,),
                )
            ]
            overrides = [
                dict(row)
                for row in db.execute(
                    "SELECT * FROM job_quota_overrides WHERE source_key=? ORDER BY created_at DESC",
                    (key,),
                )
            ]
        for item in reservations:
            item["cache_hit"] = bool(item["cache_hit"])
            item.pop("lease_token_hash", None)
        return {"reservations": reservations, "overrides": overrides}

    def reserve_quota(
        self,
        key: str,
        worker_id: str,
        lease: str,
        body: Mapping[str, Any],
    ) -> dict[str, Any]:
        timestamp = now_ms()
        if body.get("contract") != PROVIDER_PROBE_CONTRACT:
            raise ValueError("unsupported provider probe contract")
        account_key = str(body.get("account_key") or "").strip().lower()
        provider = str(body.get("provider") or "").strip().lower()
        checkpoint_key = str(body.get("checkpoint_key") or "").strip()
        cache_hit = body.get("cache_hit")
        requests = self._quota_integer(body.get("requests"), "requests")
        pages = self._quota_integer(body.get("pages"), "pages")
        source_estimated = self._quota_integer(
            body.get("estimated_micro_usd"), "estimated_micro_usd"
        )
        if not account_key or not provider or not checkpoint_key:
            raise ValueError("provider, account_key, and checkpoint_key are required")
        if not isinstance(cache_hit, bool):
            raise ValueError("cache_hit must be a boolean")
        if cache_hit and (requests or source_estimated):
            raise ValueError("cache hits cannot reserve requests or spend")
        identifier = "qres_" + secrets.token_hex(12)
        lease_digest = token_hash(lease)
        with self.transaction() as db:
            job = self.require_lease(db, key, worker_id, lease)
            recipe_digest = str(job["recipe_digest"] or "")
            recipe = db.execute(
                """SELECT provider_account,provider FROM worker_recipes
                WHERE worker_id=? AND recipe_digest=?""",
                (worker_id, recipe_digest),
            ).fetchone()
            if not recipe or recipe["provider_account"] != account_key or recipe["provider"] != provider:
                raise Conflict("quota probe does not match the leased recipe provider account")
            account = db.execute(
                "SELECT * FROM provider_accounts WHERE account_key=?", (account_key,)
            ).fetchone()
            if not account or account["provider"] != provider:
                raise Conflict("provider account is not configured")
            if not account["enabled"]:
                raise Conflict("provider account is disabled")
            probe_currency = str(body.get("currency") or "USD").upper()
            if probe_currency != str(account["currency"]):
                raise Conflict("provider probe currency does not match the account currency")
            estimate_currency = str(
                body.get("estimate_currency") or probe_currency
            ).upper()
            if len(estimate_currency) != 3 or not estimate_currency.isalpha():
                raise ValueError(
                    "estimate_currency must be a three-letter ISO 4217 code"
                )
            existing = db.execute(
                """SELECT * FROM quota_reservations
                WHERE source_key=? AND recipe_digest=? AND lease_token_hash=?""",
                (key, recipe_digest, lease_digest),
            ).fetchone()
            if existing:
                if (
                    existing["account_key"] != account_key
                    or existing["checkpoint_key"] != checkpoint_key
                    or bool(existing["cache_hit"]) != cache_hit
                    or int(existing["reserved_requests"]) != requests
                    or int(existing["reserved_pages"]) != pages
                    or str(existing["estimate_currency"] or probe_currency)
                    != estimate_currency
                    or int(
                        existing["reserved_estimate_micro_units"]
                        if existing["reserved_estimate_micro_units"] is not None
                        else existing["reserved_estimated_micro_usd"]
                    )
                    != source_estimated
                ):
                    raise Conflict("quota retry does not match its existing reservation")
                value = dict(existing)
                value.pop("lease_token_hash", None)
                return {"authorized": True, "reservation": value, "idempotent": True}
            resume_identifier = body.get("resume_reservation_id")
            if isinstance(resume_identifier, str) and resume_identifier:
                resumable = db.execute(
                    """SELECT * FROM quota_reservations WHERE id=? AND source_key=?
                    AND recipe_digest=? AND account_key=? AND checkpoint_key=?
                    AND state IN ('reserved','ambiguous')""",
                    (
                        resume_identifier,
                        key,
                        recipe_digest,
                        account_key,
                        checkpoint_key,
                    ),
                ).fetchone()
                if resumable:
                    if (
                        str(resumable["estimate_currency"] or probe_currency)
                        != estimate_currency
                        or (
                            not cache_hit
                            and (
                                int(resumable["reserved_requests"]) != requests
                                or int(resumable["reserved_pages"]) != pages
                                or int(
                                    resumable["reserved_estimate_micro_units"]
                                    if resumable["reserved_estimate_micro_units"]
                                    is not None
                                    else resumable["reserved_estimated_micro_usd"]
                                )
                                != source_estimated
                            )
                        )
                    ):
                        raise Conflict(
                            "provider checkpoint estimate does not match its reservation"
                        )
                    db.execute(
                        """UPDATE quota_reservations SET worker_id=?,lease_token_hash=?,
                        state='reserved',reconcile_by=? WHERE id=?""",
                        (worker_id, lease_digest, timestamp + 86_400_000, resume_identifier),
                    )
                    value = dict(
                        db.execute(
                            "SELECT * FROM quota_reservations WHERE id=?",
                            (resume_identifier,),
                        ).fetchone()
                    )
                    value.pop("lease_token_hash", None)
                    value["cache_hit"] = bool(value["cache_hit"])
                    return {"authorized": True, "reservation": value, "resumed": True}
                if not cache_hit:
                    raise Conflict(
                        "provider checkpoint names a reservation that cannot be resumed; "
                        "reconcile it before retrying"
                    )
            try:
                estimated, fx_rate_id = self._converted_estimate(
                    db,
                    account,
                    estimate_currency,
                    source_estimated,
                    timestamp,
                ) if not cache_hit else (0, None)
            except LookupError:
                defer_until = timestamp + 300_000
                reason = (
                    f"no current {estimate_currency}/{account['currency']} FX rate "
                    "for provider estimate"
                )
                db.execute(
                    """UPDATE jobs SET status='todo',worker_id=NULL,lease_token=NULL,
                    lease_expires_at=NULL,not_before=?,blocked_reason=?,updated_at=?
                    WHERE source_key=?""",
                    (defer_until, reason, timestamp, key),
                )
                return {
                    "authorized": False,
                    "reason": reason,
                    "not_before": defer_until,
                }
            if account["cooldown_until"] and int(account["cooldown_until"]) > timestamp:
                defer_until = int(account["cooldown_until"])
                reason = str(account["cooldown_reason"] or "provider cooldown")
                db.execute(
                    """UPDATE jobs SET status='todo',worker_id=NULL,lease_token=NULL,
                    lease_expires_at=NULL,not_before=?,blocked_reason=?,updated_at=? WHERE source_key=?""",
                    (defer_until, reason, timestamp, key),
                )
                return {"authorized": False, "reason": reason, "not_before": defer_until}
            if not cache_hit:
                active = int(
                    db.execute(
                        """SELECT COUNT(*) FROM quota_reservations
                        WHERE account_key=? AND cache_hit=0 AND state IN ('reserved','ambiguous')""",
                        (account_key,),
                    ).fetchone()[0]
                )
                if active >= int(account["concurrency_limit"]):
                    defer_until = timestamp + 30_000
                    reason = "provider account concurrency limit reached"
                    db.execute(
                        """UPDATE jobs SET status='todo',worker_id=NULL,lease_token=NULL,
                        lease_expires_at=NULL,not_before=?,blocked_reason=?,updated_at=? WHERE source_key=?""",
                        (defer_until, reason, timestamp, key),
                    )
                    return {"authorized": False, "reason": reason, "not_before": defer_until}
            self._ensure_scheduled_policy(db, account_key, timestamp)
            policies = list(
                db.execute(
                    """SELECT * FROM quota_policies WHERE account_key=?
                    AND window_start<=? AND window_end>?
                    AND (superseded_at IS NULL OR superseded_at>?) ORDER BY window_end""",
                    (account_key, timestamp, timestamp, timestamp),
                )
            )
            if not cache_hit and not policies:
                next_row = db.execute(
                    "SELECT MIN(window_start) FROM quota_policies WHERE account_key=? AND window_start>?",
                    (account_key, timestamp),
                ).fetchone()
                defer_until = int(next_row[0]) if next_row and next_row[0] else timestamp + 300_000
                reason = "no active BlobForge quota policy"
                db.execute(
                    """UPDATE jobs SET status='todo',worker_id=NULL,lease_token=NULL,
                    lease_expires_at=NULL,not_before=?,blocked_reason=?,updated_at=? WHERE source_key=?""",
                    (defer_until, reason, timestamp, key),
                )
                return {"authorized": False, "reason": reason, "not_before": defer_until}
            override = db.execute(
                """SELECT * FROM job_quota_overrides WHERE source_key=? AND recipe_digest=?
                AND revoked_at IS NULL AND consumed_by IS NULL AND expires_at>?
                ORDER BY created_at LIMIT 1""",
                (key, recipe_digest, timestamp),
            ).fetchone()
            extras = {
                "requests": int(override["extra_requests"]) if override else 0,
                "pages": int(override["extra_pages"]) if override else 0,
                "estimated": int(override["extra_micro_usd"]) if override else 0,
                "billed": int(override["extra_micro_usd"]) if override else 0,
            }
            exceeded: list[dict[str, Any]] = []
            override_needed = False
            for policy in policies:
                usage = self._policy_usage(db, policy, account, timestamp)
                if (
                    not cache_hit
                    and account["usage_basis"] == "provider_snapshot"
                    and policy["limit_billed_micro_usd"] is not None
                    and usage["billed_basis"] != "provider_snapshot"
                ):
                    snapshot = usage.get("snapshot") or {}
                    exceeded.append(
                        {
                            "policy_id": policy["id"],
                            "dimension": "provider_snapshot",
                            "used": int(usage["billed_exposure_micro_usd"]),
                            "requested": estimated,
                            "limit": int(policy["limit_billed_micro_usd"] or 0),
                            "reason": usage["billed_basis"],
                            "fresh_until": snapshot.get("fresh_until"),
                        }
                    )
                    continue
                dimensions = (
                    ("requests", policy["limit_requests"], int(usage["requests"]), requests),
                    ("pages", policy["limit_pages"], int(usage["pages"]), pages),
                    (
                        "estimated",
                        policy["limit_estimated_micro_usd"],
                        int(usage["estimated_micro_usd"]),
                        estimated,
                    ),
                    (
                        "billed",
                        policy["limit_billed_micro_usd"],
                        int(usage["billed_exposure_micro_usd"]),
                        estimated,
                    ),
                )
                for name, limit, used, requested in dimensions:
                    if limit is None or used + requested <= int(limit):
                        continue
                    override_needed = True
                    if used + requested > int(limit) + extras[name]:
                        exceeded.append(
                            {"policy_id": policy["id"], "dimension": name,
                             "used": used, "requested": requested, "limit": int(limit)}
                        )
            if exceeded:
                defer_until = min(int(policy["window_end"]) for policy in policies)
                reason = json.dumps({"kind": "quota", "exceeded": exceeded}, sort_keys=True)
                db.execute(
                    """UPDATE jobs SET status='todo',worker_id=NULL,lease_token=NULL,
                    lease_expires_at=NULL,not_before=?,blocked_reason=?,updated_at=? WHERE source_key=?""",
                    (defer_until, reason, timestamp, key),
                )
                return {"authorized": False, "reason": "quota exhausted", "not_before": defer_until, "exceeded": exceeded}
            override_id = str(override["id"]) if override_needed and override else None
            db.execute(
                """INSERT INTO quota_reservations(id,source_key,recipe_digest,account_key,
                worker_id,lease_token_hash,checkpoint_key,state,cache_hit,reserved_requests,
                reserved_pages,reserved_estimated_micro_usd,estimate_currency,
                reserved_estimate_micro_units,fx_rate_id,created_at,reconcile_by,override_id)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    identifier, key, recipe_digest, account_key, worker_id, lease_digest,
                    checkpoint_key, "reserved", int(cache_hit), requests, pages, estimated,
                    estimate_currency, source_estimated, fx_rate_id,
                    timestamp, timestamp + 86_400_000, override_id,
                ),
            )
            if override_id:
                db.execute(
                    "UPDATE job_quota_overrides SET consumed_by=? WHERE id=?",
                    (identifier, override_id),
                )
            row = db.execute("SELECT * FROM quota_reservations WHERE id=?", (identifier,)).fetchone()
        value = dict(row)
        value.pop("lease_token_hash", None)
        value["cache_hit"] = bool(value["cache_hit"])
        return {"authorized": True, "reservation": value, "idempotent": False}

    def settle_quota(
        self,
        identifier: str,
        worker_id: str,
        report: Mapping[str, Any],
    ) -> dict[str, Any]:
        timestamp = now_ms()
        if report.get("contract") != PROVIDER_ATTEMPT_CONTRACT:
            raise ValueError("unsupported provider attempt contract")
        state = str(report.get("state") or "")
        if state not in {"committed", "cache_hit", "released", "ambiguous", "rate_limited"}:
            raise ValueError("unsupported provider attempt state")
        with self.transaction() as db:
            row = db.execute(
                "SELECT * FROM quota_reservations WHERE id=?", (identifier,)
            ).fetchone()
            if not row:
                raise KeyError(identifier)
            if row["worker_id"] != worker_id:
                raise Conflict("quota reservation belongs to another worker")
            if report.get("reservation_id") != identifier:
                raise Conflict("provider report reservation does not match")
            if report.get("account_key") != row["account_key"] or report.get("checkpoint_key") != row["checkpoint_key"]:
                raise Conflict("provider report identity does not match the reservation")
            account = db.execute(
                "SELECT provider,currency FROM provider_accounts WHERE account_key=?",
                (row["account_key"],),
            ).fetchone()
            if not account or report.get("provider") != account["provider"]:
                raise Conflict("provider report does not match the reservation provider")
            report_currency = str(report.get("currency") or "USD").upper()
            if report_currency != str(account["currency"]):
                raise Conflict("provider report currency does not match the account currency")
            if row["state"] != "reserved":
                value = dict(row)
                value.pop("lease_token_hash", None)
                return value
            actual_requests = self._quota_integer(report.get("requests"), "requests")
            actual_pages = self._quota_integer(report.get("pages"), "pages")
            list_micro = self._quota_integer(report.get("list_micro_usd"), "list_micro_usd", optional=True)
            list_currency = str(
                report.get("list_currency") or report_currency
            ).upper()
            if len(list_currency) != 3 or not list_currency.isalpha():
                raise ValueError(
                    "list_currency must be a three-letter ISO 4217 code"
                )
            if list_currency != str(row["estimate_currency"] or report_currency):
                raise Conflict(
                    "provider list-price currency does not match the reservation estimate"
                )
            billed_micro = self._quota_integer(report.get("billed_micro_usd"), "billed_micro_usd", optional=True)
            credits_micro = self._quota_integer(report.get("credits_micro_usd"), "credits_micro_usd", optional=True)
            if actual_requests > int(row["reserved_requests"]) or actual_pages > int(row["reserved_pages"]):
                raise Conflict("provider usage exceeds the authorized request or page reservation")
            stored_state = "committed" if state in {"committed", "cache_hit"} else state
            if state == "rate_limited":
                stored_state = "released"
                retry_after = self._quota_integer(
                    report.get("retry_after_ms"), "retry_after_ms", optional=True
                )
                cooldown_until = timestamp + max(
                    1_000, min(retry_after or 60_000, 86_400_000)
                )
                db.execute(
                    """UPDATE provider_accounts SET cooldown_until=?,cooldown_reason=?,updated_at=?
                    WHERE account_key=?""",
                    (cooldown_until, "provider rate limited the account", timestamp, row["account_key"]),
                )
                db.execute(
                    """UPDATE jobs SET status='todo',worker_id=NULL,lease_token=NULL,
                    lease_expires_at=NULL,not_before=?,blocked_reason=?,updated_at=?
                    WHERE source_key=? AND worker_id=? AND status='processing'""",
                    (
                        cooldown_until,
                        "provider rate limited the account",
                        timestamp,
                        row["source_key"],
                        worker_id,
                    ),
                )
            db.execute(
                """UPDATE quota_reservations SET state=?,actual_requests=?,actual_pages=?,
                list_micro_usd=?,list_currency=?,billed_micro_usd=?,credits_micro_usd=?,
                detail=?,settled_at=?
                WHERE id=?""",
                (
                    stored_state, actual_requests, actual_pages, list_micro,
                    list_currency, billed_micro, credits_micro,
                    str(report.get("detail") or "")[:1000] or None,
                    timestamp, identifier,
                ),
            )
            value = dict(db.execute("SELECT * FROM quota_reservations WHERE id=?", (identifier,)).fetchone())
        value.pop("lease_token_hash", None)
        value["cache_hit"] = bool(value["cache_hit"])
        return value

    def reconcile_quota(
        self,
        identifier: str,
        *,
        state: str,
        detail: str,
        billed_micro_usd: int | None = None,
        credits_micro_usd: int | None = None,
    ) -> dict[str, Any]:
        if state not in {"committed", "released"}:
            raise ValueError("reconciliation state must be committed or released")
        detail = detail.strip()
        if not detail or len(detail) > 1000:
            raise ValueError("reconciliation detail must contain 1-1000 characters")
        billed = self._quota_integer(
            billed_micro_usd, "billed_micro_usd", optional=True
        )
        credits = self._quota_integer(
            credits_micro_usd, "credits_micro_usd", optional=True
        )
        with self.transaction() as db:
            row = db.execute(
                "SELECT * FROM quota_reservations WHERE id=?", (identifier,)
            ).fetchone()
            if not row:
                raise KeyError(identifier)
            if row["state"] not in {"reserved", "ambiguous"}:
                raise Conflict("only reserved or ambiguous attempts can be reconciled")
            db.execute(
                """UPDATE quota_reservations SET state=?,billed_micro_usd=?,
                credits_micro_usd=?,detail=?,settled_at=? WHERE id=?""",
                (state, billed, credits, detail, now_ms(), identifier),
            )
            value = dict(
                db.execute(
                    "SELECT * FROM quota_reservations WHERE id=?", (identifier,)
                ).fetchone()
            )
        value.pop("lease_token_hash", None)
        value["cache_hit"] = bool(value["cache_hit"])
        return value

    def quota_summary(self) -> dict[str, Any]:
        timestamp = now_ms()
        with self.transaction() as db:
            accounts = [dict(row) for row in db.execute("SELECT * FROM provider_accounts ORDER BY account_key")]
            for account in accounts:
                self._ensure_scheduled_policy(db, account["account_key"], timestamp)
            accounts_by_key = {account["account_key"]: account for account in accounts}
            policies = [dict(row) for row in db.execute("SELECT * FROM quota_policies ORDER BY account_key,revision DESC")]
            for policy in policies:
                policy["usage"] = self._policy_usage(
                    db, policy, accounts_by_key[policy["account_key"]], timestamp
                )
                policy["active"] = (
                    policy["window_start"] <= timestamp < policy["window_end"]
                    and (
                        policy["superseded_at"] is None
                        or timestamp < policy["superseded_at"]
                    )
                )
            usage = [
                dict(row)
                for row in db.execute(
                    """SELECT account_key,state,
                    COALESCE(list_currency,(
                        SELECT currency FROM provider_accounts
                        WHERE provider_accounts.account_key=quota_reservations.account_key
                    )) list_currency,
                    COUNT(*) attempts,
                    COALESCE(SUM(reserved_requests),0) requests,
                    COALESCE(SUM(reserved_pages),0) pages,
                    COALESCE(SUM(reserved_estimated_micro_usd),0) estimated_micro_usd,
                    COALESCE(SUM(list_micro_usd),0) list_micro_usd,
                    COALESCE(SUM(billed_micro_usd),0) billed_micro_usd,
                    COALESCE(SUM(credits_micro_usd),0) credits_micro_usd
                    FROM quota_reservations
                    GROUP BY account_key,state,list_currency
                    ORDER BY account_key,state,list_currency"""
                )
            ]
            schedules = [
                dict(row)
                for row in db.execute(
                    """SELECT quota_schedules.*,provider_accounts.currency
                    FROM quota_schedules JOIN provider_accounts USING(account_key)
                    ORDER BY quota_schedules.account_key"""
                )
            ]
            for schedule in schedules:
                schedule["enabled"] = bool(schedule["enabled"])
            waiting = int(
                db.execute(
                    "SELECT COUNT(*) FROM jobs WHERE not_before>? AND blocked_reason IS NOT NULL",
                    (timestamp,),
                ).fetchone()[0]
            )
            ambiguous = [
                dict(row)
                for row in db.execute(
                    """SELECT id,source_key,recipe_digest,account_key,checkpoint_key,state,
                    reserved_requests,reserved_pages,reserved_estimated_micro_usd,detail,
                    created_at,reconcile_by FROM quota_reservations
                    WHERE state='ambiguous' ORDER BY created_at DESC LIMIT 200"""
                )
            ]
            overrides = [
                dict(row)
                for row in db.execute(
                    """SELECT * FROM job_quota_overrides WHERE revoked_at IS NULL
                    AND consumed_by IS NULL AND expires_at>? ORDER BY created_at DESC LIMIT 200""",
                    (timestamp,),
                )
            ]
            snapshots = [
                dict(row)
                for row in db.execute(
                    """SELECT * FROM provider_usage_snapshots
                    ORDER BY observed_at DESC,created_at DESC LIMIT 500"""
                )
            ]
            fx_rates = [
                dict(row)
                for row in db.execute(
                    """SELECT * FROM provider_fx_rates
                    ORDER BY observed_at DESC,created_at DESC LIMIT 500"""
                )
            ]
        for account in accounts:
            account["enabled"] = bool(account["enabled"])
        return {"accounts": accounts, "schedules": schedules,
                "policies": policies, "usage": usage,
                "ambiguous": ambiguous, "overrides": overrides,
                "provider_usage_snapshots": snapshots,
                "provider_fx_rates": fx_rates,
                "waiting_jobs": waiting, "generated_at": timestamp}

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
        input_kinds = sorted(
            {str(value) for value in capability.get("input_kinds", ["source"]) if value}
        )
        if not digest or not media_types:
            raise ValueError("each worker capability needs recipe_digest and media_types")
        if not input_kinds or any(
            value not in {"source", "artifact"} for value in input_kinds
        ):
            raise ValueError("capability input_kinds must contain source and/or artifact")
        if (
            digest.startswith("blake3:")
            and blake3_bytes(canonical_json_bytes(recipe)) != digest
        ):
            raise ValueError("tagged recipe_digest does not match canonical recipe JSON")
        provider_account = capability.get("provider_account")
        provider = capability.get("provider")
        if provider_account is not None:
            provider_account = str(provider_account).strip().lower()
            provider = str(provider or "").strip().lower()
            if not provider_account or not provider:
                raise ValueError("provider capabilities need provider and provider_account")
        elif provider is not None:
            raise ValueError("provider requires provider_account")
        claim_unassigned = capability.get("claim_unassigned", True)
        if not isinstance(claim_unassigned, bool):
            raise ValueError("capability claim_unassigned must be a boolean")
        return {
            "recipe_digest": digest,
            "backend": backend,
            "recipe": recipe,
            "media_types": media_types,
            "input_kinds": input_kinds,
            "artifact_type": str(capability.get("artifact_type") or "mdaf/v1"),
            "provider_account": provider_account,
            "provider": provider,
            "claim_unassigned": claim_unassigned,
        }

    def register_capabilities(self, worker_id: str, capabilities: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
        normalized = [self._normalize_capability(value) for value in capabilities]
        timestamp = now_ms()
        with self.transaction() as db:
            db.execute("DELETE FROM worker_recipes WHERE worker_id=?", (worker_id,))
            for value in normalized:
                db.execute("""INSERT INTO recipes(recipe_digest,backend,recipe_json,media_types_json,artifact_type,last_seen,input_kinds_json,provider_account,provider)
                    VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(recipe_digest) DO UPDATE SET backend=excluded.backend,
                    recipe_json=excluded.recipe_json,media_types_json=excluded.media_types_json,
                    artifact_type=excluded.artifact_type,last_seen=excluded.last_seen,
                    input_kinds_json=excluded.input_kinds_json,
                    provider_account=excluded.provider_account,provider=excluded.provider""",
                    (value["recipe_digest"], value["backend"], json.dumps(value["recipe"], sort_keys=True),
                     json.dumps(value["media_types"]), value["artifact_type"], timestamp,
                     json.dumps(value["input_kinds"]), value["provider_account"], value["provider"]))
                db.execute("""INSERT INTO worker_recipes(worker_id,recipe_digest,last_seen,
                    input_kinds_json,provider_account,provider,claim_unassigned)
                    VALUES(?,?,?,?,?,?,?)""",
                           (worker_id, value["recipe_digest"], timestamp,
                            json.dumps(value["input_kinds"]), value["provider_account"],
                            value["provider"], int(value["claim_unassigned"])))
        return normalized

    def recipes(self, media_type: str | None = None) -> list[dict[str, Any]]:
        with self.connect() as db:
            rows = list(db.execute("""SELECT r.*,COUNT(w.worker_id) worker_count FROM recipes r
                LEFT JOIN worker_recipes wr USING(recipe_digest)
                LEFT JOIN workers w ON w.worker_id=wr.worker_id AND w.revoked=0 AND w.status!='offline'
                GROUP BY r.recipe_digest
                ORDER BY r.backend,r.recipe_digest"""))
        result = []
        for row in rows:
            value = dict(row)
            value["recipe"] = json.loads(value.pop("recipe_json"))
            value["media_types"] = json.loads(value.pop("media_types_json"))
            value["input_kinds"] = json.loads(value.pop("input_kinds_json"))
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
                lease_expires_at=NULL,progress_json=NULL,error_message=NULL,not_before=NULL,
                blocked_reason=NULL,updated_at=?{retry_sql}
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
            db.execute("DELETE FROM quota_reservations WHERE source_key=?", (key,))
            db.execute("DELETE FROM job_quota_overrides WHERE source_key=?", (key,))
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
        db.execute(
            """UPDATE quota_reservations SET state='ambiguous',detail=COALESCE(detail,'job lease expired before settlement')
            WHERE state='reserved' AND EXISTS (
              SELECT 1 FROM jobs j WHERE j.source_key=quota_reservations.source_key
              AND j.recipe_digest=quota_reservations.recipe_digest
              AND j.worker_id=quota_reservations.worker_id AND j.status='processing'
              AND j.lease_expires_at<?
            )""",
            (timestamp,),
        )
        cursor = db.execute("""UPDATE jobs SET status='todo',worker_id=NULL,lease_token=NULL,lease_expires_at=NULL,
            updated_at=? WHERE status='processing' AND lease_expires_at<?""", (timestamp, timestamp))
        return cursor.rowcount

    def claim(self, worker_id: str, priorities: list[str], capabilities: list[Mapping[str, Any]]) -> dict[str, Any] | None:
        timestamp = now_ms()
        normalized = [self._normalize_capability(value) for value in capabilities]
        if not normalized:
            return None
        with self.transaction() as db:
            registered = {
                str(row["recipe_digest"]): row
                for row in db.execute(
                    """SELECT wr.recipe_digest,wr.input_kinds_json,
                    wr.claim_unassigned,r.backend,r.recipe_json,r.media_types_json,
                    r.artifact_type,wr.provider_account,wr.provider
                    FROM worker_recipes wr JOIN recipes r USING(recipe_digest)
                    WHERE wr.worker_id=?""",
                    (worker_id,),
                )
            }
            if registered:
                constrained = []
                for value in normalized:
                    registered_value = registered.get(value["recipe_digest"])
                    if registered_value is None:
                        continue
                    registered_media_types = json.loads(
                        registered_value["media_types_json"]
                    )
                    registered_input_kinds = json.loads(
                        registered_value["input_kinds_json"]
                    )
                    media_types = [
                        item
                        for item in value["media_types"]
                        if item in registered_media_types
                    ]
                    input_kinds = [
                        item
                        for item in value["input_kinds"]
                        if item in registered_input_kinds
                    ]
                    if not media_types or not input_kinds:
                        continue
                    constrained.append(
                        {
                            **value,
                            "backend": str(registered_value["backend"]),
                            "recipe": json.loads(registered_value["recipe_json"]),
                            "media_types": media_types,
                            "input_kinds": input_kinds,
                            "artifact_type": str(registered_value["artifact_type"]),
                            "provider_account": registered_value[
                                "provider_account"
                            ],
                            "provider": registered_value["provider"],
                            "claim_unassigned": bool(
                                registered_value["claim_unassigned"]
                            ),
                        }
                    )
                normalized = constrained
            normalized = [value for value in normalized if value["input_kinds"]]
            if not normalized:
                return None
            self.recover_expired(db)
            placeholders = ",".join("?" for _ in priorities) or "''"
            predicates: list[str] = []
            capability_params: list[Any] = []
            for capability in normalized:
                media_placeholders = ",".join("?" for _ in capability["media_types"])
                input_placeholders = ",".join("?" for _ in capability["input_kinds"])
                recipe_predicate = (
                    "(j.recipe_digest IS NULL OR j.recipe_digest=?)"
                    if capability["claim_unassigned"]
                    else "j.recipe_digest=?"
                )
                predicates.append(
                    f"(s.media_type IN ({media_placeholders}) "
                    f"AND j.input_kind IN ({input_placeholders}) "
                    f"AND {recipe_predicate})"
                )
                capability_params.extend(capability["media_types"])
                capability_params.extend(capability["input_kinds"])
                capability_params.append(capability["recipe_digest"])
            row = db.execute(f"""SELECT j.source_key FROM jobs j JOIN sources s USING(source_key)
                WHERE j.status='todo' AND (j.not_before IS NULL OR j.not_before<=?)
                AND j.priority IN ({placeholders}) AND ({' OR '.join(predicates)})
                ORDER BY CASE j.priority WHEN '1_urgent' THEN 1 WHEN '2_high' THEN 2 WHEN '3_normal' THEN 3 ELSE 4 END,j.created_at LIMIT 1""",
                (timestamp, *priorities, *capability_params)).fetchone()
            if not row:
                return None
            key = str(row[0]); lease = secrets.token_urlsafe(24)
            media_type, requested_recipe, input_kind = db.execute("""SELECT s.media_type,j.recipe_digest,j.input_kind FROM jobs j
                JOIN sources s USING(source_key) WHERE j.source_key=?""", (key,)).fetchone()
            selected = next(value for value in normalized if media_type in value["media_types"] and
                            input_kind in value["input_kinds"] and
                            (requested_recipe == value["recipe_digest"] or
                             (requested_recipe is None and value["claim_unassigned"])))
            db.execute("""UPDATE jobs SET status='processing',worker_id=?,lease_token=?,lease_expires_at=?,
                recipe_digest=COALESCE(recipe_digest,?),recipe_json=COALESCE(recipe_json,?),
                not_before=NULL,blocked_reason=NULL,updated_at=? WHERE source_key=?""",
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

    def artifact_by_id(self, artifact_id: int) -> dict[str, Any] | None:
        with self.connect() as db:
            row = db.execute(
                "SELECT * FROM artifacts WHERE id=?", (artifact_id,)
            ).fetchone()
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
                db.execute("""UPDATE jobs SET status='done',recipe_digest=?,input_kind='source',
                    input_artifact_id=NULL,parent_recipe_digest=NULL,not_before=NULL,
                    blocked_reason=NULL,updated_at=? WHERE source_key=?""",
                    (recipe, timestamp, key))
                action = "selected"
            else:
                db.execute("""UPDATE jobs SET status='todo',recipe_digest=?,recipe_json=NULL,worker_id=NULL,
                    lease_token=NULL,lease_expires_at=NULL,error_message=NULL,input_kind='source',
                    input_artifact_id=NULL,parent_recipe_digest=NULL,completed_at=NULL,done_seq=NULL,
                    not_before=NULL,blocked_reason=NULL,updated_at=? WHERE source_key=?""", (recipe, timestamp, key))
                action = "queued"
        return {"action": action, "job": self.get_job(key)}

    @staticmethod
    def _recipe_from_db(db: sqlite3.Connection, digest: str) -> dict[str, Any]:
        row = db.execute(
            "SELECT recipe_json FROM recipes WHERE recipe_digest=?", (digest,)
        ).fetchone()
        if row:
            value = json.loads(row[0])
            if isinstance(value, dict):
                return value
        return load_known_recipe(digest)

    def plan_reprocessing(
        self,
        target_recipe: str,
        source_recipe: str,
        *,
        source_keys: list[str] | None = None,
        execute: bool = False,
        priority: str | None = None,
    ) -> dict[str, Any]:
        """Plan or atomically queue artifact-input derivatives."""
        timestamp = now_ms()
        with self.transaction() as db:
            target_row = db.execute(
                "SELECT recipe_json,enabled FROM recipes WHERE recipe_digest=?",
                (target_recipe,),
            ).fetchone()
            if not target_row:
                raise KeyError(target_recipe)
            if not bool(target_row["enabled"]):
                raise Conflict("target recipe is retired")
            target_definition = json.loads(target_row["recipe_json"])
            source_definition = self._recipe_from_db(db, source_recipe)
            assert_reprocessable(source_definition, target_definition)

            filters = ["a.recipe_digest=?"]
            params: list[Any] = [source_recipe]
            requested_count = None
            if source_keys is not None:
                source_keys = list(dict.fromkeys(source_keys))
                requested_count = len(source_keys)
                if not source_keys:
                    return {
                        "target_recipe_digest": target_recipe,
                        "source_recipe_digest": source_recipe,
                        "eligible": 0,
                        "already_present": 0,
                        "processing": 0,
                        "queued": 0,
                        "requested": 0,
                        "not_found": 0,
                        "eligible_source_keys": [],
                    }
                placeholders = ",".join("?" for _ in source_keys)
                filters.append(f"a.source_key IN ({placeholders})")
                params.extend(source_keys)
            rows = list(
                db.execute(
                    f"""SELECT a.id,a.source_key,j.status,
                        EXISTS(SELECT 1 FROM artifacts target
                          WHERE target.source_key=a.source_key
                          AND target.recipe_digest=?) AS target_exists
                        FROM artifacts a JOIN jobs j USING(source_key)
                        WHERE {' AND '.join(filters)} ORDER BY a.source_key""",
                    (target_recipe, *params),
                )
            )
            already = [row for row in rows if row["target_exists"]]
            processing = [
                row for row in rows if not row["target_exists"] and row["status"] == "processing"
            ]
            eligible = [
                row for row in rows if not row["target_exists"] and row["status"] != "processing"
            ]
            if execute:
                for row in eligible:
                    fields = "priority=?," if priority is not None else ""
                    values: list[Any] = [priority] if priority is not None else []
                    db.execute(
                        f"""UPDATE jobs SET {fields}status='todo',recipe_digest=?,recipe_json=?,
                            worker_id=NULL,lease_token=NULL,lease_expires_at=NULL,
                            progress_json=NULL,error_message=NULL,retry_count=0,
                            completed_at=NULL,done_seq=NULL,input_kind='artifact',
                            input_artifact_id=?,parent_recipe_digest=?,not_before=NULL,
                            blocked_reason=NULL,updated_at=?
                            WHERE source_key=?""",
                        (
                            *values,
                            target_recipe,
                            json.dumps(target_definition, sort_keys=True),
                            row["id"],
                            source_recipe,
                            timestamp,
                            row["source_key"],
                        ),
                    )
            return {
                "target_recipe_digest": target_recipe,
                "source_recipe_digest": source_recipe,
                "eligible": len(eligible),
                "already_present": len(already),
                "processing": len(processing),
                "queued": len(eligible) if execute else 0,
                "requested": requested_count,
                "not_found": (
                    requested_count - len(rows)
                    if requested_count is not None
                    else None
                ),
                "eligible_source_keys": [str(row["source_key"]) for row in eligible],
            }

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
