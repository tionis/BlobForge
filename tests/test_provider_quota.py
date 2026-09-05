import hashlib
from datetime import datetime, timezone

import httpx
import pytest

import blobforge.server.database as database_module
from blobforge.converters.contract import (
    PROVIDER_ATTEMPT_CONTRACT,
    PROVIDER_PROBE_CONTRACT,
)
from blobforge.server.app import create_app
from blobforge.server.config import ServerSettings
from blobforge.server.database import Conflict, Database, monthly_quota_window, now_ms


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _capability():
    return {
        "backend": "hosted-test",
        "recipe_digest": "hosted-recipe-v1",
        "recipe": {"engine": "hosted-test"},
        "media_types": ["application/pdf"],
        "artifact_type": "mdaf/v1",
        "input_kinds": ["source"],
        "provider": "test-provider",
        "provider_account": "test:primary",
    }


def _enqueue(database, key):
    database.enqueue(
        key,
        {
            "digest_algorithm": "sha256",
            "digest": key,
            "media_type": "application/pdf",
            "original_name": f"{key}.pdf",
            "priority": "3_normal",
        },
    )


def _claim(database, key):
    job = database.claim("hosted", ["3_normal"], [_capability()])
    assert job is not None and job["hash"] == key
    return job


def _probe(job, *, cache_hit=False):
    return {
        "contract": PROVIDER_PROBE_CONTRACT,
        "lease_token": job["lease_token"],
        "provider": "test-provider",
        "account_key": "test:primary",
        "checkpoint_key": f"checkpoint:{job['hash']}",
        "cache_hit": cache_hit,
        "requests": 0 if cache_hit else 1,
        "pages": 0 if cache_hit else 8,
        "estimated_micro_usd": 0 if cache_hit else 32_000,
    }


def _database(tmp_path):
    database = Database(tmp_path / "state.sqlite3", lease_seconds=60, max_retries=3)
    database.bootstrap_workers({"hosted": "worker-secret"})
    database.register_capabilities("hosted", [_capability()])
    database.configure_provider_account(
        "test:primary", "test-provider", concurrency_limit=1
    )
    timestamp = now_ms()
    database.create_quota_policy(
        "test:primary",
        window_start=timestamp - 1_000,
        window_end=timestamp + 86_400_000,
        label="one request",
        limit_requests=1,
        limit_pages=8,
        limit_estimated_micro_usd=32_000,
    )
    return database


def _epoch(value: str) -> int:
    return round(datetime.fromisoformat(value).astimezone(timezone.utc).timestamp() * 1000)


def test_monthly_quota_window_uses_local_reset_midnight_across_dst():
    assert monthly_quota_window(
        _epoch("2026-08-31T12:00:00+00:00"),
        reset_day=1,
        timezone_name="UTC",
    ) == (
        _epoch("2026-08-01T00:00:00+00:00"),
        _epoch("2026-09-01T00:00:00+00:00"),
    )
    assert monthly_quota_window(
        _epoch("2026-08-31T12:00:00+00:00"),
        reset_day=28,
        timezone_name="Europe/Berlin",
    ) == (
        _epoch("2026-08-28T00:00:00+02:00"),
        _epoch("2026-09-28T00:00:00+02:00"),
    )
    assert monthly_quota_window(
        _epoch("2026-10-30T12:00:00+00:00"),
        reset_day=28,
        timezone_name="Europe/Berlin",
    ) == (
        _epoch("2026-10-28T00:00:00+01:00"),
        _epoch("2026-11-28T00:00:00+01:00"),
    )


def test_monthly_schedule_materializes_once_and_enforces_account_currency(tmp_path):
    database = Database(tmp_path / "state.sqlite3", lease_seconds=60, max_retries=3)
    database.bootstrap_workers({"hosted": "worker-secret"})
    database.register_capabilities("hosted", [_capability()])
    database.configure_provider_account(
        "test:primary", "test-provider", currency="EUR", concurrency_limit=1
    )
    database.configure_quota_schedule(
        "test:primary",
        timezone_name="Europe/Berlin",
        reset_day=28,
        label="monthly paid allowance",
        limit_estimated_micro_usd=12_750_000,
        limit_billed_micro_usd=12_750_000,
    )
    first = database.quota_summary()
    second = database.quota_summary()
    assert len(first["policies"]) == len(second["policies"]) == 1
    assert first["accounts"][0]["currency"] == "EUR"
    assert first["schedules"][0]["reset_day"] == 28
    assert first["policies"][0]["currency"] == "EUR"
    assert first["policies"][0]["limit_billed_micro_usd"] == 12_750_000

    _enqueue(database, "8" * 64)
    job = _claim(database, "8" * 64)
    with pytest.raises(Conflict, match="probe currency"):
        database.reserve_quota(job["hash"], "hosted", job["lease_token"], _probe(job))
    euro_probe = {**_probe(job), "currency": "EUR"}
    assert database.reserve_quota(
        job["hash"], "hosted", job["lease_token"], euro_probe
    )["authorized"]


def test_monthly_schedule_realigns_boundary_without_resetting_used_allowance(
    tmp_path, monkeypatch
):
    timestamp = _epoch("2026-08-31T00:30:00+00:00")
    monkeypatch.setattr("blobforge.server.database.now_ms", lambda: timestamp)
    database = Database(tmp_path / "state.sqlite3", lease_seconds=60, max_retries=3)
    database.bootstrap_workers({"hosted": "worker-secret"})
    database.register_capabilities("hosted", [_capability()])
    database.configure_provider_account(
        "test:primary", "test-provider", currency="EUR", concurrency_limit=2
    )
    database.configure_quota_schedule(
        "test:primary",
        timezone_name="Europe/Berlin",
        reset_day=28,
        label="monthly paid allowance",
        limit_estimated_micro_usd=32_000,
        limit_billed_micro_usd=32_000,
    )
    _enqueue(database, "a" * 64)
    first_job = _claim(database, "a" * 64)
    first = database.reserve_quota(
        first_job["hash"],
        "hosted",
        first_job["lease_token"],
        {**_probe(first_job), "currency": "EUR"},
    )
    assert first["authorized"]

    _enqueue(database, "b" * 64)
    second_job = _claim(database, "b" * 64)
    denied_under_old_boundary = database.reserve_quota(
        second_job["hash"],
        "hosted",
        second_job["lease_token"],
        {**_probe(second_job), "currency": "EUR"},
    )
    assert not denied_under_old_boundary["authorized"]
    assert denied_under_old_boundary["not_before"] == _epoch(
        "2026-09-28T00:00:00+02:00"
    )

    schedule = database.configure_quota_schedule(
        "test:primary",
        timezone_name="Europe/Berlin",
        reset_day=1,
        label="monthly paid allowance",
        limit_estimated_micro_usd=32_000,
        limit_billed_micro_usd=32_000,
    )
    assert len(schedule["superseded_policy_ids"]) == 1
    assert schedule["released_quota_delays"] == 1
    assert database.get_job("b" * 64)["not_before"] is None
    summary = database.quota_summary()
    old = next(policy for policy in summary["policies"] if policy["superseded_at"])
    replacement = next(
        policy for policy in summary["policies"] if policy["id"] == old["superseded_by"]
    )
    assert not old["active"]
    assert replacement["active"]
    assert replacement["window_start"] == _epoch("2026-08-01T00:00:00+02:00")
    assert replacement["window_end"] == _epoch("2026-09-01T00:00:00+02:00")
    assert replacement["usage"]["estimated_micro_usd"] == 32_000

    second_job = _claim(database, "b" * 64)
    denied = database.reserve_quota(
        second_job["hash"],
        "hosted",
        second_job["lease_token"],
        {**_probe(second_job), "currency": "EUR"},
    )
    assert not denied["authorized"]
    assert denied["reason"] == "quota exhausted"
    assert denied["not_before"] == _epoch("2026-09-01T00:00:00+02:00")


def test_monthly_schedule_rejects_boundary_that_omits_current_usage(tmp_path, monkeypatch):
    timestamp = _epoch("2026-08-31T00:30:00+00:00")
    monkeypatch.setattr("blobforge.server.database.now_ms", lambda: timestamp)
    database = Database(tmp_path / "state.sqlite3", lease_seconds=60, max_retries=3)
    database.configure_provider_account("test:primary", "test-provider", currency="EUR")
    database.configure_quota_schedule(
        "test:primary",
        timezone_name="Europe/Berlin",
        reset_day=1,
        limit_estimated_micro_usd=32_000,
    )
    with pytest.raises(Conflict, match="omit usage"):
        database.configure_quota_schedule(
            "test:primary",
            timezone_name="Europe/Berlin",
            reset_day=28,
            limit_estimated_micro_usd=32_000,
        )
    summary = database.quota_summary()
    assert summary["schedules"][0]["reset_day"] == 1
    assert len(summary["policies"]) == 1


def test_explicit_only_hosted_capability_never_claims_unassigned_work(tmp_path):
    database = Database(tmp_path / "state.sqlite3", lease_seconds=60, max_retries=3)
    database.bootstrap_workers({"hosted": "worker-secret"})
    capability = {**_capability(), "claim_unassigned": False}
    registered = database.register_capabilities("hosted", [capability])
    assert registered[0]["claim_unassigned"] is False

    unassigned_key = "0" * 64
    exact_key = "1" * 64
    _enqueue(database, unassigned_key)
    assert database.claim("hosted", ["3_normal"], [capability]) is None

    _enqueue(database, exact_key)
    database.request_conversion(exact_key, capability["recipe_digest"])
    job = database.claim("hosted", ["3_normal"], [capability])
    assert job is not None
    assert job["hash"] == exact_key
    assert database.get_job(unassigned_key)["status"] == "todo"


def test_claim_cannot_broaden_registered_explicit_only_capability(tmp_path):
    database = Database(tmp_path / "state.sqlite3", lease_seconds=60, max_retries=3)
    database.bootstrap_workers({"hosted": "worker-secret"})
    explicit_only = {**_capability(), "claim_unassigned": False}
    database.register_capabilities("hosted", [explicit_only])
    _enqueue(database, "2" * 64)

    broadened = {**explicit_only, "claim_unassigned": True}
    assert database.claim("hosted", ["3_normal"], [broadened]) is None


@pytest.mark.parametrize("status", ["failed", "dead", "processing", "done"])
def test_quota_allowance_requires_explicitly_queued_job(tmp_path, status):
    database = _database(tmp_path)
    _enqueue(database, "a" * 64)
    _claim(database, "a" * 64)
    with database.transaction() as db:
        db.execute("UPDATE jobs SET status=?", (status,))
    with pytest.raises(Conflict, match="queued job"):
        database.create_quota_override(
            "a" * 64, "hosted-recipe-v1", extra_requests=1, extra_pages=0,
            extra_micro_usd=0, reason="test", actor="admin:test",
            expires_at=now_ms() + 3_600_000,
        )
    assert database.quota_records("a" * 64)["overrides"] == []


def test_quota_exhaustion_defers_without_retry_and_bounded_override_releases(tmp_path):
    database = _database(tmp_path)
    _enqueue(database, "a" * 64)
    first = _claim(database, "a" * 64)
    authorized = database.reserve_quota(
        first["hash"], "hosted", first["lease_token"], _probe(first)
    )
    reservation = authorized["reservation"]
    assert authorized["authorized"]
    committed = database.settle_quota(
        reservation["id"],
        "hosted",
        {
            "contract": PROVIDER_ATTEMPT_CONTRACT,
            "reservation_id": reservation["id"],
            "provider": "test-provider",
            "account_key": "test:primary",
            "checkpoint_key": f"checkpoint:{first['hash']}",
            "state": "committed",
            "requests": 1,
            "pages": 8,
            "list_micro_usd": 32_000,
            "billed_micro_usd": 0,
            "credits_micro_usd": 32_000,
        },
    )
    assert committed["state"] == "committed"
    _enqueue(database, "b" * 64)
    second = _claim(database, "b" * 64)
    denied = database.reserve_quota(
        second["hash"], "hosted", second["lease_token"], _probe(second)
    )
    assert not denied["authorized"]
    blocked = database.get_job(second["hash"])
    assert blocked["status"] == "todo"
    assert blocked["retry_count"] == 0
    assert blocked["not_before"] is not None

    insufficient = database.create_quota_override(
        second["hash"], "hosted-recipe-v1", extra_requests=1,
        extra_pages=0, extra_micro_usd=0, reason="request only", actor="admin:test",
        expires_at=now_ms() + 3_600_000,
    )
    attempted = _claim(database, second["hash"])
    assert not database.reserve_quota(
        attempted["hash"], "hosted", attempted["lease_token"], _probe(attempted)
    )["authorized"]
    override = database.create_quota_override(
        second["hash"],
        "hosted-recipe-v1",
        extra_requests=1,
        extra_pages=8,
        extra_micro_usd=32_000,
        reason="finish the selected evaluation canary",
        actor="admin:test",
        expires_at=now_ms() + 3_600_000,
    )
    assert override["consumed_by"] is None
    retried = _claim(database, second["hash"])
    overage = database.reserve_quota(
        retried["hash"], "hosted", retried["lease_token"], _probe(retried)
    )
    assert overage["authorized"]
    assert overage["reservation"]["override_id"] == override["id"]
    records = database.quota_records(second["hash"])
    records_by_id = {row["id"]: row for row in records["overrides"]}
    assert records_by_id[override["id"]]["consumed_by"] == overage["reservation"]["id"]
    assert records_by_id[insufficient["id"]]["revoked_at"] is not None
    assert records_by_id[insufficient["id"]]["consumed_by"] is None


@pytest.mark.parametrize('budget,authorized', [(40000, True), (1, False)])
def test_fx_outage_falls_back_but_does_not_bypass_budget(tmp_path, budget, authorized):
    database = Database(tmp_path / 'state.sqlite3', lease_seconds=60, max_retries=3)
    database.bootstrap_workers({'hosted': 'worker-secret'})
    database.register_capabilities('hosted', [_capability()])
    database.configure_provider_account('test:primary', 'test-provider', currency='EUR')
    timestamp = now_ms()
    database.create_quota_policy('test:primary', window_start=timestamp-1000,
        window_end=timestamp+86400000, label='fallback test', limit_estimated_micro_usd=budget)
    key = '8' * 64
    _enqueue(database, key)
    job = _claim(database, key)
    response = database.reserve_quota(key, 'hosted', job['lease_token'],
        {**_probe(job), 'currency': 'EUR', 'estimate_currency': 'USD'})
    assert response['authorized'] is authorized
    assert 'FX rate' not in response.get('reason', '')
    if authorized:
        assert response['reservation']['fx_rate_id']
        assert response['reservation']['reserved_estimated_micro_usd'] > 0
    assert database.quota_summary()['fx_status']['warnings']


def test_cross_currency_operator_fx_retains_both_amounts(
    tmp_path,
):
    database = Database(tmp_path / "state.sqlite3", lease_seconds=60, max_retries=3)
    database.bootstrap_workers({"hosted": "worker-secret"})
    database.register_capabilities("hosted", [_capability()])
    database.configure_provider_account(
        "test:primary", "test-provider", currency="EUR", concurrency_limit=1
    )
    timestamp = now_ms()
    database.create_quota_policy(
        "test:primary",
        window_start=timestamp - 1_000,
        window_end=timestamp + 86_400_000,
        label="EUR allowance",
        limit_estimated_micro_usd=30_000,
        limit_billed_micro_usd=30_000,
    )
    key = "9" * 64
    _enqueue(database, key)
    job = _claim(database, key)
    probe = {
        **_probe(job),
        "currency": "EUR",
        "estimate_currency": "USD",
    }
    rate = database.record_provider_fx_rate(
        "test:primary",
        source_currency="USD",
        rate_numerator=9,
        rate_denominator=10,
        observed_at=timestamp,
        valid_until=timestamp + 3_600_000,
        source="operator supplied conservative rate",
        reason="bound the EUR allowance without relabeling USD list price",
        actor="admin:test",
    )
    assert rate["released_fx_delays"] == 0
    retried = job
    reservation = database.reserve_quota(
        key,
        "hosted",
        retried["lease_token"],
        {**probe, "lease_token": retried["lease_token"]},
    )["reservation"]
    assert reservation["estimate_currency"] == "USD"
    assert reservation["reserved_estimate_micro_units"] == 32_000
    assert reservation["reserved_estimated_micro_usd"] == 28_800
    assert reservation["fx_rate_id"] == rate["id"]

    settled = database.settle_quota(
        reservation["id"],
        "hosted",
        {
            "contract": PROVIDER_ATTEMPT_CONTRACT,
            "reservation_id": reservation["id"],
            "provider": "test-provider",
            "account_key": "test:primary",
            "currency": "EUR",
            "list_currency": "USD",
            "checkpoint_key": f"checkpoint:{key}",
            "state": "committed",
            "requests": 1,
            "pages": 8,
            "list_micro_usd": 32_000,
            "billed_micro_usd": None,
            "credits_micro_usd": None,
        },
    )
    assert settled["list_currency"] == "USD"
    summary = database.quota_summary()
    assert summary["provider_fx_rates"][0]["id"] == rate["id"]
    assert summary["usage"][0]["list_currency"] == "USD"
    assert summary["usage"][0]["estimated_micro_usd"] == 28_800


def test_manual_provider_snapshot_replaces_estimate_ceiling_without_rewriting_history(
    tmp_path,
):
    database = Database(tmp_path / "state.sqlite3", lease_seconds=60, max_retries=3)
    database.bootstrap_workers({"hosted": "worker-secret"})
    database.register_capabilities("hosted", [_capability()])
    database.configure_provider_account(
        "test:primary", "test-provider", currency="EUR", concurrency_limit=1
    )
    timestamp = now_ms()
    old_policy = database.create_quota_policy(
        "test:primary",
        window_start=timestamp - 3_600_000,
        window_end=timestamp + 86_400_000,
        label="subscription allowance",
        limit_estimated_micro_usd=40_000,
        limit_billed_micro_usd=40_000,
    )
    _enqueue(database, "1" * 64)
    first = _claim(database, "1" * 64)
    reserved = database.reserve_quota(
        first["hash"],
        "hosted",
        first["lease_token"],
        {**_probe(first), "currency": "EUR"},
    )["reservation"]
    database.settle_quota(
        reserved["id"],
        "hosted",
        {
            "contract": PROVIDER_ATTEMPT_CONTRACT,
            "reservation_id": reserved["id"],
            "provider": "test-provider",
            "account_key": "test:primary",
            "currency": "EUR",
            "checkpoint_key": f"checkpoint:{first['hash']}",
            "state": "committed",
            "requests": 1,
            "pages": 8,
            "list_micro_usd": 32_000,
            "billed_micro_usd": None,
            "credits_micro_usd": None,
        },
    )
    _enqueue(database, "2" * 64)
    second = _claim(database, "2" * 64)
    denied = database.reserve_quota(
        second["hash"],
        "hosted",
        second["lease_token"],
        {**_probe(second), "currency": "EUR"},
    )
    assert not denied["authorized"]
    assert {item["dimension"] for item in denied["exceeded"]} == {"estimated", "billed"}

    observed_at = now_ms()
    snapshot = database.record_provider_usage_snapshot(
        "test:primary",
        reported_billed_micro_usd=2_400,
        observed_at=observed_at,
        coverage_through=observed_at,
        reason="provider console reports EUR 0.0024 through the first purchase",
        actor="admin:test",
        activate_snapshot_accounting=True,
        snapshot_max_age_ms=3_600_000,
    )
    assert snapshot["replacement_policy_id"]
    assert snapshot["released_quota_delays"] == 1

    retried = _claim(database, second["hash"])
    authorized = database.reserve_quota(
        retried["hash"],
        "hosted",
        retried["lease_token"],
        {**_probe(retried), "currency": "EUR"},
    )
    assert authorized["authorized"]
    summary = database.quota_summary()
    active = next(policy for policy in summary["policies"] if policy["active"])
    historical = next(policy for policy in summary["policies"] if policy["id"] == old_policy["id"])
    assert historical["superseded_by"] == active["id"]
    assert active["limit_estimated_micro_usd"] is None
    assert active["limit_billed_micro_usd"] == 40_000
    assert active["usage"]["billed_basis"] == "provider_snapshot"
    assert active["usage"]["billed_exposure_micro_usd"] == 34_400
    assert active["usage"]["post_snapshot_estimated_micro_usd"] == 32_000
    assert sum(row["estimated_micro_usd"] for row in summary["usage"]) == 64_000
    assert summary["provider_usage_snapshots"][0]["reported_billed_micro_usd"] == 2_400


def test_snapshot_accounting_requires_a_fresh_monotonic_snapshot(tmp_path):
    database = Database(tmp_path / "state.sqlite3", lease_seconds=60, max_retries=3)
    database.bootstrap_workers({"hosted": "worker-secret"})
    database.register_capabilities("hosted", [_capability()])
    database.configure_provider_account("test:primary", "test-provider", currency="EUR")
    timestamp = now_ms()
    database.create_quota_policy(
        "test:primary",
        window_start=timestamp - 3_600_000,
        window_end=timestamp + 86_400_000,
        limit_estimated_micro_usd=100_000,
        limit_billed_micro_usd=100_000,
    )
    observed_at = timestamp - 20 * 60_000
    database.record_provider_usage_snapshot(
        "test:primary",
        reported_billed_micro_usd=10_000,
        observed_at=observed_at,
        coverage_through=observed_at,
        reason="stale console observation",
        actor="admin:test",
        activate_snapshot_accounting=True,
        snapshot_max_age_ms=15 * 60_000,
    )
    _enqueue(database, "3" * 64)
    job = _claim(database, "3" * 64)
    denied = database.reserve_quota(
        job["hash"],
        "hosted",
        job["lease_token"],
        {**_probe(job), "currency": "EUR"},
    )
    assert not denied["authorized"]
    assert denied["exceeded"][0]["dimension"] == "provider_snapshot"
    assert denied["exceeded"][0]["reason"] == "snapshot_stale"
    with pytest.raises(Conflict, match="observed_at must advance"):
        database.record_provider_usage_snapshot(
            "test:primary",
            reported_billed_micro_usd=11_000,
            observed_at=observed_at,
            coverage_through=observed_at,
            reason="duplicate observation",
            actor="admin:test",
            activate_snapshot_accounting=False,
            snapshot_max_age_ms=15 * 60_000,
        )


def test_exclusive_consumer_bootstraps_each_window_and_uses_internal_ledger(
    tmp_path, monkeypatch
):
    current = [_epoch("2026-08-31T12:00:00+00:00")]
    monkeypatch.setattr(database_module, "now_ms", lambda: current[0])
    database = Database(tmp_path / "state.sqlite3", lease_seconds=60, max_retries=3)
    database.bootstrap_workers({"hosted": "worker-secret"})
    database.register_capabilities("hosted", [_capability()])
    database.configure_provider_account("test:primary", "test-provider")
    database.configure_quota_schedule(
        "test:primary",
        timezone_name="UTC",
        reset_day=1,
        limit_estimated_micro_usd=100_000,
        limit_billed_micro_usd=100_000,
    )
    database.record_provider_usage_snapshot(
        "test:primary",
        reported_billed_micro_usd=10_000,
        observed_at=current[0],
        coverage_through=current[0],
        reason="August provider console observation",
        actor="admin:test",
        activate_snapshot_accounting=True,
        snapshot_max_age_ms=6 * 60 * 60 * 1000,
    )

    current[0] = _epoch("2026-09-08T12:00:00+00:00")
    _enqueue(database, "e" * 64)
    delayed = _claim(database, "e" * 64)
    denied = database.reserve_quota(
        delayed["hash"],
        "hosted",
        delayed["lease_token"],
        _probe(delayed),
    )
    assert not denied["authorized"]
    assert denied["exceeded"][0]["reason"] == "snapshot_missing"

    account = database.configure_provider_account(
        "test:primary", "test-provider", exclusive_consumer=True
    )
    assert account["exclusive_consumer"] is True
    retried = _claim(database, "e" * 64)
    authorized = database.reserve_quota(
        retried["hash"],
        "hosted",
        retried["lease_token"],
        _probe(retried),
    )
    assert authorized["authorized"]

    current[0] += 24 * 60 * 60 * 1000
    summary = database.quota_summary()
    september = next(
        policy
        for policy in summary["policies"]
        if policy["window_start"] == _epoch("2026-09-01T00:00:00+00:00")
        and not policy["superseded_at"]
    )
    snapshot = september["usage"]["snapshot"]
    assert september["usage"]["billed_basis"] == "provider_snapshot"
    assert september["usage"]["billed_exposure_micro_usd"] == 32_000
    assert september["usage"]["post_snapshot_estimated_micro_usd"] == 32_000
    assert snapshot["source"] == "automatic-exclusive-reset"
    assert snapshot["reported_billed_micro_usd"] == 0
    assert snapshot["coverage_through"] == september["window_start"]
    assert snapshot["freshness_exempt"] is True

    disabled = database.configure_provider_account(
        "test:primary", "test-provider", exclusive_consumer=False
    )
    assert disabled["exclusive_consumer"] is False
    september = next(
        policy
        for policy in database.quota_summary()["policies"]
        if policy["window_start"] == _epoch("2026-09-01T00:00:00+00:00")
        and not policy["superseded_at"]
    )
    assert september["usage"]["billed_basis"] == "snapshot_stale"


def test_exclusive_consumer_requires_provider_snapshot_accounting(tmp_path):
    database = Database(tmp_path / "state.sqlite3", lease_seconds=60, max_retries=3)
    database.configure_provider_account("test:primary", "test-provider")
    with pytest.raises(Conflict, match="requires provider-snapshot accounting"):
        database.configure_provider_account(
            "test:primary", "test-provider", exclusive_consumer=True
        )


def test_cache_hit_needs_no_policy_allowance_and_ambiguous_attempt_is_reconciled(tmp_path):
    database = Database(tmp_path / "state.sqlite3", lease_seconds=60, max_retries=3)
    database.bootstrap_workers({"hosted": "worker-secret"})
    database.register_capabilities("hosted", [_capability()])
    database.configure_provider_account("test:primary", "test-provider")
    _enqueue(database, "c" * 64)
    job = _claim(database, "c" * 64)
    cached = database.reserve_quota(
        job["hash"], "hosted", job["lease_token"], _probe(job, cache_hit=True)
    )
    assert cached["authorized"]
    assert cached["reservation"]["reserved_requests"] == 0
    settled = database.settle_quota(
        cached["reservation"]["id"],
        "hosted",
        {
            "contract": PROVIDER_ATTEMPT_CONTRACT,
            "reservation_id": cached["reservation"]["id"],
            "provider": "test-provider",
            "account_key": "test:primary",
            "checkpoint_key": f"checkpoint:{job['hash']}",
            "state": "cache_hit",
            "requests": 0,
            "pages": 0,
            "list_micro_usd": 0,
            "billed_micro_usd": 0,
            "credits_micro_usd": 0,
        },
    )
    assert settled["state"] == "committed"

    with database.transaction() as db:
        db.execute(
            "UPDATE quota_reservations SET state='ambiguous' WHERE id=?",
            (cached["reservation"]["id"],),
        )
    reconciled = database.reconcile_quota(
        cached["reservation"]["id"],
        state="released",
        detail="provider dashboard confirms no purchase",
    )
    assert reconciled["state"] == "released"


def test_checkpoint_resumes_the_original_ambiguous_reservation(tmp_path):
    database = _database(tmp_path)
    key = "d" * 64
    _enqueue(database, key)
    first = _claim(database, key)
    reservation = database.reserve_quota(
        key, "hosted", first["lease_token"], _probe(first)
    )["reservation"]
    with database.transaction() as db:
        db.execute(
            "UPDATE quota_reservations SET state='ambiguous' WHERE id=?",
            (reservation["id"],),
        )
    database.release(key, "hosted", first["lease_token"])
    second = _claim(database, key)
    resumed_probe = _probe(second)
    resumed_probe["cache_hit"] = True
    resumed_probe["requests"] = 0
    resumed_probe["pages"] = 0
    resumed_probe["estimated_micro_usd"] = 0
    resumed_probe["resume_reservation_id"] = reservation["id"]
    resumed = database.reserve_quota(
        key, "hosted", second["lease_token"], resumed_probe
    )
    assert resumed["resumed"] is True
    assert resumed["reservation"]["id"] == reservation["id"]
    settled = database.settle_quota(
        reservation["id"],
        "hosted",
        {
            "contract": PROVIDER_ATTEMPT_CONTRACT,
            "reservation_id": reservation["id"],
            "provider": "test-provider",
            "account_key": "test:primary",
            "checkpoint_key": f"checkpoint:{key}",
            "state": "committed",
            "requests": 1,
            "pages": 8,
            "list_micro_usd": 32_000,
            "billed_micro_usd": None,
            "credits_micro_usd": None,
        },
    )
    assert settled["state"] == "committed"
    assert database.quota_summary()["usage"][0]["attempts"] == 1


def test_unsettled_checkpoint_cannot_start_a_fresh_paid_reservation(tmp_path):
    database = _database(tmp_path)
    key = "9" * 64
    _enqueue(database, key)
    first = _claim(database, key)
    reservation = database.reserve_quota(
        key, "hosted", first["lease_token"], _probe(first)
    )["reservation"]
    database.reconcile_quota(
        reservation["id"],
        state="released",
        detail="operator released the old reservation",
    )
    database.release(key, "hosted", first["lease_token"])
    second = _claim(database, key)
    probe = _probe(second)
    probe["resume_reservation_id"] = reservation["id"]
    with pytest.raises(Conflict, match="cannot be resumed"):
        database.reserve_quota(key, "hosted", second["lease_token"], probe)


def test_provider_account_concurrency_defers_second_purchase(tmp_path):
    database = _database(tmp_path)
    # A second overlapping policy can raise budget without changing concurrency.
    timestamp = now_ms()
    database.create_quota_policy(
        "test:primary",
        window_start=timestamp - 1000,
        # Explicitly differ from the helper's one-day window, even in the same millisecond.
        window_end=timestamp + 2 * 86_400_000,
        label="larger concurrency test budget",
        limit_requests=10,
        limit_pages=100,
        limit_estimated_micro_usd=1_000_000,
    )
    first_key, second_key = "e" * 64, "f" * 64
    _enqueue(database, first_key)
    _enqueue(database, second_key)
    first = _claim(database, first_key)
    assert database.reserve_quota(
        first_key, "hosted", first["lease_token"], _probe(first)
    )["authorized"]
    second = _claim(database, second_key)
    denied = database.reserve_quota(
        second_key, "hosted", second["lease_token"], _probe(second)
    )
    assert denied["authorized"] is False
    assert "concurrency" in denied["reason"]
    assert database.get_job(second_key)["retry_count"] == 0


def test_rate_limit_releases_allowance_and_applies_shared_cooldown(tmp_path):
    database = _database(tmp_path)
    key = "7" * 64
    _enqueue(database, key)
    job = _claim(database, key)
    reservation = database.reserve_quota(
        key, "hosted", job["lease_token"], _probe(job)
    )["reservation"]
    settled = database.settle_quota(
        reservation["id"],
        "hosted",
        {
            "contract": PROVIDER_ATTEMPT_CONTRACT,
            "reservation_id": reservation["id"],
            "provider": "test-provider",
            "account_key": "test:primary",
            "checkpoint_key": f"checkpoint:{key}",
            "state": "rate_limited",
            "requests": 0,
            "pages": 0,
            "list_micro_usd": None,
            "billed_micro_usd": None,
            "credits_micro_usd": None,
            "retry_after_ms": 120_000,
        },
    )
    assert settled["state"] == "released"
    blocked = database.get_job(key)
    assert blocked["status"] == "todo"
    assert blocked["retry_count"] == 0
    summary = database.quota_summary()
    assert summary["accounts"][0]["cooldown_until"] == blocked["not_before"]
    assert "rate limited" in summary["accounts"][0]["cooldown_reason"]


@pytest.mark.anyio
async def test_quota_admin_and_worker_http_protocol(tmp_path):
    app = create_app(
        ServerSettings(
            data_dir=tmp_path,
            client_token="admin-secret",
            worker_tokens={"hosted": "worker-secret"},
            lease_seconds=60,
        )
    )
    admin = {"Authorization": "Bearer admin-secret"}
    worker = {"Authorization": "Bearer worker-secret"}
    source = b"%PDF-1.7\nquota protocol fixture\n"
    key = hashlib.sha256(source).hexdigest()
    timestamp = now_ms()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        unconfirmed = await client.post(
            f"/api/v1/admin/jobs/{'0' * 64}/quota-overrides",
            headers=admin,
            json={},
        )
        assert unconfirmed.status_code == 400
        assert "confirm=true" in unconfirmed.text
        account = await client.put(
            "/api/v1/admin/provider-accounts/test%3Aprimary",
            headers=admin,
            json={
                "provider": "test-provider",
                "enabled": True,
                "concurrency_limit": 1,
            },
        )
        assert account.status_code == 200
        unconfirmed_fx = await client.post(
            "/api/v1/admin/provider-fx-rates",
            headers=admin,
            json={"account_key": "test:primary"},
        )
        assert unconfirmed_fx.status_code == 400
        fx = await client.post(
            "/api/v1/admin/provider-fx-rates",
            headers=admin,
            json={
                "account_key": "test:primary",
                "source_currency": "EUR",
                "rate_numerator": 1_100_000,
                "rate_denominator": 1_000_000,
                "observed_at": timestamp,
                "valid_until": timestamp + 3_600_000,
                "source": "test reference rate",
                "reason": "exercise the immutable administrative FX contract",
                "confirm": True,
            },
        )
        assert fx.status_code == 200
        assert fx.json()["account_currency"] == "USD"
        schedule = await client.put(
            "/api/v1/admin/quota-schedules/test%3Aprimary",
            headers=admin,
            json={
                "timezone": "Europe/Berlin",
                "reset_day": 28,
                "label": "monthly test allowance",
                "limit_requests": 10,
            },
        )
        assert schedule.status_code == 200
        assert schedule.json()["reset_day"] == 28
        policy = await client.post(
            "/api/v1/admin/quota-policies",
            headers=admin,
            json={
                "account_key": "test:primary",
                "window_start": timestamp - 1000,
                "window_end": timestamp + 86_400_000,
                "limit_requests": 1,
                "limit_pages": 8,
                "limit_estimated_micro_usd": 32_000,
            },
        )
        assert policy.status_code == 200
        transfer = (
            await client.post(
                f"/api/v1/jobs/{key}/raw-upload-url",
                headers=admin,
                json={"digest_algorithm": "sha256", "media_type": "application/pdf"},
            )
        ).json()
        assert (await client.put(transfer["url"], content=source)).status_code == 200
        assert (
            await client.put(
                f"/api/v1/jobs/{key}",
                headers=admin,
                json={
                    "digest_algorithm": "sha256",
                    "digest": key,
                    "media_type": "application/pdf",
                    "priority": "3_normal",
                    "original_name": "quota.pdf",
                    "size_bytes": len(source),
                },
            )
        ).status_code == 200
        capability = _capability()
        registration = await client.post(
            "/api/v1/workers/register",
            headers=worker,
            json={"worker_id": "hosted", "capabilities": [capability]},
        )
        assert registration.status_code == 200
        claim = (
            await client.post(
                "/api/v1/jobs/claim",
                headers=worker,
                json={
                    "worker_id": "hosted",
                    "priorities": ["3_normal"],
                    "capabilities": [capability],
                },
            )
        ).json()["job"]
        invalid_probe = _probe(claim)
        invalid_probe["contract"] = "unsupported"
        assert (
            await client.post(
                f"/api/v1/jobs/{key}/quota-reservation",
                headers=worker,
                json=invalid_probe,
            )
        ).status_code == 400
        reservation_response = await client.post(
            f"/api/v1/jobs/{key}/quota-reservation",
            headers=worker,
            json=_probe(claim),
        )
        assert reservation_response.status_code == 200
        reservation = reservation_response.json()["reservation"]
        invalid_report = {
            "contract": "unsupported",
            "reservation_id": reservation["id"],
            "provider": "test-provider",
            "account_key": "test:primary",
            "checkpoint_key": f"checkpoint:{key}",
            "state": "committed",
            "requests": 1,
            "pages": 8,
        }
        assert (
            await client.post(
                f"/api/v1/quota-reservations/{reservation['id']}/settle",
                headers=worker,
                json=invalid_report,
            )
        ).status_code == 400
        settled = await client.post(
            f"/api/v1/quota-reservations/{reservation['id']}/settle",
            headers=worker,
            json={
                "contract": PROVIDER_ATTEMPT_CONTRACT,
                "reservation_id": reservation["id"],
                "provider": "test-provider",
                "account_key": "test:primary",
                "checkpoint_key": f"checkpoint:{key}",
                "state": "committed",
                "requests": 1,
                "pages": 8,
                "list_micro_usd": 32_000,
                "billed_micro_usd": 0,
                "credits_micro_usd": 32_000,
            },
        )
        assert settled.status_code == 200
        detail = await client.get(f"/api/v1/admin/jobs/{key}", headers=admin)
        assert detail.json()["quota"]["reservations"][0]["state"] == "committed"
        summary = await client.get("/api/v1/admin/quotas", headers=admin)
        assert summary.status_code == 200
        assert summary.json()["usage"][0]["credits_micro_usd"] == 32_000


@pytest.mark.anyio
async def test_admin_can_record_confirmed_manual_provider_usage_snapshot(tmp_path):
    app = create_app(
        ServerSettings(
            data_dir=tmp_path,
            client_token="admin-secret",
            worker_tokens={},
            lease_seconds=60,
        )
    )
    admin = {"Authorization": "Bearer admin-secret"}
    timestamp = now_ms()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        account = await client.put(
            "/api/v1/admin/provider-accounts/test%3Aprimary",
            headers=admin,
            json={"provider": "test-provider", "currency": "EUR"},
        )
        assert account.status_code == 200
        policy = await client.post(
            "/api/v1/admin/quota-policies",
            headers=admin,
            json={
                "account_key": "test:primary",
                "window_start": timestamp - 3_600_000,
                "window_end": timestamp + 86_400_000,
                "limit_estimated_micro_usd": 12_750_000,
                "limit_billed_micro_usd": 12_750_000,
            },
        )
        assert policy.status_code == 200
        body = {
            "account_key": "test:primary",
            "reported_billed_micro_usd": 960_000,
            "observed_at": timestamp,
            "coverage_through": timestamp,
            "reason": "Mistral console showed EUR 0.96",
            "activate_snapshot_accounting": True,
            "snapshot_max_age_seconds": 21_600,
        }
        unconfirmed = await client.post(
            "/api/v1/admin/provider-usage-snapshots", headers=admin, json=body
        )
        assert unconfirmed.status_code == 400
        created = await client.post(
            "/api/v1/admin/provider-usage-snapshots",
            headers=admin,
            json={**body, "confirm": True},
        )
        assert created.status_code == 200
        assert created.json()["source"] == "manual-console"
        assert created.json()["replacement_policy_id"]
        summary = (
            await client.get("/api/v1/admin/quotas", headers=admin)
        ).json()
        assert summary["accounts"][0]["usage_basis"] == "provider_snapshot"
        assert summary["provider_usage_snapshots"][0]["actor"] == "token:bootstrap"
        active = next(item for item in summary["policies"] if item["active"])
        assert active["limit_estimated_micro_usd"] is None
        assert active["usage"]["billed_exposure_micro_usd"] == 960_000
        invalid = await client.put(
            "/api/v1/admin/provider-accounts/test%3Aprimary",
            headers=admin,
            json={
                "provider": "test-provider",
                "currency": "EUR",
                "exclusive_consumer": "yes",
            },
        )
        assert invalid.status_code == 400
        exclusive = await client.put(
            "/api/v1/admin/provider-accounts/test%3Aprimary",
            headers=admin,
            json={
                "provider": "test-provider",
                "currency": "EUR",
                "exclusive_consumer": True,
            },
        )
        assert exclusive.status_code == 200
        assert exclusive.json()["exclusive_consumer"] is True
        summary = (
            await client.get("/api/v1/admin/quotas", headers=admin)
        ).json()
        assert summary["accounts"][0]["exclusive_consumer"] is True
        event = app.state.database.audit_events(1)[0]
        assert event["action"] == "quota.account.configure"
        assert event["detail"]["exclusive_consumer"] is True
