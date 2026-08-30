import hashlib

import httpx
import pytest

from blobforge.converters.contract import (
    PROVIDER_ATTEMPT_CONTRACT,
    PROVIDER_PROBE_CONTRACT,
)
from blobforge.server.app import create_app
from blobforge.server.config import ServerSettings
from blobforge.server.database import Conflict, Database, now_ms


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
    assert records["overrides"][0]["consumed_by"] == overage["reservation"]["id"]


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
        window_end=timestamp + 86_400_000,
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
