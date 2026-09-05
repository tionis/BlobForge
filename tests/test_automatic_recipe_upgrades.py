"""Automatic release following must never purchase replacement extraction."""
import copy

import pytest

from blobforge.recipe_lifecycle import recipe_digest
from blobforge.recipe_runtime import mistral_wiki_v3_recipe, mistral_wiki_v5_recipe
from blobforge.server.database import Database


def setup(tmp_path):
    database = Database(tmp_path / 'test.sqlite3', lease_seconds=900, max_retries=3)
    database.bootstrap_workers({'worker': 'secret'})
    options = dict(max_pages=100, max_cost_usd=1, response_cache=tmp_path / 'cache',
                   api_rights_confirmed=True, provider_account='mistral:test')
    old = mistral_wiki_v3_recipe(**options).capability()
    new = mistral_wiki_v5_recipe(**options).capability()
    database.register_capabilities('worker', [old, new])
    database.enqueue('a' * 64, {'media_type': 'application/pdf', 'recipe_digest': old['recipe_digest']})
    database.request_conversion('a' * 64, old['recipe_digest'])
    return database, old, new


def reconcile(database, *caps):
    with database.transaction() as db:
        database._upgrade_assigned_recipes(db, list(caps), 123)


def parent(database, recipe):
    with database.transaction() as db:
        cursor = db.execute("""INSERT INTO artifacts(source_key,recipe_digest,identity,
            storage_path,media_type,artifact_type,size_bytes,sha256,blake3,created_at)
            VALUES(?,?,'identity','missing.mdaf','application/zip','mdaf/v1',1,'sha','b3',1)""",
            ('a' * 64, recipe['recipe_digest']))
        db.execute("UPDATE jobs SET status='done',completed_at=1,done_seq=1")
        return cursor.lastrowid


def test_pending_source_follows_release_preserving_backoff_and_retry(tmp_path):
    database, old, new = setup(tmp_path)
    with database.transaction() as db:
        db.execute("UPDATE jobs SET retry_count=2,not_before=9999999999999,blocked_reason='quota'")
    # Real claim hook reconciles even a quota-delayed job, but cannot lease it.
    assert database.claim('worker', ['3_normal'], [new]) is None
    job = database.get_job('a' * 64)
    assert job['recipe_digest'] == new['recipe_digest']
    assert job['input_kind'] == 'source'
    assert job['retry_count'] == 2
    assert job['not_before'] == 9999999999999
    assert job['blocked_reason'] == 'quota'
    reconcile(database, new)
    assert len(database.audit_events(10)) == 1


def test_completed_artifact_replays_offline_and_preserves_parent(tmp_path):
    database, old, new = setup(tmp_path)
    parent_id = parent(database, old)
    reconcile(database, new)
    job = database.get_job('a' * 64)
    assert job['status'] == 'todo'
    assert job['input_kind'] == 'artifact'
    assert job['input_artifact_id'] == parent_id
    assert job['parent_recipe_digest'] == old['recipe_digest']
    assert database.artifact('a' * 64, old['recipe_digest']) is not None
    # Missing bytes are for worker validation, never a reason to schedule source OCR.
    reconcile(database, new)
    assert len(database.audit_events(10)) == 1


def test_registered_worker_claims_upgrade_as_artifact_not_source(tmp_path):
    database, old, new = setup(tmp_path)
    parent_id = parent(database, old)
    job = database.claim('worker', ['3_normal'], [new])
    assert job['input_kind'] == 'artifact'
    assert job['input_artifact_id'] == parent_id
    assert job['recipe_digest'] == new['recipe_digest']
    with database.connect() as db:
        assert db.execute('SELECT count(*) FROM quota_reservations').fetchone()[0] == 0


@pytest.mark.parametrize('state', ['reserved', 'ambiguous', 'committed', 'released'])
def test_pending_source_purchase_boundary_is_preserved(tmp_path, state):
    database, old, new = setup(tmp_path)
    with database.transaction() as db:
        db.execute("""INSERT INTO provider_accounts(account_key,provider,created_at,updated_at)
            VALUES('mistral:test','mistral',1,1)""")
        db.execute("""INSERT INTO quota_reservations(id,source_key,recipe_digest,account_key,
            worker_id,lease_token_hash,checkpoint_key,state,reserved_requests,reserved_pages,
            reserved_estimated_micro_usd,created_at,reconcile_by)
            VALUES('reservation',?,?,'mistral:test','worker','lease','checkpoint',?,1,1,1,1,1)""",
            ('a' * 64, old['recipe_digest'], state))
    reconcile(database, new)
    expected = new if state == 'released' else old
    assert database.get_job('a' * 64)['recipe_digest'] == expected['recipe_digest']
    with database.connect() as db:
        assert db.execute('SELECT state FROM quota_reservations').fetchone()[0] == state


@pytest.mark.parametrize('status', ['processing', 'failed', 'dead'])
def test_active_or_failed_jobs_are_not_retargeted(tmp_path, status):
    database, old, new = setup(tmp_path)
    with database.transaction() as db:
        db.execute('UPDATE jobs SET status=?', (status,))
    reconcile(database, new)
    assert database.get_job('a' * 64)['recipe_digest'] == old['recipe_digest']


@pytest.mark.parametrize('change', ['major', 'extraction', 'family', 'retired', 'account', 'unassigned', 'missing_artifact'])
def test_incompatible_or_unauthorized_work_is_not_upgraded(tmp_path, change):
    database, old, new = setup(tmp_path)
    new = copy.deepcopy(new)
    lifecycle = new['recipe']['lifecycle']
    with database.transaction() as db:
        if change == 'major':
            lifecycle['recipe_version'] = '2.0.0'
            lifecycle['extraction']['major'] = 2
            lifecycle['extraction']['version'] = '2.0.0'
        elif change == 'extraction':
            lifecycle['extraction']['recipe_digest'] = 'blake3:' + '0' * 64
        elif change == 'family':
            lifecycle['family'] = 'unrelated'
        elif change == 'retired':
            db.execute('UPDATE recipes SET enabled=0 WHERE recipe_digest=?', (new['recipe_digest'],))
        elif change == 'account':
            new['provider_account'] = 'mistral:other'
        elif change == 'unassigned':
            db.execute('UPDATE jobs SET recipe_digest=NULL')
        elif change == 'missing_artifact':
            db.execute("UPDATE jobs SET status='done'")
    reconcile(database, new)
    expected = None if change == 'unassigned' else old['recipe_digest']
    assert database.get_job('a' * 64)['recipe_digest'] == expected


def test_source_only_worker_cannot_reextract_completed_artifact(tmp_path):
    database, old, new = setup(tmp_path)
    parent(database, old)
    new['input_kinds'] = ['source']
    reconcile(database, new)
    assert database.get_job('a' * 64)['recipe_digest'] == old['recipe_digest']


def test_existing_target_is_selected_without_reprocessing(tmp_path):
    database, old, new = setup(tmp_path)
    parent(database, old)
    parent(database, new)
    reconcile(database, new)
    job = database.get_job('a' * 64)
    assert job['status'] == 'done'
    assert job['recipe_digest'] == new['recipe_digest']


def test_existing_target_without_old_parent_is_never_reextracted(tmp_path):
    database, old, new = setup(tmp_path)
    parent(database, new)
    with database.transaction() as db:
        db.execute("UPDATE jobs SET status='todo',done_seq=NULL,completed_at=NULL")
    assert database.claim('worker', ['3_normal'], [new]) is None
    job = database.get_job('a' * 64)
    assert job['status'] == 'done'
    assert job['recipe_digest'] == new['recipe_digest']
    assert job['done_seq'] is not None


def test_highest_version_wins_and_equal_versions_fail_closed(tmp_path):
    database, old, new = setup(tmp_path)
    intermediate = copy.deepcopy(new)
    intermediate['recipe']['lifecycle']['recipe_version'] = '1.3.1'
    intermediate['recipe_digest'] = recipe_digest(intermediate['recipe'])
    database.register_capabilities('worker', [intermediate, new])
    reconcile(database, intermediate, new)
    assert database.get_job('a' * 64)['recipe_digest'] == new['recipe_digest']
    with database.transaction() as db:
        db.execute('UPDATE jobs SET recipe_digest=?', (old['recipe_digest'],))
    other = copy.deepcopy(new)
    other['recipe']['implementation_note'] = 'competing build'
    other['recipe_digest'] = recipe_digest(other['recipe'])
    database.register_capabilities('worker', [other, new])
    reconcile(database, other, new)
    assert database.get_job('a' * 64)['recipe_digest'] == old['recipe_digest']
