from fractions import Fraction

import pytest

from blobforge.server.database import Database, now_ms
from blobforge.server import fx


@pytest.fixture
def db(tmp_path):
    database = Database(tmp_path / 'db.sqlite', lease_seconds=60, max_retries=3)
    database.configure_provider_account('test:fx', 'test', currency='EUR')
    return database


def quote(db, currency='USD', timestamp=None):
    with db.transaction() as connection:
        account = connection.execute("SELECT * FROM provider_accounts WHERE account_key='test:fx'").fetchone()
        amount, identity = db._converted_estimate(connection, account, currency, 1000000, timestamp or now_ms())
        row = connection.execute('SELECT * FROM provider_fx_rates WHERE id=?', (identity,)).fetchone()
    return amount, dict(row) if row else None


def feed(date='2026-09-04', usd='1.25'):
    return f'<Envelope><Cube><Cube time="{date}"><Cube currency="USD" rate="{usd}"/><Cube currency="JPY" rate="180"/></Cube></Cube></Envelope>'.encode()


def test_bootstrap_unknown_and_same_currency_never_require_network(db):
    amount, rate = quote(db)
    assert amount == (1000000 * 5500 + 5811 - 1) // 5811
    assert rate['source'] == 'bundled-ecb'
    assert quote(db)[1]['id'] == rate['id']
    assert db.quota_summary()['fx_status']['warnings']
    assert quote(db, 'XYZ')[0] == 1100000
    assert any('1:1' in w['message'] for w in db.quota_summary()['fx_status']['warnings'])
    assert quote(db, 'EUR') == (1000000, None)


def test_refresh_inversion_margin_persistence_and_failure(db, monkeypatch):
    timestamp = 1788652800000  # 2026-09-06 UTC
    monkeypatch.setattr('blobforge.server.database.now_ms', lambda: timestamp)
    assert fx.refresh(db, lambda: feed())
    amount, rate = quote(db, timestamp=timestamp)
    assert amount == 880000  # 1 / 1.25 * 1.1 EUR per USD
    assert rate['source'] == 'ecb-reference'
    assert Fraction(rate['rate_numerator'], rate['rate_denominator']) == Fraction(22, 25)
    assert quote(db, 'JPY', timestamp)[0] == 6112  # Round upward, not nearest.
    reopened = Database(db.path, lease_seconds=60, max_retries=3)
    assert quote(reopened, timestamp=timestamp)[0] == amount
    def fail():
        raise TimeoutError('private proxy token must not escape')
    assert not fx.refresh(db, fail)
    status = db.quota_summary()['fx_status']
    assert 'TimeoutError' in status['error'] and 'private' not in status['error']
    assert quote(db, timestamp=timestamp)[0] == amount
    quote(db, timestamp=timestamp + 40 * fx.DAY)
    assert 'stale' in db.quota_summary()['fx_status']['warnings'][0]['message']
    assert fx.refresh(db, lambda: feed())
    quote(db, timestamp=timestamp)
    assert not db.quota_summary()['fx_status']['warnings']
    assert db.quota_summary()['fx_status']['error'] is None


def test_operator_override_expiry_and_cache_replacement(db, monkeypatch):
    timestamp = 1788652800000
    monkeypatch.setattr('blobforge.server.database.now_ms', lambda: timestamp)
    manual = db.record_provider_fx_rate('test:fx', source_currency='USD', rate_numerator=9,
        rate_denominator=10, observed_at=timestamp, valid_until=timestamp+1000,
        source='operator', reason='test explicit override', actor='admin')
    assert quote(db, timestamp=timestamp)[1]['id'] == manual['id']
    amount, stale = quote(db, timestamp=timestamp+2000)
    assert amount == 990000 and stale['source'] == 'last-known-operator'
    assert fx.refresh(db, lambda: feed())
    assert quote(db, timestamp=timestamp+2000)[1]['source'] == 'ecb-reference'
    assert not fx.refresh(db, lambda: feed(date='2026-09-03'))
    assert 'ValueError' in db.quota_summary()['fx_status']['error']


def test_removed_feed_currency_keeps_last_quote_without_compounding_margin(db, monkeypatch):
    timestamp = 1788652800000
    monkeypatch.setattr('blobforge.server.database.now_ms', lambda: timestamp)
    assert fx.refresh(db, lambda: feed())
    original = quote(db, 'JPY', timestamp)
    assert fx.refresh(db, lambda: feed().replace(b'<Cube currency="JPY" rate="180"/>', b''))
    for _ in range(3):
        assert quote(db, 'JPY', timestamp) == original
    assert db.quota_summary()['fx_status']['warnings']


@pytest.mark.parametrize('body', [b'broken', b'x'*65537, b'<!DOCTYPE a><a/>',
    feed(usd='0'), feed(usd='NaN'), feed(usd='-1'), feed(usd='1e999'),
    feed(date='2099-01-01'), feed(date='2001-01-01')])
def test_bad_feed_retains_fallback(db, body):
    assert not fx.refresh(db, lambda: body)
    assert quote(db)[0] > 0


@pytest.mark.anyio
async def test_lifespan_refresh_is_nonblocking_and_can_be_disabled(tmp_path, monkeypatch):
    import asyncio
    from blobforge.server.app import create_app
    from blobforge.server.config import ServerSettings
    calls = []
    monkeypatch.setattr(fx, 'refresh', lambda db: calls.append(db.path))
    for enabled in (False, True):
        app = create_app(ServerSettings(data_dir=tmp_path, client_token='test', worker_tokens={}, fx_refresh_enabled=enabled))
        async with app.router.lifespan_context(app):
            for _ in range(20):
                await asyncio.sleep(0.01)
                if calls:
                    break
        assert bool(calls) is enabled


@pytest.fixture
def anyio_backend():
    return 'asyncio'


def test_fx_configuration_defaults_and_override(monkeypatch):
    from blobforge.server.config import ServerSettings
    monkeypatch.setenv('BLOBFORGE_SERVER_CLIENT_TOKEN', 'test')
    monkeypatch.delenv('BLOBFORGE_SERVER_FX_REFRESH', raising=False)
    assert ServerSettings.from_env().fx_refresh_enabled
    monkeypatch.setenv('BLOBFORGE_SERVER_FX_REFRESH', 'false')
    assert not ServerSettings.from_env().fx_refresh_enabled


def test_startup_releases_only_legacy_fx_delays(tmp_path):
    from blobforge.server.app import create_app
    from blobforge.server.config import ServerSettings
    settings = ServerSettings(data_dir=tmp_path, client_token='test', worker_tokens={}, fx_refresh_enabled=False)
    app = create_app(settings)
    database = app.state.database
    for key in ('fx', 'quota'):
        database.enqueue(key, {})
    with database.transaction() as connection:
        connection.execute("UPDATE jobs SET not_before=9999999999999,blocked_reason='no current USD/EUR FX rate for provider estimate' WHERE source_key='fx'")
        connection.execute("UPDATE jobs SET not_before=9999999999999,blocked_reason='quota exhausted' WHERE source_key='quota'")
    create_app(settings)
    assert database.get_job('fx')['not_before'] is None
    assert database.get_job('quota')['not_before'] == 9999999999999
