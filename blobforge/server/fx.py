"""Best-effort reference FX for estimates, never a purchase-availability gate."""

import json
import re
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from fractions import Fraction

URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
DAY = 86_400_000
# ECB daily reference feed retrieved 2026-09-06, dated 2026-09-04.
# Units per EUR. This is a bootstrap estimate, not a promised transaction rate.
BUNDLED_DATE = "2026-09-04"
BUNDLED = dict(EUR="1", USD="1.1622", JPY="181.59", CZK="24.189", DKK="7.4747",
              GBP="0.85898", HUF="363.28", PLN="4.3148", RON="5.2530", SEK="11.1005",
              CHF="0.9405", ISK="140.80", NOK="10.8035", TRY="56.2995", AUD="1.6134",
              BRL="5.9405", CAD="1.6038", CNY="7.7994", HKD="9.1120", IDR="20496.04",
              ILS="3.4954", INR="109.8165", KRW="1569.38", MXN="19.6401", MYR="4.7005",
              NZD="1.9755", PHP="72.812", SGD="1.4724", THB="38.260", ZAR="18.5571")


def parse_feed(body: bytes, timestamp: int):
    if len(body) > 65536 or b"<!DOCTYPE" in body.upper() or b"<!ENTITY" in body.upper():
        raise ValueError("invalid FX feed size or declarations")
    root = ET.fromstring(body)
    dates = [node for node in root.iter() if "time" in node.attrib]
    if len(dates) != 1:
        raise ValueError("FX feed must contain one observation date")
    date = dates[0].attrib["time"]
    observed = int(datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    if observed > timestamp or observed < timestamp - 31 * DAY:
        raise ValueError("FX feed observation outside accepted date window")
    rates = {"EUR": "1"}
    for node in dates[0]:
        currency, value = node.attrib.get("currency", ""), node.attrib.get("rate", "")
        if not re.fullmatch(r"[A-Z]{3}", currency) or currency in rates:
            raise ValueError("invalid or duplicate FX currency")
        if not re.fullmatch(r"[0-9]{1,8}(?:\.[0-9]{1,8})?", value) or not 0 < Fraction(value) < 100000000:
            raise ValueError("invalid FX rate")
        rates[currency] = value
    if "USD" not in rates:
        raise ValueError("FX feed must contain USD")
    return observed, rates


def fetch_feed():
    request = urllib.request.Request(URL, headers={"Accept": "application/xml", "User-Agent": "BlobForge-FX/1"})
    with urllib.request.urlopen(request, timeout=5) as response:
        return response.read(65537)


def refresh(database, fetcher=fetch_feed):
    from .database import now_ms
    timestamp = now_ms()
    try:
        observed, rates = parse_feed(fetcher(), timestamp)
        with database.transaction() as db:
            previous = db.execute("SELECT observed_at FROM provider_fx_cache WHERE id=1").fetchone()
            if previous and previous[0] is not None and observed < previous[0]:
                raise ValueError("FX feed would replace newer evidence")
            db.execute("""INSERT INTO provider_fx_cache(id,observed_at,rates_json,last_attempt,last_success,error)
                VALUES(1,?,?,?,?,NULL) ON CONFLICT(id) DO UPDATE SET
                observed_at=excluded.observed_at,rates_json=excluded.rates_json,
                last_attempt=excluded.last_attempt,last_success=excluded.last_success,error=NULL""",
                       (observed, json.dumps(rates), timestamp, timestamp))
            if observed >= timestamp - 7 * DAY:
                for warning in db.execute("""SELECT w.account_key,w.source_currency,a.currency
                    FROM provider_fx_warnings w JOIN provider_accounts a USING(account_key)""").fetchall():
                    if warning["source_currency"] in rates and warning["currency"] in rates:
                        db.execute("DELETE FROM provider_fx_warnings WHERE account_key=? AND source_currency=?",
                                   (warning["account_key"], warning["source_currency"]))
        return True
    except Exception as exc:
        # No exception strings/URLs in logs or UI: they may contain proxy secrets.
        with database.transaction() as db:
            db.execute("""INSERT INTO provider_fx_cache(id,last_attempt,error) VALUES(1,?,?)
                ON CONFLICT(id) DO UPDATE SET last_attempt=excluded.last_attempt,error=excluded.error""",
                       (timestamp, f"FX refresh failed ({type(exc).__name__}); fallback remains available"))
        return False


def automatic_quote(db, account_key, source_currency, account_currency, timestamp):
    """Return an auditable estimate with a 10% margin; no network under the lock."""
    cache = db.execute("SELECT * FROM provider_fx_cache WHERE id=1").fetchone()
    rates = json.loads(cache["rates_json"] or "{}") if cache else {}
    previous = db.execute("""SELECT * FROM provider_fx_rates WHERE account_key=?
        AND source_currency=? AND account_currency=? AND actor!='system:fx'
        AND observed_at<=? ORDER BY observed_at DESC,created_at DESC LIMIT 1""",
                          (account_key, source_currency, account_currency, timestamp)).fetchone()
    warning = None
    if source_currency in rates and account_currency in rates and (
        not previous or cache["observed_at"] >= timestamp - 7 * DAY or cache["observed_at"] >= previous["observed_at"]
    ):
        ratio = Fraction(rates[account_currency]) / Fraction(rates[source_currency])
        source = "ecb-reference"
        evidence = f"ECB reference date {datetime.fromtimestamp(cache['observed_at'] / 1000, timezone.utc).date().isoformat()}"
        if cache["observed_at"] < timestamp - 7 * DAY:
            warning = "Using stale last-known ECB reference rates"
    elif previous:
        ratio = Fraction(previous["rate_numerator"], previous["rate_denominator"])
        source, evidence = "last-known-operator", previous["id"]
        warning = "Using expired last-known operator rate"
    elif (previous_auto := db.execute("""SELECT * FROM provider_fx_rates WHERE account_key=?
        AND source_currency=? AND account_currency=? AND actor='system:fx'
        AND source IN ('ecb-reference','last-known-operator')
        ORDER BY created_at DESC,observed_at DESC LIMIT 1""", (account_key, source_currency, account_currency)).fetchone()):
        # A currency can disappear from a newer feed. Retain its last quote,
        # including its existing margin, rather than compounding that margin.
        db.execute("""INSERT INTO provider_fx_warnings(account_key,source_currency,message,updated_at)
            VALUES(?,?,?,?) ON CONFLICT(account_key,source_currency)
            DO UPDATE SET message=excluded.message,updated_at=excluded.updated_at""",
                   (account_key, source_currency, "Using last-known automatic estimate; current currency reference unavailable", timestamp))
        return previous_auto
    elif source_currency in BUNDLED and account_currency in BUNDLED:
        ratio = Fraction(BUNDLED[account_currency]) / Fraction(BUNDLED[source_currency])
        source, evidence = "bundled-ecb", BUNDLED_DATE
        warning = "Using bundled bootstrap reference rates"
    else:
        ratio, source, evidence = Fraction(1), "emergency-parity", "no reference for this currency pair"
        warning = "No known FX rate: using approximate 1:1 parity; configure an operator rate"
    ratio *= Fraction(11, 10)
    if warning:
        db.execute("""INSERT INTO provider_fx_warnings(account_key,source_currency,message,updated_at)
            VALUES(?,?,?,?) ON CONFLICT(account_key,source_currency)
            DO UPDATE SET message=excluded.message,updated_at=excluded.updated_at""",
                   (account_key, source_currency, warning, timestamp))
    else:
        db.execute("DELETE FROM provider_fx_warnings WHERE account_key=? AND source_currency=?",
                   (account_key, source_currency))
    reason = f"{evidence}; 10% estimate safety margin; not a billing conversion"
    row = db.execute("""SELECT * FROM provider_fx_rates WHERE account_key=? AND source_currency=?
        AND account_currency=? AND source=? AND reason=? AND actor='system:fx'
        AND rate_numerator=? AND rate_denominator=?
        ORDER BY created_at DESC LIMIT 1""", (account_key, source_currency, account_currency, source, reason,
                                            ratio.numerator, ratio.denominator)).fetchone()
    if row:
        return row
    import secrets
    identifier = "qfx_" + secrets.token_hex(10)
    observed = timestamp
    while db.execute("SELECT 1 FROM provider_fx_rates WHERE account_key=? AND source_currency=? AND account_currency=? AND observed_at=?",
                     (account_key, source_currency, account_currency, observed)).fetchone():
        observed += 1
    db.execute("""INSERT INTO provider_fx_rates(id,account_key,source_currency,account_currency,
        rate_numerator,rate_denominator,observed_at,valid_until,source,reason,actor,created_at)
        VALUES(?,?,?,?,?,?,?,0,?,?,'system:fx',?)""",
               (identifier, account_key, source_currency, account_currency, ratio.numerator,
                ratio.denominator, observed, source, reason, timestamp))
    return db.execute("SELECT * FROM provider_fx_rates WHERE id=?", (identifier,)).fetchone()
