# Non-blocking FX estimates

Provider list estimates and billing allowances can have different currencies.
FX here is approximate quota reservation evidence, not a currency transaction or
a replacement for provider billing. Original list amounts/currencies and the
converted reservation amount/rate identity remain immutable after reservation.

The coordinator fetches the [ECB daily reference XML](https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml)
at startup and hourly in a background task. ECB publishes reference rates on
working days for informational use; see its
[reference-rate guidance](https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/index.en.html).
Quotes are units per EUR; cross-rates use destination/source, not the inverse.
Requests use TLS verification, a five-second socket timeout and a 64 KiB response
limit. Invalid, future, excessively old or regressing observations cannot replace
the durable last-known snapshot. Fetching never runs inside a job's transaction
or delays API startup. Shutdown stops the hourly loop; a pending bounded fetch
may finish first. `BLOBFORGE_SERVER_FX_REFRESH=false` disables external retrieval,
not fallback estimation. No credentials are sent to ECB.

Selection order:

1. An unexpired explicit operator rate, unchanged and without an added margin.
2. A cached ECB cross-rate (fresh within seven days, allowing weekends/holidays),
   or a newer last-known operator observation if both are stale.
3. The latest available expired operator or automatic quote for that pair.
4. The bundled ECB snapshot dated 2026-09-04, covering the current reference
   currencies and making cold starts useful without network access.
5. For a completely unknown pair only, approximate 1:1 parity with an explicit
   administrator warning to configure a manual rate. This is an emergency
   continuity policy, not a claim that those currencies have equal value.

Automatic and fallback estimates include a 10% safety margin, with integer
rounding upward. Reusing a quote never compounds its margin. These estimates
cannot guarantee an exact billing amount, especially for stale/unknown pairs.
All existing quota ceilings, concurrency, purchase-reconciliation, provider
cooldown, rights and user-approved overage checks still apply. An exhausted
quota may deny a request; inability to retrieve FX alone cannot.

The Quotas UI/API exposes last attempt/success and sanitized refresh failures,
plus per-account/pair fallback warnings. A successful refresh clears the fetch
error; a fresh/manual quote clears that pair's warning. Warnings record the last
use, not a failed job. A fresh feed or operator override also clears warnings
for the currency pairs it covers. Failed refreshes retain snapshots across restarts.
Computed rate records have actor `system:fx` and `valid_until=0`: these immutable
reservation records are not manual overrides. Their observation time is quote
creation time; the reason records the underlying ECB date or parent rate ID.

Only additive cache/warning tables are introduced. The MDAF SPEC, recipe identity,
artifact bytes and paid extraction parameters are unchanged. Startup clears only
legacy `no current … FX rate` queue delays and records an audit event.
