# Hosted API Workers and Quota Accounting

Status: implemented and deployed; bounded Citadel success-path canaries and
failure-injection coverage complete

Date: 2026-08-30

## Decision

Hosted conversion adapters run as ordinary fenced BlobForge workers, not inside
the coordinator process. They may be deployed on the same VPS as the
coordinator, but each provider credential is a separate security and failure
boundary. The initial Citadel deployment should therefore use one system
Podman Quadlet per provider account, with:

- a provider-specific worker credential and API key;
- the lightweight hosted-worker image rather than a local ML image;
- an unprivileged container UID with a provider-specific writable cache;
- uv's disposable runtime cache explicitly rooted on the writable `/tmp`
  tmpfs, never under the read-only application tree;
- a persistent, backed-up response-checkpoint volume;
- access to the coordinator over the host-local listener; and
- conservative CPU, memory, process, and restart limits.

The coordinator remains the sole scheduler and quota authority. A hosted
worker can advertise multiple recipes for one provider account and can process
local artifact-only upgrades between paid source conversions. Do not put
unrelated provider keys in one container merely to obtain multipurpose
dispatch: separate workers can still share the coordinator queue.

Hosted capabilities are **explicit-assignment only**. Their
`claim_unassigned=false` registration means they can lease a job only after an
administrator or routing policy has selected that exact immutable recipe.
They cannot consume the historical unassigned queue. The coordinator persists
this property with the worker registration and treats the stored value as
authoritative during every claim; a later claim request cannot broaden it.
Local compatibility workers retain `claim_unassigned=true` unless their recipe
opts out. Older clients that never register any capability remain supported,
but production hosted workers must always register the versioned capability.

## Three different limits

BlobForge must not collapse every restriction into one `quota` boolean:

1. **Applicability and safety limits** include processing rights, privacy,
   provider file/page limits, recipe compatibility, and immutable per-request
   constraints. A quota override never bypasses these.
2. **BlobForge budgets** are locally enforced request, page, estimated
   list-price, and eventually billed-cash limits for a logical provider
   account and time window. These can receive a bounded administrative
   allowance.
3. **Provider-side limits** include account credits, rate limits, concurrency,
   and a provider's own monthly cap. BlobForge observes these but cannot
   override them. A `429`, exhausted account, or provider rejection defers work
   even if a local overage was authorized.

List price, billed amount, and credits/discounts remain separate ledger
fields. Promotional credit must not make the underlying work appear free, and
an API that reports only some of these values must leave the others unknown
rather than infer them.

Money is stored as integer micro-USD, never SQLite floating point. Page and
request counts are integers. Provider/account identifiers are logical names
such as `mistral:primary`; credentials never enter SQLite, capability JSON,
MDAFs, logs, or audit details.

## Two-phase paid-work protocol

A source job is not charged merely because it was leased. The worker first
downloads and locally preflights it:

1. The normal capability-aware claim creates a fenced job lease.
   Worker-only source, parent-artifact, and output capability URLs use the
   coordinator URL on which the worker made its request, rather than routing
   through public ingress. A transient signed-input network failure releases
   the lease and waits before reclaiming; it occurs before provider preflight
   and must not consume a conversion retry.
2. The adapter probe computes the provider request/checkpoint identity, page
   count, estimated list price, and whether a durable successful response is
   already cached. It performs no network request.
3. A cache hit records a zero-purchase execution and proceeds without consuming
   the paid budget. A cache miss asks the coordinator to atomically reserve
   request, page, and estimated-cost allowance for this lease.
4. The coordinator counts committed usage plus all live reservations in the
   active policy window. It returns an opaque reservation identifier or denies
   the purchase with the exact exhausted dimensions and next eligible time.
5. The adapter receives that reservation identifier, performs at most the
   authorized request, and durably checkpoints a successful provider response
   before validation, normalization, or MDAF packaging.
6. Immediately after the purchase boundary, the adapter atomically writes a
   structured attempt report. The worker settles the reservation from that
   report even when later packaging fails. Completion of the MDAF is not the
   billing boundary.
7. A request that provably never crossed the purchase boundary releases its
   reservation. Ambiguous outcomes remain reserved for reconciliation rather
   than being silently retried.

The probe and attempt report are a new provider-adapter ABI, not strings parsed
from diagnostics. They contain the provider account, checkpoint key/state,
requested and processed pages, estimated list price, reported list price,
billed amount, credits, reservation identifier, and failure/cooldown detail as
available. The coordinator binds these reports to the exact recipe through the
fenced lease and worker capability; the provider-request digest is part of the
checkpoint identity and frozen recipe parameters.

The reservation identifier must also be retained in the response checkpoint.
If the worker dies after checkpoint publication but before settlement, the
next probe can reconcile the same reservation and must not purchase the work
again. A successful checkpoint is still the primary duplicate-purchase guard;
the quota ledger is not a response cache.

## Coordinator persistence

The coordinator uses these normalized records:

- `provider_accounts`: non-secret logical account, provider, immutable ISO 4217
  billing currency after first use, enabled state, concurrency ceiling, and
  shared cooldown.
- `provider_fx_rates`: append-only conversion evidence for a provider's
  list-price currency and one account billing currency. Each record retains an
  integer numerator/denominator, observation and expiry times, evidence source,
  reason, actor, and creation time. Same-currency estimates use the identity
  rate and require no record.
- `quota_schedules`: one optional monthly reset rule per account. The reset is
  local midnight in an explicit IANA timezone, supports days 1 through 28, and
  materializes the containing immutable policy window transactionally on
  configuration, summary, or reservation.
- `quota_policies`: account, explicit effective window, and hard
  request/page/estimated-list-price/billed-cash limits. Multiple simultaneous
  policies may enforce, for example, a daily request rate, monthly promotional
  allowance, and all-time evaluation campaign ceiling. Policy revisions are
  immutable after use. A recurring reset-boundary correction may append
  `superseded_at`, `superseded_by`, and a reason to the old policy without
  rewriting its window, limits, or usage. The coordinator accepts this only
  when the replacement window starts no later than the old one and every limit
  is equally strict or stricter, so all current-cycle usage remains counted and
  the correction cannot manufacture allowance.
- `quota_reservations`: job, exact recipe, worker, fenced lease/attempt,
  checkpoint key, original estimate/currency, conservative account-currency
  estimate, immutable FX-rate reference when applicable, state (`reserved`,
  `committed`, `released`, or `ambiguous`), measured usage, list-price currency,
  billing fields, and timestamps.
- `job_quota_overrides`: one job and exact recipe, bounded extra request/page/
  cost allowance, reason, actor, expiry, creation time, and consuming
  reservation. Overrides are single-use unless explicitly created otherwise.

Reservations and budget checks occur in one `BEGIN IMMEDIATE` transaction so
two workers cannot both spend the same remaining allowance. An expired job
lease does not automatically release an ambiguous reservation: only a
definitively pre-purchase attempt may be released. Reserved rows have a longer
reconciliation deadline than job leases.

A quota denial returns the leased job to `todo` with `not_before`,
`blocked_reason=quota`, and the policy window reset time. Claim queries ignore
jobs whose `not_before` is in the future. This avoids burning retry counts or
creating a hot claim/release loop. Applying an override clears the quota delay
and makes the job immediately eligible. Provider rate limits can set a shared
account cooldown so every worker stops submitting, while unrelated local or
provider work continues.

The worker protocol uses
`POST /api/v1/jobs/{source}/quota-reservation` for a lease-bound probe and
`POST /api/v1/quota-reservations/{id}/settle` for the purchase-boundary report.
Settlement is independent of MDAF completion. Administrative APIs configure
accounts and immutable policy windows, create/revoke overages, summarize
usage, and reconcile ambiguous attempts. It also configures recurring monthly
schedules through `PUT /api/v1/admin/quota-schedules/{account}`. The management
console exposes these operations under **Quotas**.

Cross-currency estimates use an optional `estimate_currency` in the v1 probe.
The existing `currency` continues to identify the provider account's billing
currency. If they differ, authorization selects the newest unexpired matching
FX record, converts with integer ceiling division, and stores both amounts plus
the rate ID. Missing or expired evidence returns the job to `todo` for five
minutes without consuming a retry or creating a reservation. Recording a
matching rate through `POST /api/v1/admin/provider-fx-rates` releases those
delays. The endpoint requires `confirm=true`; rates cannot last more than 31
days. BlobForge never fetches, guesses, or silently refreshes exchange rates.
Operators must include any desired safety margin in the recorded rate.

Changing an enabled schedule's timezone or reset day materializes the
replacement window and transactionally supersedes the old active scheduled
window under those coverage rules. The historical policy remains visible in
the quota summary but no longer participates in authorization after its
supersession timestamp. A boundary move that would omit already-counted usage
or weaken any limit fails with HTTP 409 and rolls back the schedule change.
Quota-delayed jobs for that provider account are released once so their next
claim recomputes the correct replacement-window deferral; this never increments
their conversion retry count or authorizes spend by itself.

Legacy JSON and SQLite field names retain the suffix `micro_usd` for wire and
database compatibility. Policy, billed, credit, snapshot, override, and
`reserved_estimated_micro_usd` values are one-millionth of the provider
account's declared currency. A probe's legacy `estimated_micro_usd` and a
report's `list_micro_usd` are instead explicitly qualified by
`estimate_currency` and `list_currency`; absent qualifiers default to the
account currency for older workers. Cross-currency reservations additionally
store `reserved_estimate_micro_units` in the source price currency. Actual
billed cash and credits must still use the account currency. Older records are
migrated as same-currency observations without changing amounts.

Mistral OCR wiki-v3 and Datalab accurate wiki-v1 implement the structured ABI.
Mistral reserves its fixed per-page estimate. Datalab exposes no trustworthy
pre-request quote, so a new submission reserves the configured per-job ceiling
and settles to returned list, billed, and credit amounts. Checkpoints carry the
reservation identifier; a later lease resumes an unsettled reservation after a
crash rather than purchasing or accounting for the same request twice.
Until an attempt reports actual billed cash, billed-limit enforcement counts
its reserved estimate as conservative billed exposure. The console labels this
distinctly from known billed cash in the historical usage ledger.

## Administrative override

The UI presents this as **Allow one quota overage**, and the API requires:

- the exact queued job and recipe;
- a human reason;
- an expiry;
- explicit maximum additional requests, pages, and/or micro-units in the
  provider account's currency; and
- explicit `confirm=true` after the estimated allowance is known.

There is intentionally no permanent `ignore_quota` job flag. The allowance is
consumed by one reservation, remains in the audit/usage ledger, and is visible
on the job detail page. An administrator can revoke an unused allowance. The
override changes only BlobForge budget arithmetic; rights, privacy, recipe
safety caps, provider cooldowns, and provider-side limits still apply.

The same management area exposes current windows, committed and reserved
usage, known billed cash, credits, next reset, ambiguous reservations, and jobs
waiting for allowance. Changing a policy creates a new effective revision
rather than rewriting the policy that authorized historical purchases.

## Failure handling

| Situation | Reservation | Job behavior |
| --- | --- | --- |
| durable cache hit | zero-purchase ledger event | continue packaging |
| local preflight failure | none | normal job failure |
| local budget exhausted | none | defer until reset or bounded override |
| provider `429` before purchase | release | defer account using `Retry-After` |
| provider success checkpointed | commit | package; retry later from cache if needed |
| signed input transfer network unavailable | none | release lease and retry later without consuming conversion retries |
| timeout with unknown provider outcome | ambiguous | stop automatic repurchase and reconcile |
| packaging/upload failure after provider success | commit | retry from durable cache |
| invalid/expired override | none | remain quota-deferred |

Provider errors must be classified instead of all incrementing the conversion
retry counter. Budget waiting and rate limiting are scheduling conditions;
malformed output and deterministic adapter failures are conversion failures;
ambiguous purchase outcomes require operator-visible reconciliation.

## Initial Citadel rollout

1. Build and publish the hosted-worker image containing both isolated adapter
   environments.
2. Create a currency-specific Mistral account with provider `mistral-ai` and a
   currency-specific Datalab account with provider `datalab`,
   with provider `datalab`, both at concurrency one in the quota console, then
   add explicit policy windows before starting workers.
3. Deploy one Mistral canary Quadlet, disabled until the account and policy
   exist, with a dedicated worker
   token, key environment file, and persistent checkpoint volume. The
   coordinator data and checkpoint volume must both be covered by recovery
   documentation and backup tests.
4. Exercise cache hit, normal reservation, exhausted budget, override,
   provider cooldown, crash-after-checkpoint, and packaging-failure canaries.
5. Deploy Datalab as a separate provider-account worker and verify its
   conservative ceiling reservation settles to returned billing values.
6. Only then raise concurrency or add further hosted providers. Per-account
   concurrency remains independent of worker process count.

Running workers beside the coordinator is operationally reasonable: provider
conversion is network-bound and the response cache benefits from local durable
storage. Separate containers and fenced protocols preserve the option to move
them to another host later without changing job or artifact identity.

The production corpus contains PDFs up to 502 pages. Worker-side safety
ceilings therefore allow at most 1,000 pages per purchase, EUR 5 for Mistral,
and USD 10 for Datalab; coordinator monthly schedules remain the smaller
aggregate authority. These per-job ceilings are safety bounds, not spending
targets, and do not alter immutable recipe identity or provider cache keys.

The initial unattended schedules reset at local midnight on day 28 in
`Europe/Berlin`. Mistral uses a EUR 12.75 estimated and billed-exposure ceiling.
Datalab uses its current USD 20 monthly free-credit allowance as both the
conservative estimate and billed-exposure ceiling. Provider promotions are
external facts: operators must verify and revise a future schedule before the
next immutable window is materialized if the plan changes.

Example Quadlets and secret templates are in `deploy/quadlet/`. The coordinator
SQLite directory and provider response-cache volumes form one recovery
boundary. A database restore without checkpoints can preserve charges but lose
replay data; checkpoints without the database can preserve provider output but
lose settlement. Back up and restore-test both before unattended paid work.

The first Citadel cache-hit canary caught a runtime-boundary error before
provider access: non-root `uv run` defaulted to `/app/.cache/uv`, but `/app` is
read-only. Hosted images must set `UV_CACHE_DIR=/tmp/uv-cache`; deployment may
repeat that environment setting as defense in depth. A release probe must run
an evaluator-project `uv run` as UID 10001 with a read-only root filesystem,
not merely invoke the already-installed BlobForge entry point.

The hosted supervisor is container PID 1 and therefore installs explicit
SIGINT/SIGTERM handlers. Idle waits are interruptible. Adapter probe/conversion
subprocesses run in isolated process groups and are terminated on shutdown so
the supervisor can settle a cache hit, release a pre-purchase lease, or retain
an ambiguous purchase for reconciliation before deregistering. A Podman stop
probe must observe exit code zero and the deregistration path before the image
is promoted.

## Citadel production evidence

The 2026-08-30 rollout used digest-pinned hosted image
`sha256:89f3e7202ba280a3e66938054ce492bf274c6ca7953e41f1bfb74c89288c3256`
with one provider enabled at a time. Both workers registered exact-recipe-only
capabilities, ran as UID 10001 with a read-only root filesystem, and stopped
with exit status zero and no restart. The coordinator retained every legacy
artifact throughout the campaign.

The eight-page *Shadows and Mirrors* source replayed the durable Mistral and
Datalab checkpoints. Both quota entries committed as cache hits with zero
requests, pages, or money exposure. The resulting MDAFs validated independently
inside the production coordinator and published eight page mappings.

The nine-page *Massive Monsters* source then exercised a real cache miss for
each provider. Datalab reserved its USD 0.10 safety ceiling, completed one
request and nine pages, and reported USD 0.07 billed. Mistral reserved and
settled nine pages at USD 0.036 list price; its actual billed cash and credits
remain unknown rather than being inferred. Both artifacts passed fail-closed
MDAF validation with nine page mappings and exact composite-recipe provenance.
The catalog ended with four new hosted artifacts, all four reservations in
`committed`, no retries, SQLite `quick_check=ok`, and the original 1,377 done /
431 todo job split.

After the campaign both provider units were initially returned to
disabled/inactive steady state. A quiesced Restic snapshot including SQLite,
immutable objects, and both response caches completed in 16 seconds. An
isolated full restore completed in 221 seconds and passed its recovery
verification.

On 2026-08-31, revision `f9ff3fe` and its digest-pinned server/hosted images
were deployed with both concurrency-one workers enabled. Integration tests
exercise exhaustion, a single-use bounded override, shared cooldown,
crash-after-checkpoint resume, ambiguous reconciliation, and packaging failure
after provider commit. Production registration then proved each worker idle on
its exact account and recipe with `claim_unassigned=false`; the 1,377 done / 431
todo split did not change and the new monthly windows started at zero usage.
A fresh quiesced snapshot, service resume, isolated restore, SQLite integrity
check, and post-restore worker/queue/quota assertions all passed. The workers
may therefore remain online for explicitly assigned jobs, but must never be
changed to claim unassigned work without a separate routing decision.

### Subscription allowance reconciliation

The OCR attempt response supplies usage units but no authoritative billed cash
or included-subscription consumption. When `billed_micro_usd` is unknown, the
current fail-safe ledger uses the reservation estimate for both estimated and
billed quota dimensions. This prevents accidental overspend, but it can stop
early when an account plan applies a discount or included usage. In the August
2026 production window, 3,184 pages therefore initially counted as EUR 12.736
because a USD 0.004/page list price had been mislabeled as the EUR account
currency. Mistral's console first reported EUR 0.96, then advanced to EUR
10.91 for the same purchases while the worker was stopped. The first display
was incomplete provider reporting, not a discount. The later snapshot left
EUR 1.84 of the EUR 12.75 allowance. List-price currency and account billing
currency are now modeled separately with explicit exchange-rate provenance.
Historical rows remain unchanged; a manual provider snapshot remains the
authoritative allowance baseline for actual consumption.

These are different accounting facts and remain separate. Settled reservations
retain their list-price estimate; an operator must not rewrite them to
distribute an account-level console total across jobs.

An administrator can record a manual observation with
`POST /api/v1/admin/provider-usage-snapshots`. The append-only record contains:

- the exact provider account, account currency, and quota window;
- provider-reported billed/subscription usage;
- when the administrator observed it and the latest purchase the display is
  known to cover;
- the administrator identity and required evidence/reason.

The first confirmed snapshot can atomically activate `provider_snapshot`
accounting. BlobForge supersedes rather than edits the current policy, removes
the list-estimate ceiling from the replacement while retaining its billed
limit and non-money limits, and updates the recurring schedule so future
windows use the same basis. The old policy and every reservation remain in the
audit history.

Authorization then calculates billed exposure as the provider-reported
baseline plus full reservation estimates for every reserved, committed, or
ambiguous purchase created after `coverage_through`. A snapshot cannot cover
an unsettled older attempt. Observed time, coverage, and reported usage must
advance monotonically within a window. The default snapshot freshness is six
hours and can be configured from 15 minutes through seven days. A missing or
stale snapshot prevents new paid reservations but does not prevent cache hits;
recording a new snapshot releases quota-delayed jobs for recomputation.

An account whose allowance is spent only by BlobForge may explicitly enable
`exclusive_consumer` after provider-snapshot accounting has been activated.
This is an administrator assertion about the provider account, not a statement
inferred from a credential or from the absence of known external purchases.
For every recurring quota window BlobForge then appends exactly one
`automatic-exclusive-reset` snapshot with zero reported usage and
`coverage_through` equal to the window start. The observation records when
BlobForge materialized or inspected the window; it does not pretend that the
provider console was queried.

Exclusive reset baselines do not expire. BlobForge conservatively adds the
full account-currency reservation amount for every later reserved, committed,
or ambiguous purchase, so work can continue without six-hour console checks.
Administrators may still append a real manual snapshot to reconcile delayed
provider billing, discounts, or credits; subsequent reservations are counted
after that snapshot's coverage cutoff. Disabling exclusivity immediately
restores the ordinary missing/stale snapshot gate. Never enable this mode when
another key, application, or person can consume the same provider allowance.

The manual flow requires `confirm=true` and is available in the management
quota console. Set `coverage_through` only as late as the provider display is
known to include: an optimistic cutoff could omit a purchase during provider
reporting lag. Treat a fresh dashboard reading as provisional and append a
later monotonic observation when the provider finishes reporting; never infer
a stable per-page rate from an early value. Automated snapshots would still
require a dedicated Mistral Admin API key rather than the inference key.
