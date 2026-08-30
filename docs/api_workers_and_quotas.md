# Hosted API Workers and Quota Accounting

Status: implemented; Citadel production canaries pending

Date: 2026-08-30

## Decision

Hosted conversion adapters run as ordinary fenced BlobForge workers, not inside
the coordinator process. They may be deployed on the same VPS as the
coordinator, but each provider credential is a separate security and failure
boundary. The initial Citadel deployment should therefore use one rootless
Podman Quadlet per provider account, with:

- a provider-specific worker credential and API key;
- the lightweight hosted-worker image rather than a local ML image;
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

- `provider_accounts`: non-secret logical account, provider, enabled state,
  concurrency ceiling, and shared cooldown.
- `quota_policies`: account, explicit effective window, and hard
  request/page/estimated-list-price/billed-cash limits. Multiple simultaneous
  policies may enforce, for example, a daily request rate, monthly promotional
  allowance, and all-time evaluation campaign ceiling. Policy revisions are
  immutable after use.
- `quota_reservations`: job, exact recipe, worker, fenced lease/attempt,
  checkpoint key, reserved units, state (`reserved`, `committed`, `released`,
  or `ambiguous`), measured usage, billing fields, and timestamps.
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
usage, and reconcile ambiguous attempts. The management console exposes these
operations under **Quotas**.

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
- explicit maximum additional requests, pages, and/or micro-USD; and
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
2. Create `mistral:primary` with provider `mistral-ai` and `datalab:primary`
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

Initial worker-side safety ceilings should match the canary rather than the
full corpus: 20 pages and USD 0.05 for Mistral, and 20 pages and USD 0.10 for
Datalab. Coordinator policy windows are an additional aggregate limit, not a
replacement for these immutable per-job adapter caps.

Example Quadlets and secret templates are in `deploy/quadlet/`. The coordinator
SQLite directory and provider response-cache volumes form one recovery
boundary. A database restore without checkpoints can preserve charges but lose
replay data; checkpoints without the database can preserve provider output but
lose settlement. Back up and restore-test both before unattended paid work.
