# Edge Backend Evaluation

## Status

Platform decision pending. The user-preferred experiment is Bunny-only
coordination, while the production-oriented comparison favors Cloudflare Workers
with a SQLite-backed Durable Object. Worker execution and blob storage remain
separate in either design.

Managed message queues are explicitly rejected for the primary backlog. The
expected queue contains hundreds of jobs, workers complete only a few jobs per
day, and normal backlog age can be several months. A durable database work table
is therefore required; pending jobs must not expire because of broker retention.

## Objective Platform Recommendation

For a production coordination backend, prefer Cloudflare Workers with one
SQLite-backed Durable Object for the global BlobForge coordinator. Do not use
Cloudflare Queues as the authoritative backlog.

A single Durable Object matches this low-throughput workload particularly well:
it serializes all coordination requests, provides transactional and strongly
consistent SQLite storage, retains job rows without message expiration, supports
alarms for maintenance, and provides 30-day point-in-time recovery. SQLite-backed
Durable Objects are generally available. The Free plan's storage and request
limits are far beyond a backlog of hundreds of jobs; the single-object throughput
ceiling is also irrelevant at only a few completions per day.

The Cloudflare shape is:

1. An HTTP Worker authenticates clients and routes coordination calls to one
   named BlobForge Durable Object.
2. The Durable Object owns the manifest, jobs, priority ordering, leases,
   progress, retries, workers, and configuration in its embedded SQLite database.
3. External Python workers call the Worker API to claim, heartbeat, complete, or
   fail jobs. Blob data remains in the existing object store.
4. Inline expired-lease recovery remains the primary mechanism; a Durable Object
   alarm may provide proactive cleanup but is not required for correctness.

This is objectively safer today than Bunny Database because Cloudflare's storage
is strongly consistent and transactional, SQLite-backed Durable Objects are GA,
and point-in-time recovery is included. Bunny Database is still in Public Preview
and explicitly documents a failover window in which recently committed writes
can be lost. Bunny remains a reasonable learning experiment or cost-minimizing
choice, but it is not the stronger production coordination platform on the
currently published guarantees.

## Implementation Complexity

Bunny is marginally simpler for a proof of concept: one Edge Script opens a
libSQL client, creates conventional tables, and implements HTTP routes. It avoids
Cloudflare's Worker router, Durable Object binding, class migration tags, and
object initialization lifecycle.

Cloudflare is simpler for the production-quality implementation. The public
Worker adds a small routing layer, but the single Durable Object serializes all
job-state operations and supplies strongly consistent transactions, persisted
alarms, and point-in-time recovery. The implementation does not need to design
around replica read consistency or build reconciliation for Bunny's documented
failover write-loss window.

Both options still require the same domain behavior: authentication, schema,
job states, priority ordering, worker registration, fencing leases, heartbeats,
retries, progress, errors, API models, and Python client changes. The Durable
Object does not remove leases because workers can still crash after claiming a
job; it makes the lease transitions easier to reason about and test.

Therefore:

- Small demonstration: Bunny requires less scaffolding.
- Complete reliable backend: Cloudflare Worker + Durable Object requires less
  custom distributed-systems code and is the simpler implementation overall.

## Recommendation

Use Bunny Edge Scripting as BlobForge's authenticated control-plane API and
Bunny Database as the authoritative metadata/job-state store. Keep raw PDFs and
output archives in the existing object store, and keep PDF conversion in the
existing long-running Python workers. Bunny does not execute conversions or
host the workers.

An edge request must enqueue or mutate state and return immediately. It must not
remain alive while a PDF is converted. Bunny Edge Scripting currently limits a
request to 30 seconds of CPU and 128 MB of active memory, so it is suitable for
API and coordination logic but not Marker PDF execution.

The recommended flow is:

1. A CLI, bot, or UI calls the edge API.
2. The edge API authenticates the caller and transactionally creates or changes
   a job in Bunny Database.
3. Existing Python workers claim jobs using a single atomic SQL state
   transition, download input from object storage, convert locally, upload the
   result, and record completion or failure.
4. Workers periodically renew a job lease and publish progress. Any worker may
   reclaim an expired lease; no dedicated always-running janitor is required for
   correctness.

## Why Bunny Database First

Bunny Database is SQLite/libSQL-compatible, supports write transactions, and is
directly available to Edge Scripts. BlobForge's expected metadata workload is a
good fit for a single-writer SQLite model: conversions are long, while job-state
writes are short and relatively infrequent. Atomic job claiming should use a
conditional `UPDATE ... RETURNING` inside a write transaction instead of the
PostgreSQL-specific `FOR UPDATE SKIP LOCKED` approach.

The important reservations are that Bunny Database is in Public Preview, its
documented default database-size limit is currently 1 GB, writes are forwarded
to one active primary and do not execute in parallel, and a committed write may
still be lost during primary failover before its WAL is uploaded to durable
storage. Bunny documents that exposure as up to 10 seconds or 4096 WAL frames.
Replica reads also do not guarantee read-your-writes. Its tokens currently offer
only full-access or read-only database-wide permissions. Full-access credentials
should therefore remain behind the edge API. Worker credentials need a separate
authentication design and should be replaceable without changing the job model.

## Background and Scheduling

The background duration belongs to the external Python worker, not the edge
script. Job
leases make janitor recovery opportunistic: claim operations can first requeue
expired jobs, and dashboard/operator requests can do the same as maintenance.
A small scheduled trigger is optional rather than part of correctness.

There is no continuously hosted coordinator. Workers poll the edge API with
adaptive backoff, and heartbeat through the same API while processing. Durable
background behavior comes from persisted job and lease state in Bunny Database,
not from keeping an Edge Script invocation alive.

## Scope Boundaries

The coordination backend contains only Bunny services:

- Bunny Edge Scripting for authentication, authorization, validation, and job
  state endpoints.
- Bunny Database for the manifest, job state, leases, retries, worker registry,
  progress, errors, and operational configuration.

Cloudflare Queues, Cloudflare Workers, hosted PostgreSQL, Bunny Magic Containers,
and direct S3 queue markers are not part of the target coordination design.
External workers and the current object store remain part of BlobForge, but are
clients/data storage rather than coordination services.

## What Cloudflare Would Add

Cloudflare currently provides several first-class coordination primitives that
Bunny's documented Edge Scripting and Database products do not provide directly:

- Queues with at-least-once delivery, HTTP pull consumers, visibility leases,
  configurable retries/delays, and dead-letter queues.
- Cron Triggers and Durable Object alarms for scheduled maintenance without an
  external caller.
- Workflows for durable multi-step execution, persisted waits, automatic retries,
  and built-in execution observability.
- Durable Objects as strongly consistent, serializable per-object coordination
  points.
- More granular platform API tokens for pull consumers; Bunny Database tokens
  are currently database-wide full-access or read-only.

For BlobForge, managed pull queues are the material difference. The other
features are useful but not required by the current job model. Cloudflare Queues
are nevertheless a poor primary backlog for BlobForge: Free retention is 24
hours and Paid retention is configurable only up to 14 days, while BlobForge's
normal backlog can take months to clear. Using a database as the authoritative
backlog and a message queue only as a wake-up hint would retain nearly all of the
custom database-queue complexity while adding a second system.

Bunny Database leases can provide the required non-expiring queue behavior, but
BlobForge must implement and operate claiming, visibility expiry, retry,
dead-letter, wake-up/polling, reconciliation, and observability itself. They also
cannot strengthen Bunny Database's documented failover durability window.

The durable `jobs` table should retain `todo`, `failed`, `dead`, and `done` rows
according to BlobForge policy rather than infrastructure retention. Claiming
selects one eligible row by priority and age, assigns a random fencing token and
lease expiry atomically, and returns it. Each heartbeat extends that lease only
when the fencing token still matches. A later claim first makes expired leases
eligible again, eliminating the need for a continuously scheduled janitor.

If the Bunny-only direction remains preferred, the proof of concept must test
duplicate and missing-transition recovery and define a reconciliation source for
an acknowledged enqueue that is lost during database failover. Content hashes
make duplicate conversion safe, but they do not by themselves recover a lost
enqueue.

## Pricing Comparison

Rates checked 2026-07-15:

- Bunny Database is free during Public Preview. Its published post-preview rates
  are $0.30 per billion rows read, $0.30 per million rows written, and $0.10 per
  GB per active region per month.
- Bunny Edge Scripting is $0.20 per million requests plus $0.02 per 1000 seconds
  of CPU time. The general Bunny platform advertises a $1 monthly minimum.
- Cloudflare Workers Free includes 100,000 requests per day. Workers Paid has a
  $5 monthly minimum and includes 10 million requests and 30 million CPU-ms per
  month.
- Cloudflare Queues Free includes 10,000 operations per day with fixed 24-hour
  retention. Paid includes one million operations per month, charges $0.40 per
  additional million, and supports retention from four to fourteen days. A
  normally delivered sub-64-KB message costs three operations: write, read, and
  delete; retries add reads.
- Cloudflare D1 on Paid includes 25 billion rows read and 50 million rows written
  per month, which would cover BlobForge's status database at ordinary scale.

For a small, intermittent BlobForge deployment, Cloudflare can be $0 on its free
plan and Bunny will be approximately $0 during preview or dominated by Bunny's
$1 account minimum afterward. Bunny has the better low-volume paid shape because
there is no $5 compute-plan floor and database jobs do not expire after a queue
retention period.

Cloudflare becomes economically attractive at very high heartbeat/write volume
because its $5 Paid allowance includes 50 million D1 row writes, while Bunny's
published database write rate continues linearly. At one million ordinary queue
messages per month, Cloudflare Queue operations alone are about $0.80 above the
included allowance, for an approximate $5.80 account total before unusual CPU
or retries. This comparison is not directly equivalent to Bunny because progress
heartbeats and status still require database writes on either design. Queue
pricing is ultimately moot for the selected architecture because its retention
limits do not fit BlobForge's months-long backlog.

## Migration Constraints

The current uncommitted PostgreSQL prototype is useful as a catalogue of
operations and schema fields, but it is not a safe migration baseline. Workers
and the dashboard partly use it while ingestion, janitor recovery, and several
operator commands still treat S3 markers as authoritative. The new backend must
have exactly one authority for job state; a migration may temporarily dual-write
for verification, but it must not allow independent state transitions in both
stores.

Before migration, define and test the state machine (`todo`, `processing`,
`failed`, `dead`, `done`), lease fencing, idempotent completion, retry accounting,
and object/database failure ordering. Preserve content hashes as job and blob
idempotency keys.

## Primary References

- https://docs.bunny.net/database
- https://docs.bunny.net/database/connect/typescript
- https://docs.bunny.net/database/limits
- https://docs.bunny.net/database/replication
- https://docs.bunny.net/database/durability-and-consistency
- https://docs.bunny.net/scripting/limits
- https://docs.bunny.net/scripting/pricing
- https://bunny.net/database/
- https://bunny.net/pricing/
- https://developers.cloudflare.com/queues/configuration/pull-consumers/
- https://developers.cloudflare.com/queues/reference/delivery-guarantees/
- https://developers.cloudflare.com/queues/configuration/dead-letter-queues/
- https://developers.cloudflare.com/queues/platform/pricing/
- https://developers.cloudflare.com/workers/platform/pricing/
- https://developers.cloudflare.com/workflows/
- https://developers.cloudflare.com/durable-objects/
- https://developers.cloudflare.com/workers/configuration/cron-triggers/
