# Cloudflare coordination backend

## Decision

BlobForge uses one Cloudflare Worker and one SQLite-backed Durable Object as its
coordination plane. Bunny/S3 remains the blob plane for source PDFs and output
ZIPs. The Durable Object is deliberately a single global authority: this queue
only completes a few jobs per day, so correctness, operability, and a simple
state machine matter more than horizontal database throughput.

The legacy S3 queue remains available as a compatibility fallback when the
coordinator environment variables are absent. It must not run concurrently
with coordinator-backed workers after cutover.

## Components

- `cloudflare/src/index.ts`: HTTP API, IndieAuth, queue state machine, lease
  fencing, alarms, audit records, and SQLite schema.
- `cloudflare/src/ui.ts`: dependency-free management console.
- `blobforge/coordinator_client.py`: worker/ingestor HTTP client.
- `blobforge/coordinator_migration.py`: one-time importer for the existing S3
  queue and terminal state.
- Bunny/S3: immutable raw PDFs and completed ZIP archives only after cutover.

The queue transition model is:

```text
todo ──claim──> processing ──complete──> done
                    │
                    ├──fail──> failed ──claim──> processing
                    │              └──retry budget exhausted──> dead
                    └──lease expiry/release──> todo (or dead after expiry budget)
```

Every claim has an opaque lease token and expiry. Heartbeat, completion,
failure, and release operations require the worker ID plus that token. This
fences late workers after lease recovery. Durable Object alarms recover expired
leases without a separate janitor. Claims are request-loss tolerant: a worker
that repeats a claim receives its still-active lease.

If an output upload succeeds but the completion call is lost, the next holder
detects the content-addressed ZIP in S3 and finalizes the job without repeating
conversion.

## Deploy

Use a stable HTTPS custom domain for the service before testing IndieAuth. Its
origin becomes both the IndieAuth client ID and callback origin.

```bash
cd cloudflare
npm install
npx wrangler secret put WORKER_API_TOKEN
npx wrangler secret put MIGRATION_API_TOKEN
npx wrangler deploy
```

`WORKER_API_TOKEN` is shared by trusted BlobForge ingestors and workers.
`MIGRATION_API_TOKEN` can rewrite imported state and should be distinct; delete
or rotate it after cutover. Neither secret belongs in `wrangler.jsonc` or an
image. For local development, put them in the gitignored `cloudflare/.dev.vars`.

The configured administrator is `https://eric.wendland.dev/`. Login performs
IndieAuth server metadata discovery, Authorization Code + PKCE, state
validation, and exact canonical identity comparison. Sessions are stored as
hashes in SQLite and sent in `Secure`, `HttpOnly`, `SameSite=Lax` cookies.
State-changing UI calls also require a same-origin request.

## Configure clients

Set these variables on every ingestor, CLI host, and conversion worker:

```bash
BLOBFORGE_COORDINATOR_URL=https://blobforge.example
BLOBFORGE_COORDINATOR_TOKEN=<WORKER_API_TOKEN value>
```

Keep all existing `BLOBFORGE_S3_*` variables: PDF and ZIP traffic still goes to
the object store. With both coordinator variables present, ingestion writes
metadata/jobs to Cloudflare and workers claim through Cloudflare. Without them,
BlobForge uses the legacy S3 coordination paths.

Runtime settings are edited in the management UI. The UI also shows queue
counts, recent jobs, progress, workers, and supports retry, cancellation,
reprioritization, and immediate expired-lease recovery.

## Cut over the existing queue

The migration is intentionally explicit and repeatable. It preserves todo,
failed, dead, and done states and retry counts. Old processing locks become
todo because their S3 lock cannot safely authorize a Cloudflare completion.
Done wins over every other legacy marker.

1. Deploy the coordinator and verify `/api/v1/health`.
2. Stop every legacy worker and janitor. Do not ingest during the snapshot.
3. Configure `BLOBFORGE_COORDINATOR_URL` and
   `BLOBFORGE_COORDINATOR_TOKEN` on the migration host.
4. Export `BLOBFORGE_MIGRATION_TOKEN` with the migration secret.
5. Preview and then import:

```bash
uv run blobforge coordinator-migrate --dry-run
uv run blobforge coordinator-migrate
```

6. Compare the UI totals with the dry-run summary and sample completed/dead
   records.
7. Start only coordinator-configured workers and resume ingestion.
8. Rotate/delete `MIGRATION_API_TOKEN` after validation.

Do not delete the old queue prefixes during initial validation. They provide a
read-only rollback reference, but must never be consumed in parallel with the
new coordinator.

## Operations and limits

The snapshot endpoints return at most 250 recent jobs for the console, while
aggregate counts cover the whole queue. Durable Object SQLite owns job data and
is strongly ordered within this single object. Source/output storage volume is
not charged to it. Back up critical coordination state with Cloudflare's
Durable Object recovery/export facilities before destructive migrations.

The worker API token can enqueue and operate leases but cannot use admin routes
or migration import. The UI session cannot invoke the migration route. Audit
records capture login and management/queue actions.
