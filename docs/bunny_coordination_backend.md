# Bunny coordination backend

## Decision

BlobForge uses a Bunny standalone Edge Script as its coordination API and a
Bunny Database as its durable state store. Bunny/S3 remains responsible for raw
PDFs and completed ZIPs. This keeps the complete service on Bunny while avoiding
an always-on container or a self-managed PostgreSQL server.

Bunny Database is a managed libSQL/SQLite service. It is currently in public
preview, so its service APIs may evolve. The application isolates database use
behind `bunny/src/database.ts` and keeps the worker-facing HTTP API stable to
make future adaptation inexpensive.

Official platform references:

- [Bunny Database](https://docs.bunny.net/database)
- [Connect an Edge Script to Bunny Database](https://docs.bunny.net/database/connect/scripting)
- [Bunny Edge Scripting](https://docs.bunny.net/scripting)
- [Edge Scripting limits](https://docs.bunny.net/scripting/limits)

## Components

- `bunny/src/index.ts`: Bunny runtime entry point and database connection.
- `bunny/src/app.ts`: HTTP API, IndieAuth, signed state/sessions, validation, and UI routes.
- `bunny/src/database.ts`: schema and atomic queue operations.
- `bunny/src/ui.ts`: dependency-free management console.
- `blobforge/coordinator_client.py`: worker/ingestor HTTP client.
- `blobforge/coordinator_migration.py`: one-time legacy S3 state importer.
- Bunny/S3: content-addressed raw PDFs and output ZIPs.

The queue transition model is:

```text
todo ──claim──> processing ──complete──> done
                    │
                    ├──fail──> failed ──claim──> processing
                    │              └──retry budget exhausted──> dead
                    └──lease expiry/release──> todo (or dead after expiry budget)
```

Claims are single atomic SQLite `UPDATE ... RETURNING` statements. They select
the oldest available job in priority order, ensure a worker has no other active
job, assign an opaque lease token, and set an expiry. Heartbeat, completion,
failure, and release require both the worker ID and token, fencing late requests
after recovery.

Bunny Edge Scripting has no Durable Object-style alarm primitive. BlobForge does
not need a background script: expired leases are recovered atomically before a
claim, before a UI/API snapshot, or through the UI recovery button. The next
worker poll therefore makes abandoned work available immediately. The Edge
Script scales to zero between requests and does not run PDF conversions itself.

If an output upload succeeds but the completion call is lost, a later worker
detects the content-addressed ZIP and finalizes the database job without
repeating conversion.

## Create and deploy

1. In Bunny Dashboard, create a database under **Edge Platform → Database**.
2. On its **Access** page, generate credentials.
3. Create a **Standalone** Edge Script connected to this repository.
4. Configure the project to run `cd bunny && npm ci && npm run build` and deploy
   `bunny/dist/index.js` as the entry file.
5. From the database Access page, use **Add Secrets to Edge Script**, which adds
   `BUNNY_DATABASE_URL` and `BUNNY_DATABASE_AUTH_TOKEN`.
6. Add these Edge Script secrets:

```text
WORKER_API_TOKEN=<long random worker secret>
MIGRATION_API_TOKEN=<different long random migration secret>
SESSION_SIGNING_SECRET=<third independent long random secret>
```

7. Add these non-secret environment variables:

```text
ADMIN_MES=https://eric.wendland.dev/,https://another-admin.example/
SESSION_TTL_SECONDS=43200
LEASE_SECONDS=900
```

Use a stable HTTPS custom hostname before testing IndieAuth. The request origin
becomes the IndieAuth client ID and callback origin. `ADMIN_MES` is a
comma-separated allowlist. `ADMIN_ME` remains accepted as a single-admin
compatibility setting when `ADMIN_MES` is absent.

Alternatively, Bunny's repository integration can generate its deployment
workflow after the project, build command, and entry file are selected in the
dashboard. This avoids committing account-specific script identifiers.

## Authentication and authorization

The login page asks for an IndieAuth profile URL. A value without a protocol is
normalized to `https://`; explicitly non-HTTPS profiles are rejected. Discovery
only starts after the normalized URL matches `ADMIN_MES`.

The form is converted by the local `/login.js` module into a top-level
same-origin navigation before the external authorization redirect. This keeps
the response's strict `form-action 'self'` CSP without browsers treating the
IndieAuth authorization endpoint as a disallowed form destination.

The management UI uses Authorization Code + PKCE. PKCE state and authenticated
sessions are HMAC-signed with `SESSION_SIGNING_SECRET`, so the callback and next
page request do not depend on cross-region database replication. Session
cookies are `Secure`, `HttpOnly`, and `SameSite=Lax`; UI writes also require a
same-origin request. Rotating the signing secret immediately logs out every
administrator.

The production cookie is always named `__Host-blobforge_session`; it does not
depend on the internal protocol Bunny presents to a particular edge instance.
Authenticated and HTML responses send browser/CDN/surrogate no-store headers.
`GET /auth/status` provides safe transport diagnostics: it reports whether the
cookie arrived and whether it validates, but never returns the signed token.

The worker secret can enqueue and operate leases, but cannot call admin or
migration routes and cannot forge admin sessions. The migration secret can
import state but cannot operate workers or UI routes. All three secrets must be
independent. Rotate or remove the migration secret after cutover.

## Configure BlobForge clients

Set these variables on every ingestor, CLI host, and conversion worker:

```bash
BLOBFORGE_COORDINATOR_URL=https://blobforge.example
BLOBFORGE_COORDINATOR_TOKEN=<WORKER_API_TOKEN value>
```

Keep the existing `BLOBFORGE_S3_*` variables. Source and result traffic still
goes to object storage. When the coordinator variables are absent, BlobForge
uses its legacy S3 coordination paths for compatibility.

Runtime settings are edited in the management UI. The UI displays queue counts,
recent jobs, progress, workers, and supports retry, cancellation,
reprioritization, and immediate expired-lease recovery.

## Cut over the existing queue

The migration preserves todo, failed, dead, and done states, retry counts, file
paths, tags, sources, and sizes. Old processing locks become todo because their
S3 locks cannot authorize Bunny Database updates. Done output takes precedence
over every legacy marker.

1. Deploy the Edge Script and verify `/api/v1/health` reports the database as
   connected.
2. Stop every legacy worker and janitor. Pause ingestion during the snapshot.
3. Set `BLOBFORGE_COORDINATOR_URL` and `BLOBFORGE_COORDINATOR_TOKEN` on the
   migration host.
4. Export `BLOBFORGE_MIGRATION_TOKEN` with the Edge Script migration secret.
5. Preview and import:

```bash
uv run blobforge coordinator-migrate --dry-run
uv run blobforge coordinator-migrate
```

6. Compare UI totals with the dry-run summary and sample completed/dead jobs.
7. Start only coordinator-configured workers and resume ingestion.
8. Rotate or delete `MIGRATION_API_TOKEN` after validation.

Do not delete the old S3 queue prefixes during initial validation. They are a
read-only rollback reference, but must not be consumed alongside Bunny
Database.

## Efficiency and limits

The coordination workload makes only short SQL calls. PDF conversion remains on
external workers, so the Edge Script stays far below Bunny's per-request CPU and
memory limits. A worker poll typically causes one lease-recovery batch and one
atomic claim. The UI snapshot uses one read batch and returns at most 250 recent
jobs while aggregate counts cover the entire queue.

Database and Edge Script usage scale to zero when idle. As of July 2026, Bunny
Database is free during public preview; Edge Scripting is request/CPU based and
has a platform-level monthly minimum. Confirm current prices before deployment
because preview terms can change.
