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
- `bunny/src/object_store.ts`: WebCrypto AWS SigV4 presigning and output existence checks.
- `bunny/src/management_ui.ts`: authenticated file library, worker management, ZIP preview, and persistent ToC behavior.
- `bunny/src/markdown_runtime.ts`: Marked + DOMPurify browser rendering boundary.
- `bunny/scripts/generate-markdown.mjs`: reproducibly embeds the self-hosted browser renderer in the Edge Script bundle.
- `bunny/src/database.ts`: schema and atomic queue operations.
- `bunny/src/ui.ts`: dependency-free management console.
- `blobforge/coordinator_client.py`: worker/ingestor HTTP client.
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

If an output upload succeeds but the completion call is lost, the coordinator
detects the content-addressed ZIP on the next claim and lets the worker finalize
the database job without repeating conversion.

## Least-privilege object transfers

Conversion workers do not receive bucket credentials and do not initialize an
S3 client. On claim, the coordinator returns a short-lived SigV4 `GET` URL for
exactly `store/raw/<hash>.pdf` plus metadata already held in Bunny Database.
Because conversion can take hours, no upload URL is issued at claim time. The
lease holder requests a fresh `PUT` URL for exactly `store/out/<hash>.zip`
immediately before upload. That request requires the current worker token,
worker identity, file hash, and fenced lease token.

The coordinator performs a signed `HEAD` before completion and will not mark a
job done unless its output exists. It also checks for an already-uploaded output
when a job is claimed, preserving idempotency when an upload succeeds but the
completion response is lost. URLs are content-addressed and expire according to
`DOWNLOAD_URL_TTL_SECONDS` and `UPLOAD_URL_TTL_SECONDS`.

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
CLIENT_API_TOKEN=<long random ingestor/CLI secret>
SESSION_SIGNING_SECRET=<third independent long random secret>
S3_ACCESS_KEY_ID=<coordinator object-store access key>
S3_SECRET_ACCESS_KEY=<coordinator object-store secret key>
```

7. Add these non-secret environment variables:

```text
ADMIN_MES=https://eric.wendland.dev/,https://another-admin.example/
SESSION_TTL_SECONDS=43200
LEASE_SECONDS=900
S3_ENDPOINT_URL=https://s3.example.com
S3_BUCKET=blobforge
S3_REGION=us-east-1
S3_PREFIX=pdf/
S3_FORCE_PATH_STYLE=true
DOWNLOAD_URL_TTL_SECONDS=3600
UPLOAD_URL_TTL_SECONDS=900
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
page request do not depend on cross-region database replication. Rotating the
signing secret immediately logs out every administrator.

Standalone Bunny deployment testing showed that the callback's `Set-Cookie`
did not reach the subsequent request. The callback therefore redirects to
`/console` with the signed session in the URL fragment. Fragments are not sent
to the Edge Script or in HTTP referrers; the local application module stores the
token in same-origin browser storage and immediately removes the fragment from
the address bar and history entry. Management API calls authenticate with the
dedicated `Authorization: BlobForge-Session …` scheme. The `/console` document
is only a public shell: all queue data and mutations remain behind signed-token
validation, and writes additionally require a same-origin request.

This design makes the session readable to same-origin JavaScript, so the strict
same-origin CSP, absence of third-party scripts, and short session lifetime are
part of the security boundary. Signing out removes the browser copy; because
sessions are stateless, a copied token remains valid until expiry or signing-key
rotation. Authenticated and HTML responses send browser/CDN/surrogate no-store
headers. `GET /auth/status` reports cookie and session-header presence plus
signed-session validity without returning the token. A normal address-bar
request does not include the application-managed session header.

`CLIENT_API_TOKEN` is used by trusted ingestors and command-line readers. It can
enqueue/read jobs and read coordinator snapshots, but cannot operate worker
leases or call admin routes. `WORKER_API_TOKEN` remains a temporary deployment
fallback name for `CLIENT_API_TOKEN`; new deployments should use the latter.

Worker credentials are created in the IndieAuth management UI. Each token is
bound to one generated worker ID, returned only once, stored only as a SHA-256
hash, and immediately rejected after revocation. The token can register and
heartbeat its own worker, claim one fenced lease, operate that lease, request
its exact transfer URL, and read dashboard/config data. It cannot enqueue jobs,
act as another worker, call admin routes, or forge admin sessions.
Revocation requeues its active lease and marks the runtime worker stopped.
Revoked credentials and runtime records are excluded from ordinary fleet
snapshots. Administrators load them explicitly through the management
console's **Revoked workers** view or `GET /api/v1/admin/workers?revoked=true`.

## Configure BlobForge clients

Trusted ingestors and administrative CLI hosts use:

```bash
BLOBFORGE_COORDINATOR_URL=https://blobforge.example
BLOBFORGE_COORDINATOR_TOKEN=<CLIENT_API_TOKEN value>
```

Ingestors keep their existing `BLOBFORGE_S3_*` variables so they can write raw
PDFs. Conversion workers do not need any `BLOBFORGE_S3_*` variable. Create a
worker in the management UI and run the no-clone Linux installation command it
displays:

```bash
curl -fsSLO https://raw.githubusercontent.com/tionis/BlobForge/main/scripts/install-linux-worker.sh
chmod +x install-linux-worker.sh
./install-linux-worker.sh --coordinator-url https://blobforge.example --token bfw_...
```

The installer pulls the published container and creates a user systemd service.
See `docs/linux_worker_setup.md` for scheduling, GPU passthrough, upgrades,
manual containers, and native `uv` installation.

The same credential can read coordinator-backed terminal dashboards:

```bash
blobforge dashboard --coordinator-url https://blobforge.example --token bfw_...
blobforge workers --coordinator-url https://blobforge.example --token bfw_...
```

Environment variables remain supported for long-running services. Ingestion and
distributed workers require the coordinator variables; the old S3 coordination
fallback is no longer supported.

### Upgrade an existing shared-token deployment

This is an intentional worker-authentication cutover: the old shared
`WORKER_API_TOKEN` no longer authorizes lease operations. Plan a brief worker
pause, then:

1. Add the coordinator's S3 variables and `CLIENT_API_TOKEN` (the old shared
   value may be reused temporarily as the client token) and deploy the script.
2. Sign in to the UI and create one enrollment for each conversion worker.
3. Upgrade BlobForge on each worker and start it with the displayed command.
4. Remove all bucket credentials and obsolete worker-ID configuration from
   conversion hosts after their enrolled startup succeeds.

Schema initialization creates `worker_credentials` automatically. Existing
queue, runtime worker, and job records are retained; old runtime records remain
visible in coordinator snapshots until replaced or stale.

An enrollment's worker ID is the label's deterministic lowercase ASCII slug:
`GPU Workstation` becomes `gpu-workstation`. The coordinator rejects duplicate
IDs and labels that normalize to an existing slug with HTTP 409; revoked IDs
remain reserved so audit history cannot silently change owners. Each displayed
token is a credential for that one ID and must not be shared across workers.
Reusable provisioning, if needed, is a separate dynamic-registration-token
design: a bootstrap token would authorize registration and mint a distinct
worker ID/credential for every registering instance (potentially using an
incrementing suffix). It is not part of ordinary enrollment.

Runtime settings are edited in the management UI. The UI displays queue counts,
recent jobs, progress, enrolled/runtime workers, and supports worker creation
and revocation, retry, cancellation, reprioritization, and immediate
expired-lease recovery. Browser PDF upload uses a coordinator-issued pre-signed
URL, so PDF bodies do not pass through Edge Scripting.

## Progress and failure diagnostics

Workers publish explicit macro stages (`claimed`, `downloading`, `inspecting`,
`converting`, `packaging`, and `uploading`) with an approximate overall
percentage. Marker/tqdm counters, rates, and ETA are included when available.
Isolated conversions publish model-loading, conversion, extraction, and output
writing checkpoints through an atomic JSON side channel monitored by the parent
worker, so subprocess isolation does not make progress disappear.
Changing stage wakes the heartbeat publisher immediately when normal
heartbeats are enabled; noisy converter events are coalesced to one publication
per two seconds. An active job heartbeat renews its lease and updates the worker
record in the same request. When `heartbeat_enabled` is false, idle and prompt
progress heartbeats stop, but an active lease is renewed one-third of the way to
expiry so a transient failed renewal still leaves recovery time.
Register, claim, and heartbeat responses carry current runtime configuration,
so a changed interval takes effect after the next existing coordinator request.
Claims always return a `{ job, config }` envelope, including `job: null` for an
empty queue. Coordinator and worker releases must therefore be deployed
together; obsolete worker protocols are not supported.
Workers blocked by a run condition publish one `suspended` transition with
optional resume metadata and send no heartbeats until they resume. The Web UI
refreshes active jobs every ten seconds and renders stage, percentage, worker,
elapsed time, converter counters, and ETA.

Every failed attempt creates an append-only `job_failures` row before the job is
released. It records the attempt number, worker, timestamp, concise error,
traceback, structured context (including stage and exception type), and the last
progress/system snapshot. Lease expiry creates the same kind of record. The job
table keeps only the latest summary for queue queries, while
`GET /api/v1/admin/jobs/{hash}/failures` and the management UI expose up to 50
most-recent detailed attempts. Retrying a job clears its current error summary
but deliberately preserves this history. Database backups include the failure
table.

## Database backups

An IndieAuth administrator can click **Back up database** or call
`POST /api/v1/admin/backups` with the normal signed admin session. The Edge
Script reads all application tables in one libSQL read transaction, writes a
versioned JSON document containing the schema and rows, and uploads it to
`{S3_PREFIX}backups/coordinator/<timestamp>-<random>.json`.

The response includes the object key, byte size, per-table row counts, and a
SHA-256 checksum. Backups include worker credential hashes and administrator
audit identities, so the backup prefix must remain private. The coordinator S3
credential needs read access to raw PDFs, write access to output ZIPs and the
backup prefix, and metadata-read access to outputs. It does not need bucket
listing or deletion rights.

Bunny Database also maintains platform-level recovery snapshots. The S3 JSON
export is the application-controlled, portable copy and is deliberately stored
outside `registry/` so legacy cleanup cannot remove it.

## Remove legacy object state

Once the Bunny UI totals have been validated, preview the obsolete queue and
registry objects from a trusted host with list/delete bucket credentials:

```bash
uv run blobforge cleanup-legacy
uv run blobforge cleanup-legacy --execute
```

The destructive form requires typing `DELETE`; automation can add `--yes`.
Only `{S3_PREFIX}queue/` and `{S3_PREFIX}registry/` are touched. Raw PDFs,
converted outputs, and database backups remain intact.

## Web library, uploads, and result preview

The management console queries `GET /api/v1/admin/files` independently of the
operational snapshot. Its database query supports pagination, status and
priority filters, and case-insensitive matching across content hash, original
filename, source, paths, and tags. Consequently, completed jobs remain
discoverable even when the active/recent job list exceeds 250 entries.

PDF ingestion is a two-step direct-to-object-store flow. The browser validates
the `%PDF-` signature, computes SHA-256, requests an exact-key raw-object PUT
URL from `POST /api/v1/admin/uploads`, and uploads without sending the PDF
through Edge Scripting. It then calls
`POST /api/v1/admin/uploads/<hash>/complete`; the coordinator verifies that the
raw object exists before creating or updating the database job.

Source PDF and completed ZIP downloads use short-lived exact-key GET URLs.
Result preview downloads the ZIP in the administrator's browser and parses
stored or deflated entries with browser APIs. A self-hosted Marked bundle
renders full GFM Markdown, then DOMPurify sanitizes both parser output and raw
HTML before it reaches the preview DOM. Archive image references are replaced
with blob URLs only after sanitization. The renderer assigns stable,
duplicate-safe IDs to sanitized headings and derives a persistent navigation
tree from them: it is a sticky left sidebar with active-section highlighting on
desktop and a collapsible drawer on narrow screens. This avoids a top-only ToC
that disappears while reading long documents.

`npm run check` and `npm run build` regenerate
`bunny/src/generated/markdown_bundle.ts` from the pinned dependencies; the
browser never loads parser code from a third-party CDN. Preview rejects
archives with more than 10,000 entries or more than 1 GiB declared expanded
data. ZIP64 and uncommon compression methods remain downloadable but are not
previewable.

Because browser uploads and previews contact the object store directly, set a
CORS rule for the exact management-console origin. An AWS-compatible example is:

```json
[
  {
    "AllowedOrigins": ["https://blobforge.example"],
    "AllowedMethods": ["GET", "PUT", "HEAD"],
    "AllowedHeaders": ["Content-Type"],
    "ExposeHeaders": ["ETag"],
    "MaxAgeSeconds": 3600
  }
]
```

Replace the example origin with the deployed Edge Script/custom hostname. Do
not use `*` for the origin on a private library.

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
